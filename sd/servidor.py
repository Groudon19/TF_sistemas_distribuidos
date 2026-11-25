import argparse
import signal
import socket
import socketserver
import sys
import threading
import time

from sd.config import (
    ATRASO_TENTATIVA_HELLO_SEG,
    ID_SERVIDOR_INICIAL,
    IP_BIND,
    IP_SERVIDOR,
    PORTA_SERVIDOR,
    TENTATIVAS_HELLO,
    TIMEOUT_DRENAGEM_SEG,
    TIMEOUT_INTERNO_SEG,
    LeitorLinha,
    LoggerMetricas,
    enviar_e_receber,
    enviar_linha,
    formatar_erro,
    formatar_hello,
    formatar_shutdown,
    porta_do_servidor,
)
from sd.coordenador import Coordenador, Escalador
from sd.estado import Estado
from sd.mensagens import GerenciadorUsuarios, Receptor, Roteador
from sd.replicacao import Aplicador, LogOperacoes, Replicador

TIMEOUT_CONEXAO_OCIOSA_SEG = 30.0


class ServidorTCPNo(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True
    request_queue_size = 128

    def __init__(
        self,
        endereco_servidor: tuple[str, int],
        classe_handler,
        coordenador: Coordenador,
        logger: LoggerMetricas,
        evento_encerrar: threading.Event,
    ):
        super().__init__(endereco_servidor, classe_handler)
        self.coordenador = coordenador
        self.logger = logger
        self.evento_encerrar = evento_encerrar


class TratadorConexaoCliente(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        srv: ServidorTCPNo = self.server  # type: ignore[assignment]
        origem = f"{self.client_address[0]}:{self.client_address[1]}"
        leitor = LeitorLinha(self.request)

        while not srv.evento_encerrar.is_set():
            try:
                linha = leitor.ler_linha(timeout=TIMEOUT_CONEXAO_OCIOSA_SEG)
            except ValueError:
                srv.logger.registrar("erro_rede", valor=1, origem=origem, motivo="LINE_TOO_LONG")
                try:
                    enviar_linha(self.request, formatar_erro("LINE_TOO_LONG"))
                except OSError:
                    pass
                break
            except (socket.timeout, ConnectionResetError, BrokenPipeError, OSError) as exc:
                srv.logger.registrar("conexao_encerrada", valor=1, origem=origem, motivo=type(exc).__name__)
                break

            if linha is None:
                break

            if not linha.strip():
                continue

            try:
                resposta, deve_encerrar = srv.coordenador.tratar_linha(linha)
            except ConnectionResetError:
                break

            try:
                enviar_linha(self.request, resposta)
            except UnicodeEncodeError:
                srv.logger.registrar("erro_rede", valor=1, origem=origem, motivo="ENCODING")
                try:
                    enviar_linha(self.request, formatar_erro("ENCODING"))
                except OSError:
                    pass
                break
            except (socket.timeout, ConnectionResetError, BrokenPipeError, OSError) as exc:
                srv.logger.registrar("conexao_encerrada", valor=1, origem=origem, motivo=type(exc).__name__)
                break

            if deve_encerrar:
                srv.logger.registrar("servidor_encerrando", valor=1, motivo="COMANDO")
                srv.evento_encerrar.set()
                break


def _enviar_comando_shutdown(ip: str, porta: int, id_servidor: int) -> bool:
    resposta = enviar_e_receber(ip, porta, formatar_shutdown(id_servidor), timeout=TIMEOUT_INTERNO_SEG)
    return resposta == f"ACK_SHUTDOWN_{id_servidor}"


def _anunciar_hello(
    id_no: int,
    ip_coord: str,
    porta_coord: int,
    ts_inicio: float,
    logger: LoggerMetricas,
    evento_encerrar: threading.Event,
) -> None:
    esperado = f"ACK_HELLO_{id_no}"
    mensagem = formatar_hello(id_no)

    for tentativa in range(1, TENTATIVAS_HELLO + 1):
        if evento_encerrar.is_set():
            return
        try:
            resposta = enviar_e_receber(ip_coord, porta_coord, mensagem, timeout=TIMEOUT_INTERNO_SEG)
            if resposta == esperado:
                decorrido = round(time.monotonic() - ts_inicio, 4)
                logger.registrar("hello_confirmado", valor=decorrido, tentativas=tentativa)
                return
            if resposta == "ERROR_UNKNOWN_SERVER":
                evento_encerrar.set()
                return
        except OSError:
            pass

        if tentativa < TENTATIVAS_HELLO:
            time.sleep(ATRASO_TENTATIVA_HELLO_SEG)

    logger.registrar("hello_falhou", valor=TENTATIVAS_HELLO)
    evento_encerrar.set()


def ler_argumentos() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Servidor do Sistema Distribuído")
    parser.add_argument("--id", type=int, required=True, help="Identificador inteiro >= 0 do servidor")
    parser.add_argument("--coord-ip", default=IP_SERVIDOR, help="IP do coordenador atual")
    parser.add_argument("--coord-port", type=int, default=PORTA_SERVIDOR, help="Porta do coordenador atual")
    args = parser.parse_args()
    if args.id < 0:
        parser.error("--id deve ser >= 0")
    return args


def main() -> None:
    ts_inicio = time.monotonic()
    args = ler_argumentos()
    id_no: int = args.id
    porta = porta_do_servidor(id_no)
    eh_coord = id_no == ID_SERVIDOR_INICIAL

    logger = LoggerMetricas("coordenador" if eh_coord else "servidor", id_no)
    estado = Estado(meu_id=id_no, eh_coordenador=eh_coord)
    estado.endereco_coordenador = (args.coord_ip, args.coord_port)

    coordenador = Coordenador(estado=estado, logger=logger)
    evento_encerrar = threading.Event()

    log_ops = LogOperacoes(id_no=id_no)
    escalador: Escalador | None = None
    replicador: Replicador | None = None
    aplicador: Aplicador | None = None

    receptor = Receptor(estado=estado, logger=logger)
    coordenador.registrar_tratador("DELIVER", receptor.tratar_deliver)

    if eh_coord:
        estado.registrar_servidor(id_no, ip=IP_SERVIDOR, porta=porta)
        replicador = Replicador(log_ops=log_ops, logger=logger)
        coordenador.anexar_replicador(replicador)
        replicador.anexar_sob_lock("SRV_UP", {"id": id_no, "ip": IP_SERVIDOR, "port": porta})

        roteador = Roteador(coordenador=coordenador, logger=logger)
        coordenador.registrar_tratador("MSG", roteador.tratar_msg)
        coordenador.registrar_tratador("DIGEST", coordenador.tratar_digest)

        escalador = Escalador(
            coordenador=coordenador,
            logger=logger,
            func_shutdown=_enviar_comando_shutdown,
        )
        coordenador.anexar_escalador(escalador)
        evento_acordar = coordenador.evento_mudanca_estado
    else:
        aplicador = Aplicador(estado=estado, log_ops=log_ops, logger=logger)
        coordenador.registrar_tratador("REPL", aplicador.tratar_repl)
        coordenador.registrar_tratador("DIGEST", aplicador.tratar_digest)
        evento_acordar = aplicador.evento_aplicado

    gerenciador_usuarios = GerenciadorUsuarios(estado=estado, logger=logger, evento_acordar=evento_acordar)

    def _tratar_sinal(signum, _frame):
        logger.registrar("sinal_recebido", valor=signum)
        evento_encerrar.set()

    signal.signal(signal.SIGINT, _tratar_sinal)
    signal.signal(signal.SIGTERM, _tratar_sinal)

    try:
        servidor_tcp = ServidorTCPNo(
            (IP_BIND, porta),
            TratadorConexaoCliente,
            coordenador=coordenador,
            logger=logger,
            evento_encerrar=evento_encerrar,
        )
    except OSError as exc:
        sys.exit(f"Erro ao abrir socket em {IP_BIND}:{porta}: {exc}")

    thread_servidor = threading.Thread(
        target=lambda: servidor_tcp.serve_forever(poll_interval=0.1),
        daemon=True,
    )
    thread_servidor.start()

    papel = "COORDENADOR" if eh_coord else "SERVIDOR"
    print(f"[ok] Servidor {id_no} ({papel}) escutando em {IP_BIND}:{porta}")
    logger.registrar("servidor_iniciado", valor=1, porta=porta, papel=papel)

    gerenciador_usuarios.iniciar()
    if escalador is not None:
        escalador.iniciar()
    else:
        threading.Thread(
            target=_anunciar_hello,
            args=(id_no, args.coord_ip, args.coord_port, ts_inicio, logger, evento_encerrar),
            daemon=True,
        ).start()

    try:
        while not evento_encerrar.is_set():
            evento_encerrar.wait(timeout=0.5)
    finally:
        gerenciador_usuarios.parar(drenar=True, timeout=TIMEOUT_DRENAGEM_SEG)
        receptor.aguardar_ocioso(timeout=TIMEOUT_DRENAGEM_SEG)
        if escalador is not None:
            escalador.parar()
        if replicador is not None:
            replicador.parar()
        servidor_tcp.shutdown()
        servidor_tcp.server_close()
        receptor.fechar()
        log_ops.fechar()
        logger.registrar("servidor_finalizado", valor=1)
        print(f"[ok] Servidor {id_no} encerrado.")

    sys.exit(0)


if __name__ == "__main__":
    main()