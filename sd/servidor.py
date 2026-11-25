import argparse
import signal
import socket
import socketserver
import sys
import threading
import time

from sd import cadastro_client, spawner
from sd.common import config
from sd.common.logger import MetricsLogger
from sd.common.net import LineReader, send_and_recv, send_line
from sd.common.protocolo import fmt_digest, fmt_error, fmt_hello, fmt_shutdown
from sd.coordenador import Coordenador
from sd.escalador import Escalador
from sd.estado import Estado
from sd.mensagens import Receptor, Roteador
from sd.replicacao import Aplicador, OpLog, Replicador
from sd.usuarios import GerenciadorUsuarios

IDLE_CONN_TIMEOUT_SEC = 30.0


class NodeTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True
    request_queue_size = 128

    def __init__(
        self,
        server_address: tuple[str, int],
        handler_cls,
        coordinator: Coordenador,
        logger: MetricsLogger,
        shutdown_event: threading.Event,
    ):
        super().__init__(server_address, handler_cls)
        self.coordinator = coordinator
        self.logger = logger
        self.shutdown_event = shutdown_event


class ClientConnectionHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        srv: NodeTCPServer = self.server  # type: ignore[assignment]
        peer = f"{self.client_address[0]}:{self.client_address[1]}"
        reader = LineReader(self.request)

        while not srv.shutdown_event.is_set():
            try:
                line = reader.read_line(timeout=IDLE_CONN_TIMEOUT_SEC)
            except ValueError:
                srv.logger.log("erro_rede", valor=1, origem=peer, motivo="LINE_TOO_LONG")
                try:
                    send_line(self.request, fmt_error("LINE_TOO_LONG"))
                except OSError:
                    pass
                break
            except (socket.timeout, ConnectionResetError, BrokenPipeError, OSError) as exc:
                srv.logger.log("conexao_encerrada", valor=1, origem=peer, motivo=type(exc).__name__)
                break

            if line is None:
                break

            if not line.strip():
                continue

            try:
                reply, should_shutdown = srv.coordinator.handle_line(line)
            except ConnectionResetError:
                # Usado por CHAOS_DROP_ACK_PROB para simular queda de ACK após processamento
                break

            try:
                send_line(self.request, reply)
            except UnicodeEncodeError:
                srv.logger.log("erro_rede", valor=1, origem=peer, motivo="ENCODING")
                try:
                    send_line(self.request, fmt_error("ENCODING"))
                except OSError:
                    pass
                break
            except (socket.timeout, ConnectionResetError, BrokenPipeError, OSError) as exc:
                srv.logger.log("conexao_encerrada", valor=1, origem=peer, motivo=type(exc).__name__)
                break

            if should_shutdown:
                srv.logger.log("servidor_encerrando", valor=1, motivo="COMANDO")
                srv.shutdown_event.set()
                break


def _send_shutdown_cmd(ip: str, port: int, server_id: int) -> bool:
    reply = send_and_recv(ip, port, fmt_shutdown(server_id), timeout=config.INTERNAL_TIMEOUT_SEC)
    return reply == f"ACK_SHUTDOWN_{server_id}"


def _announce_hello(
    node_id: int,
    coord_ip: str,
    coord_port: int,
    boot_ts: float,
    logger: MetricsLogger,
    shutdown_event: threading.Event,
) -> None:
    expected = f"ACK_HELLO_{node_id}"
    msg = fmt_hello(node_id)

    for attempt in range(1, config.HELLO_RETRIES + 1):
        if shutdown_event.is_set():
            return
        try:
            reply = send_and_recv(coord_ip, coord_port, msg, timeout=config.INTERNAL_TIMEOUT_SEC)
            if reply == expected:
                elapsed = round(time.monotonic() - boot_ts, 4)
                logger.log("hello_confirmado", valor=elapsed, tentativas=attempt)
                return
            if reply == "ERROR_UNKNOWN_SERVER":
                shutdown_event.set()
                return
        except OSError:
            pass

        if attempt < config.HELLO_RETRIES:
            time.sleep(config.HELLO_RETRY_DELAY)

    logger.log("hello_falhou", valor=config.HELLO_RETRIES)
    shutdown_event.set()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Servidor do Sistema Distribuído")
    parser.add_argument("--id", type=int, required=True, help="Identificador inteiro >= 0 do servidor")
    parser.add_argument("--coord-ip", default=config.SERVER_IP, help="IP do coordenador atual")
    parser.add_argument("--coord-port", type=int, default=config.SERVER_PORT, help="Porta do coordenador atual")
    args = parser.parse_args()
    if args.id < 0:
        parser.error("--id deve ser >= 0")
    return args


def main() -> None:
    boot_ts = time.monotonic()
    args = parse_args()
    node_id: int = args.id
    port = config.port_for_server(node_id)
    is_coord = node_id == config.INITIAL_SERVER_ID

    logger = MetricsLogger("coordenador" if is_coord else "servidor", node_id)
    state = Estado(my_id=node_id, is_coordinator=is_coord)
    state.coordinator_addr = (args.coord_ip, args.coord_port)

    coordinator = Coordenador(state=state, logger=logger)
    shutdown_event = threading.Event()

    oplog = OpLog(node_id=node_id)
    escalador: Escalador | None = None
    replicador: Replicador | None = None
    aplicador: Aplicador | None = None

    receptor = Receptor(state=state, logger=logger)
    coordinator.register_handler("DELIVER", receptor.handle_deliver)

    if is_coord:
        state.register_server(node_id, ip=config.SERVER_IP, port=port)
        replicador = Replicador(oplog=oplog, logger=logger)
        coordinator.attach_replicator(replicador)
        replicador.append_locked("SRV_UP", {"id": node_id, "ip": config.SERVER_IP, "port": port})

        roteador = Roteador(coordenador=coordinator, logger=logger)
        coordinator.register_handler("MSG", roteador.handle_msg)
        coordinator.register_handler("DIGEST", coordinator.handle_digest)
        
        coordinator.attach_scaler(escalador)
        wake_evt = coordinator.on_state_change_event

        escalador = Escalador(
            coordenador=coordinator,
            logger=logger,
            spawn_fn=spawner.spawn_server,
            newid_fn=cadastro_client.send_newid,
            shutdown_fn=_send_shutdown_cmd,
            terminate_fn=spawner.terminate_process,
        )
        coordinator.attach_scaler(escalador)
        wake_evt = threading.Event()
    else:
        # Seguidor não se auto-registra; recebe o estado via REPL
        aplicador = Aplicador(state=state, oplog=oplog, logger=logger)
        coordinator.register_handler("REPL", aplicador.handle_repl)
        coordinator.register_handler("DIGEST", aplicador.handle_digest)
        wake_evt = aplicador.on_applied_event

    gerenciador_usuarios = GerenciadorUsuarios(state=state, logger=logger, wake_event=wake_evt)

    def _signal_handler(signum, _frame):
        logger.log("sinal_recebido", valor=signum)
        shutdown_event.set()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    try:
        tcp_server = NodeTCPServer(
            (config.BIND_IP, port),
            ClientConnectionHandler,
            coordinator=coordinator,
            logger=logger,
            shutdown_event=shutdown_event,
        )
    except OSError as exc:
        sys.exit(f"Erro ao abrir socket em {config.BIND_IP}:{port}: {exc}")

    server_thread = threading.Thread(
        target=lambda: tcp_server.serve_forever(poll_interval=0.1),
        daemon=True,
    )
    server_thread.start()

    role = "COORDENADOR" if is_coord else "SERVIDOR"
    print(f"[ok] Servidor {node_id} ({role}) escutando em {config.BIND_IP}:{port}")
    logger.log("servidor_iniciado", valor=1, porta=port, papel=role)

    gerenciador_usuarios.start()
    if escalador is not None:
        escalador.start()
    else:
        threading.Thread(
            target=_announce_hello,
            args=(node_id, args.coord_ip, args.coord_port, boot_ts, logger, shutdown_event),
            daemon=True,
        ).start()

    try:
        while not shutdown_event.is_set():
            shutdown_event.wait(timeout=0.5)
    finally:
        # Ordem obrigatória de drenagem antes de fechar o socket TCP
        gerenciador_usuarios.stop(drain=True, timeout=config.DRAIN_TIMEOUT_SEC)
        receptor.wait_idle(timeout=config.DRAIN_TIMEOUT_SEC)
        if escalador is not None:
            escalador.stop()
        if replicador is not None:
            replicador.stop()
        tcp_server.shutdown()
        tcp_server.server_close()
        receptor.close()
        oplog.close()
        logger.log("servidor_finalizado", valor=1)
        print(f"[ok] Servidor {node_id} encerrado.")

    sys.exit(0)


if __name__ == "__main__":
    main()