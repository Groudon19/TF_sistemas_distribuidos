import json
import random
import threading
import time
from pathlib import Path
from typing import Callable

from sd.config import (
    ATRASO_RECEIVE_MSG_SEG,
    DIRETORIO_LOGS,
    INTERVALO_MAX_MSG_SEG,
    MAX_TENTATIVAS_MSG,
    PRAZO_ROTEAMENTO_SEG,
    PROB_PERDA_ACK_CAOS,
    TIMEOUT_ACK_MSG_SEG,
    TIMEOUT_DRENAGEM_SEG,
    TIMEOUT_ENTREGA_SEG,
    LoggerMetricas,
    enviar_e_receber,
    formatar_ack,
    formatar_deliver,
    formatar_erro,
    formatar_msg,
    interpretar_deliver,
    interpretar_msg,
)
from sd.estado import Estado

_MENSAGENS_EXEMPLO = [
    "Ola, tudo bem por ai?",
    "Confirmando recebimento do relatorio.",
    "Teste de acentuacao: acao, coracao, Sao Paulo!",
    "Mensagem com_underscores_e_simbolos_100%!",
    "Linha 1\nLinha 2 com quebra escapada",
    "Ping distribuido entre nos ativos.",
]

_ERROS_REENVIO = frozenset({
    "ERROR_NOT_COORDINATOR",
    "ERROR_DEST_UNAVAILABLE",
    "ERROR_BUSY",
})


def receber_msg_padrao(conteudo: str | None) -> bool:
    """Função exigida pela especificação: retorna False se None, senão aguarda e retorna True."""
    if conteudo is None:
        return False
    time.sleep(ATRASO_RECEIVE_MSG_SEG)
    return True


class Receptor:
    """Executa a entrega local no servidor de destino com deduplicação local e replicada."""

    def __init__(
        self,
        estado: Estado,
        logger: LoggerMetricas,
        dir_logs: Path = DIRETORIO_LOGS,
        func_receber: Callable[[str | None], bool] = receber_msg_padrao,
        prob_perda_ack: float = PROB_PERDA_ACK_CAOS,
    ):
        self.estado = estado
        self.logger = logger
        self.func_receber = func_receber
        self.prob_perda_ack = prob_perda_ack

        self.dir_logs = Path(dir_logs)
        self.dir_logs.mkdir(parents=True, exist_ok=True)
        self.caminho_registro = self.dir_logs / f"registro_{estado.meu_id}.jsonl"

        self._lock = threading.Lock()
        self._cond_ocioso = threading.Condition(self._lock)
        self._concluidas_local: set[tuple[int, int]] = set()
        self._em_andamento: dict[tuple[int, int], threading.Event] = {}
        self._resultado_andamento: dict[tuple[int, int], bool] = {}

        self._carregar_registro_local()
        self._arquivo_reg = self.caminho_registro.open("a", encoding="utf-8")

    def _carregar_registro_local(self) -> None:
        if not self.caminho_registro.exists():
            return
        with self.caminho_registro.open("r", encoding="utf-8") as arq:
            for linha in arq:
                linha = linha.strip()
                if not linha:
                    continue
                try:
                    reg = json.loads(linha)
                    chave = (int(reg["sender"]), int(reg["msg_id"]))
                    self._concluidas_local.add(chave)
                    self.estado.marcar_processada(chave, int(reg["dest"]))
                except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                    continue
                
    def _persistir_local(self, remetente: int, id_msg: int, destinatario: int) -> None:
        registro = {
            "ts": round(time.time(), 6),
            "sender": remetente,
            "msg_id": id_msg,
            "dest": destinatario,
        }
        self._arquivo_reg.write(json.dumps(registro, ensure_ascii=True, separators=(",", ":")) + "\n")
        self._arquivo_reg.flush()

    def tratar_deliver(self, linha: str) -> tuple[str, bool]:
        id_msg, remetente, destinatario, conteudo = interpretar_deliver(linha)

        if self.estado.servidor_de(destinatario) != self.estado.meu_id:
            return formatar_erro("NOT_HERE"), False

        chave = (remetente, id_msg)
        evento_espera: threading.Event | None = None

        with self._lock:
            if chave in self._concluidas_local or self.estado.ja_processada(chave):
                acao = "CONCLUIDA"
            elif chave in self._em_andamento:
                acao = "AGUARDAR"
                evento_espera = self._em_andamento[chave]
            else:
                acao = "INICIAR"
                self._em_andamento[chave] = threading.Event()

        if acao == "CONCLUIDA":
            self.logger.registrar(
                "ack_reenviado", valor=1, componente="servidor", sender=remetente, msg_id=id_msg, dest=destinatario
            )
            return formatar_ack(id_msg), False

        if acao == "AGUARDAR":
            assert evento_espera is not None
            evento_espera.wait(timeout=TIMEOUT_ENTREGA_SEG + 1.0)
            with self._lock:
                ok = self._resultado_andamento.get(
                    chave, chave in self._concluidas_local or self.estado.ja_processada(chave)
                )
            if ok:
                self.logger.registrar(
                    "ack_reenviado", valor=1, componente="servidor", sender=remetente, msg_id=id_msg, dest=destinatario
                )
                return formatar_ack(id_msg), False
            return formatar_erro("PROCESSING"), False

        try:
            ok = bool(self.func_receber(conteudo))
        except Exception:
            ok = False

        with self._cond_ocioso:
            self._resultado_andamento[chave] = ok
            if ok:
                self._concluidas_local.add(chave)
                self._persistir_local(remetente, id_msg, destinatario)
            evt = self._em_andamento.pop(chave, None)
            if evt is not None:
                evt.set()
            if not self._em_andamento:
                self._cond_ocioso.notify_all()

        if not ok:
            return formatar_erro("PROCESSING"), False

        self.logger.registrar(
            "msg_processada", valor=1, componente="servidor", sender=remetente, msg_id=id_msg, dest=destinatario
        )

        if self.prob_perda_ack > 0.0 and random.random() < self.prob_perda_ack:
            raise ConnectionResetError("CHAOS_DROP_ACK")

        return formatar_ack(id_msg), False

    def aguardar_ocioso(self, timeout: float = TIMEOUT_DRENAGEM_SEG) -> bool:
        prazo = time.monotonic() + timeout
        with self._cond_ocioso:
            while self._em_andamento:
                restante = prazo - time.monotonic()
                if restante <= 0:
                    return False
                self._cond_ocioso.wait(timeout=restante)
            return True

    def fechar(self) -> None:
        with self._lock:
            if not self._arquivo_reg.closed:
                self._arquivo_reg.close()


class Roteador:
    """Encaminha MSG_ no coordenador até o servidor atual do destinatário e replica MSG_DONE."""

    def __init__(
        self,
        coordenador,
        logger: LoggerMetricas,
        func_entrega: Callable[[str, int, str, float], str] = enviar_e_receber,
    ):
        self.coordenador = coordenador
        self.estado: Estado = coordenador.estado
        self.logger = logger
        self.func_entrega = func_entrega

    def tratar_msg(self, linha: str) -> tuple[str, bool]:
        if not self.estado.eh_coordenador:
            return formatar_erro("NOT_COORDINATOR"), False

        id_msg, remetente, destinatario, conteudo = interpretar_msg(linha)

        if not self.estado.possui_usuario(remetente):
            return formatar_erro("UNKNOWN_USER"), False
        if not self.estado.possui_usuario(destinatario):
            return formatar_erro("UNKNOWN_DEST"), False

        chave = (remetente, id_msg)
        if self.estado.ja_processada(chave):
            self.logger.registrar(
                "ack_reenviado", valor=1, componente="servidor", sender=remetente, msg_id=id_msg, dest=destinatario
            )
            return formatar_ack(id_msg), False

        prazo = time.monotonic() + PRAZO_ROTEAMENTO_SEG
        ack_esperado = formatar_ack(id_msg)
        linha_deliver = formatar_deliver(id_msg, remetente, destinatario, conteudo)
        entregue = False

        while time.monotonic() < prazo:
            id_destino = self.estado.servidor_de(destinatario)
            endereco = self.estado.endereco_de(id_destino) if id_destino is not None else None
            if id_destino is None or not self.estado.servidor_ativo(id_destino) or endereco is None:
                time.sleep(0.2)
                continue

            ip, porta = endereco
            restante = max(0.1, min(TIMEOUT_ENTREGA_SEG, prazo - time.monotonic()))
            try:
                resposta = self.func_entrega(ip, porta, linha_deliver, restante)
            except OSError:
                self.logger.registrar(
                    "msg_reencaminhada", valor=1, sender=remetente, msg_id=id_msg, dest=destinatario, motivo="NETWORK"
                )
                time.sleep(0.1)
                continue

            if resposta == ack_esperado:
                entregue = True
                break

            if resposta == "ERROR_NOT_HERE":
                self.logger.registrar(
                    "msg_reencaminhada", valor=1, sender=remetente, msg_id=id_msg, dest=destinatario, motivo="NOT_HERE"
                )
                time.sleep(0.05)
                continue

            if resposta and resposta.startswith("ERROR_"):
                return resposta, False

        if not entregue:
            return formatar_erro("DEST_UNAVAILABLE"), False

        eh_nova = self.estado.marcar_processada(chave, destinatario)
        if eh_nova:
            with self.coordenador._lock_op:
                seq = self.coordenador._emitir("MSG_DONE", sender=remetente, msg_id=id_msg, dest=destinatario)
            self.coordenador._replicar(seq)

        return ack_esperado, False


class AgenteUsuario:
    """Thread autônoma que representa um usuário hospedado no servidor local."""

    def __init__(
        self,
        id_usuario: int,
        estado: Estado,
        logger: LoggerMetricas,
        func_envio: Callable[[str, int, str, float], str] = enviar_e_receber,
        intervalo_max: float = INTERVALO_MAX_MSG_SEG,
        atraso_inicial: float = 0.0,
    ):
        self.id_usuario = id_usuario
        self.estado = estado
        self.logger = logger
        self.func_envio = func_envio
        self.intervalo_max = intervalo_max
        self.atraso_inicial = atraso_inicial

        self._ultimo_id_msg = 0
        self._contador = 0
        self._parar = threading.Event()
        self._thread = threading.Thread(target=self._executar, name=f"Usuario-{id_usuario}", daemon=True)

    def iniciar(self) -> None:
        self._thread.start()

    def parar(self) -> None:
        self._parar.set()

    def aguardar_fim(self, timeout: float | None = None) -> None:
        self._thread.join(timeout=timeout)

    def proximo_id_msg(self) -> int:
        agora_ms = int(time.time() * 1000)
        candidato = agora_ms * 1000 + (self._contador % 1000)
        self._contador += 1
        self._ultimo_id_msg = max(self._ultimo_id_msg + 1, candidato)
        return self._ultimo_id_msg

    def _executar(self) -> None:
        if self.atraso_inicial > 0 and self._parar.wait(timeout=self.atraso_inicial):
            return

        while not self._parar.is_set():
            espera = random.uniform(0.05, max(0.05, self.intervalo_max))
            if self._parar.wait(timeout=espera):
                break

            pares = [uid for uid in self.estado.todos_ids_usuarios() if uid != self.id_usuario]
            if not pares:
                continue

            destinatario = random.choice(pares)
            conteudo = random.choice(_MENSAGENS_EXEMPLO)
            id_msg = self.proximo_id_msg()
            linha_msg = formatar_msg(id_msg, self.id_usuario, destinatario, conteudo)
            ack_esperado = formatar_ack(id_msg)

            t0 = time.monotonic()
            self.logger.registrar(
                "msg_enviada",
                valor=1,
                componente="usuario",
                id_no=self.id_usuario,
                msg_id=id_msg,
                dest=destinatario,
            )

            confirmada = False
            for tentativa in range(1, MAX_TENTATIVAS_MSG + 1):
                if tentativa > 1:
                    self.logger.registrar(
                        "msg_reenvio",
                        valor=1,
                        componente="usuario",
                        id_no=self.id_usuario,
                        msg_id=id_msg,
                        dest=destinatario,
                        tentativa=tentativa,
                    )

                ip_coord, porta_coord = self.estado.endereco_coordenador
                try:
                    resposta = self.func_envio(ip_coord, porta_coord, linha_msg, TIMEOUT_ACK_MSG_SEG)
                except OSError:
                    resposta = "ERROR_DEST_UNAVAILABLE"

                if resposta == ack_esperado:
                    latencia = round(time.monotonic() - t0, 4)
                    self.logger.registrar(
                        "msg_confirmada",
                        valor=latencia,
                        componente="usuario",
                        id_no=self.id_usuario,
                        msg_id=id_msg,
                        dest=destinatario,
                        tentativas=tentativa,
                    )
                    confirmada = True
                    break

                if resposta == "ERROR_UNKNOWN_DEST":
                    self.logger.registrar(
                        "msg_descartada",
                        valor=1,
                        componente="usuario",
                        id_no=self.id_usuario,
                        msg_id=id_msg,
                        dest=destinatario,
                        motivo=resposta,
                    )
                    confirmada = True
                    break

                if resposta not in _ERROS_REENVIO:
                    break

                time.sleep(min(1.5, 0.15 * tentativa))

            if not confirmada:
                self.logger.registrar(
                    "msg_falhou",
                    valor=1,
                    componente="usuario",
                    id_no=self.id_usuario,
                    msg_id=id_msg,
                    dest=destinatario,
                )


class GerenciadorUsuarios:
    """Sincroniza as threads de AgenteUsuario com os usuários hospedados no nó local."""

    def __init__(
        self,
        estado: Estado,
        logger: LoggerMetricas,
        evento_acordar: threading.Event | None = None,
        func_envio: Callable[[str, int, str, float], str] = enviar_e_receber,
        intervalo_max: float = INTERVALO_MAX_MSG_SEG,
    ):
        self.estado = estado
        self.logger = logger
        self.evento_acordar = evento_acordar or threading.Event()
        self.func_envio = func_envio
        self.intervalo_max = intervalo_max

        self._lock = threading.Lock()
        self._agentes: dict[int, AgenteUsuario] = {}
        self._parar = threading.Event()
        self._thread = threading.Thread(target=self._laco, name="GerenciadorUsuarios", daemon=True)

    def iniciar(self) -> None:
        self._thread.start()

    def notificar(self) -> None:
        self.evento_acordar.set()

    def reconciliar(self) -> None:
        desejados = set(self.estado.usuarios_do_servidor(self.estado.meu_id))
        iniciar_lista: list[AgenteUsuario] = []
        parar_lista: list[AgenteUsuario] = []

        with self._lock:
            atuais = set(self._agentes.keys())
            for uid in sorted(desejados - atuais):
                atraso = random.uniform(0.05, min(0.5, self.intervalo_max))
                agente = AgenteUsuario(
                    id_usuario=uid,
                    estado=self.estado,
                    logger=self.logger,
                    func_envio=self.func_envio,
                    intervalo_max=self.intervalo_max,
                    atraso_inicial=atraso,
                )
                self._agentes[uid] = agente
                iniciar_lista.append(agente)

            for uid in sorted(atuais - desejados):
                parar_lista.append(self._agentes.pop(uid))

        for ag in iniciar_lista:
            ag.iniciar()
        for ag in parar_lista:
            ag.parar()

    def _laco(self) -> None:
        while not self._parar.is_set():
            self.evento_acordar.wait(timeout=0.5)
            self.evento_acordar.clear()
            if self._parar.is_set():
                break
            self.reconciliar()

    def parar(self, drenar: bool = True, timeout: float = TIMEOUT_DRENAGEM_SEG) -> None:
        self._parar.set()
        self.evento_acordar.set()
        self._thread.join(timeout=1.0)

        with self._lock:
            agentes = list(self._agentes.values())
            self._agentes.clear()

        for ag in agentes:
            ag.parar()

        if drenar:
            prazo = time.monotonic() + timeout
            for ag in agentes:
                restante = max(0.05, prazo - time.monotonic())
                ag.aguardar_fim(timeout=restante)