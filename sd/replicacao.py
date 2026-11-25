import json
import socket
import threading
import time
from pathlib import Path
from typing import Callable

from sd import config
from sd.config import (
    ATRASO_RECONEXAO_REPL_SEG,
    DIRETORIO_LOGS,
    ESPERA_REPLICACAO_SEG,
    TIMEOUT_LINK_REPL_SEG,
    ErroProtocolo,
    LeitorLinha,
    LoggerMetricas,
    enviar_linha,
    formatar_digest,
    formatar_repl,
    interpretar_repl,
)
from sd.estado import Estado


class LogOperacoes:
    """Log ordenado de operações em memória e persistido em JSONL."""

    def __init__(self, id_no: int, dir_logs: Path = DIRETORIO_LOGS):
        self.id_no = id_no
        self.dir_logs = Path(dir_logs)
        self.dir_logs.mkdir(parents=True, exist_ok=True)
        self.caminho = self.dir_logs / f"oplog_{id_no}.jsonl"

        if self.caminho.exists():
            backup = self.dir_logs / f"oplog_{id_no}_{int(time.time())}.bak.jsonl"
            try:
                self.caminho.rename(backup)
            except OSError:
                pass

        self._lock = threading.Lock()
        self._operacoes: list[dict] = []
        self._arquivo = self.caminho.open("a", encoding="utf-8")

    def anexar(self, tipo: str, dados: dict, epoca: int = 1) -> dict:
        with self._lock:
            seq = len(self._operacoes) + 1
            operacao = {
                "seq": seq,
                "epoch": epoca,
                "ts": round(time.time(), 6),
                "type": tipo,
                "data": dados,
            }
            self._operacoes.append(operacao)
            self._arquivo.write(json.dumps(operacao, ensure_ascii=True, separators=(",", ":")) + "\n")
            self._arquivo.flush()
            return operacao

    def gravar_replicada(self, operacao: dict) -> None:
        with self._lock:
            self._operacoes.append(operacao)
            self._arquivo.write(json.dumps(operacao, ensure_ascii=True, separators=(",", ":")) + "\n")
            self._arquivo.flush()

    def topo(self) -> int:
        with self._lock:
            return len(self._operacoes)

    def obter_a_partir(self, seq: int) -> list[dict]:
        with self._lock:
            if seq <= 1:
                return list(self._operacoes)
            return list(self._operacoes[seq - 1 :])

    def obter_em(self, seq: int) -> dict | None:
        with self._lock:
            if 1 <= seq <= len(self._operacoes):
                return self._operacoes[seq - 1]
            return None

    def fechar(self) -> None:
        with self._lock:
            if not self._arquivo.closed:
                self._arquivo.close()


class Aplicador:
    """Aplica operações REPL_<json> no estado do seguidor na ordem exata de seq."""

    def __init__(self, estado: Estado, log_ops: LogOperacoes, logger: LoggerMetricas, epoca: int = 1):
        self.estado = estado
        self.log_ops = log_ops
        self.logger = logger
        self.epoca = epoca
        self.ultimo_seq = 0
        self.primeiro_repl_ts: float | None = None
        self.evento_aplicado = threading.Event()
        self._lock = threading.Lock()

    def tratar_repl(self, linha: str) -> tuple[str, bool]:
        operacao = interpretar_repl(linha)
        epoca_op = operacao["epoch"]
        seq = operacao["seq"]
        eh_recuperacao = bool(operacao.pop("recovery", False))

        with self._lock:
            if self.primeiro_repl_ts is None:
                self.primeiro_repl_ts = time.monotonic()

            if epoca_op < self.epoca:
                return f"ERROR_STALE_EPOCH_{self.epoca}", False

            if seq <= self.ultimo_seq:
                return f"ACK_REPL_{self.ultimo_seq}", False

            if seq > self.ultimo_seq + 1:
                return f"ERROR_GAP_{self.ultimo_seq}", False

            if not self.estado.aplicar_operacao(operacao):
                raise ErroProtocolo("INVALID_FORMAT", f"Operação inválida: {operacao.get('type')}")

            self.log_ops.gravar_replicada(operacao)
            self.ultimo_seq = seq

        if not eh_recuperacao:
            atraso = max(0.0, round(time.time() - float(operacao["ts"]), 6))
            self.logger.registrar("atraso_replicacao", valor=atraso, seq=seq, tipo=operacao["type"])

        self.evento_aplicado.set()
        return f"ACK_REPL_{seq}", False

    def tratar_digest(self, _linha: str) -> tuple[str, bool]:
        with self._lock:
            seq = self.ultimo_seq
            crc = self.estado.resumo_crc()
        return formatar_digest(seq, crc), False


class EloReplicacao:
    """Thread dedicada para enviar o LogOperacoes continuamente a um seguidor."""

    def __init__(
        self,
        id_seguidor: int,
        ip: str,
        porta: int,
        log_ops: LogOperacoes,
        seq_alvo_sinc: int,
        ao_confirmar_cb: Callable[[int, int, str], None],
        ao_epoca_antiga_cb: Callable[[int], None],
        logger: LoggerMetricas | None = None,
        func_envio: Callable[[str, int, str], str] | None = None,
    ):
        self.id_seguidor = id_seguidor
        self.ip = ip
        self.porta = porta
        self.log_ops = log_ops
        self.seq_alvo_sinc = seq_alvo_sinc
        self.ao_confirmar_cb = ao_confirmar_cb
        self.ao_epoca_antiga_cb = ao_epoca_antiga_cb
        self.logger = logger
        self.func_envio = func_envio

        self.cursor = 1
        self.seq_confirmado = 0
        self.situacao = "SYNCED" if seq_alvo_sinc == 0 else "CATCHING_UP"
        self._erro_inesperado_registrado = False

        self._acordar = threading.Event()
        self._parar = threading.Event()
        self._sock: socket.socket | None = None
        self._leitor: LeitorLinha | None = None
        self._thread = threading.Thread(
            target=self._executar, name=f"EloReplicacao-{id_seguidor}", daemon=True
        )

    def iniciar(self) -> None:
        self._thread.start()

    def parar(self) -> None:
        self._parar.set()
        self._acordar.set()
        self._fechar_socket()
        self._thread.join(timeout=1.5)

    def notificar(self) -> None:
        self._acordar.set()

    def _fechar_socket(self) -> None:
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None
            self._leitor = None

    def _enviar_rede(self, linha: str) -> str:
        if self.func_envio is not None:
            return self.func_envio(self.ip, self.porta, linha)

        if self._sock is None or self._leitor is None:
            sock = socket.create_connection((self.ip, self.porta), timeout=TIMEOUT_LINK_REPL_SEG)
            self._sock = sock
            self._leitor = LeitorLinha(sock)

        enviar_linha(self._sock, linha)
        resposta = self._leitor.ler_linha(timeout=TIMEOUT_LINK_REPL_SEG)
        if resposta is None:
            self._fechar_socket()
            sock = socket.create_connection((self.ip, self.porta), timeout=TIMEOUT_LINK_REPL_SEG)
            self._sock = sock
            self._leitor = LeitorLinha(sock)
            enviar_linha(self._sock, linha)
            resposta = self._leitor.ler_linha(timeout=TIMEOUT_LINK_REPL_SEG)
            if resposta is None:
                raise ConnectionResetError("EOF após reconexão de replicação")
        return resposta

    def _executar(self) -> None:
        while not self._parar.is_set():
            operacao = self.log_ops.obter_em(self.cursor)
            if operacao is None:
                self._acordar.wait(timeout=0.25)
                self._acordar.clear()
                continue

            if self.cursor <= self.seq_alvo_sinc:
                op_envio = {**operacao, "recovery": True}
            else:
                op_envio = operacao

            try:
                resposta = self._enviar_rede(formatar_repl(op_envio))
            except (OSError, ValueError):
                self._fechar_socket()
                self._parar.wait(timeout=ATRASO_RECONEXAO_REPL_SEG)
                continue

            if resposta.startswith("ACK_REPL_"):
                try:
                    n = int(resposta.split("_")[-1])
                except ValueError:
                    n = -1
                if n >= self.cursor:
                    self._erro_inesperado_registrado = False
                    self.cursor = n + 1
                    self.seq_confirmado = max(self.seq_confirmado, n)
                    if self.situacao in ("CATCHING_UP", "LAGGING") and self.seq_confirmado >= self.seq_alvo_sinc:
                        self.situacao = "SYNCED"
                    self.ao_confirmar_cb(self.id_seguidor, self.seq_confirmado, self.situacao)
                    continue

            if resposta.startswith("ERROR_GAP_"):
                self._erro_inesperado_registrado = False
                try:
                    ultimo = int(resposta.split("_")[-1])
                    self.cursor = max(1, ultimo + 1)
                except ValueError:
                    self.cursor = 1
                continue

            if resposta.startswith("ERROR_STALE_EPOCH"):
                self.ao_epoca_antiga_cb(self.id_seguidor)
                break

            if not self._erro_inesperado_registrado and self.logger is not None:
                self._erro_inesperado_registrado = True
                self.logger.registrar(
                    "erro_replicacao_resposta",
                    valor=1,
                    seguidor=self.id_seguidor,
                    cursor=self.cursor,
                    resposta=resposta[:80],
                )
            self._parar.wait(timeout=ATRASO_RECONEXAO_REPL_SEG)


class Replicador:
    """Gerencia o LogOperacoes do líder e coordena os elos com os seguidores."""

    def __init__(
        self,
        log_ops: LogOperacoes,
        logger: LoggerMetricas,
        epoca: int = 1,
        func_envio: Callable[[str, int, str], str] | None = None,
    ):
        self.log_ops = log_ops
        self.logger = logger
        self.epoca = epoca
        self.func_envio = func_envio
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._elos: dict[int, EloReplicacao] = {}
        self.epoca_obsoleta_detectada = False

    def anexar_sob_lock(self, tipo: str, dados: dict) -> int:
        operacao = self.log_ops.anexar(tipo, dados, epoca=self.epoca)
        with self._lock:
            elos = list(self._elos.values())
        for elo in elos:
            elo.notificar()
        return operacao["seq"]

    def adicionar_seguidor(self, id_servidor: int, ip: str, porta: int) -> None:
        with self._lock:
            antigo = self._elos.pop(id_servidor, None)
        if antigo is not None:
            antigo.parar()

        topo_seq = self.log_ops.topo()
        elo = EloReplicacao(
            id_seguidor=id_servidor,
            ip=ip,
            porta=porta,
            log_ops=self.log_ops,
            seq_alvo_sinc=topo_seq,
            ao_confirmar_cb=self._ao_confirmar_elo,
            ao_epoca_antiga_cb=self._ao_epoca_antiga,
            logger=self.logger,
            func_envio=self.func_envio,
        )
        with self._lock:
            self._elos[id_servidor] = elo
        elo.iniciar()
        elo.notificar()

    def remover_seguidor(self, id_servidor: int) -> None:
        with self._lock:
            elo = self._elos.pop(id_servidor, None)
            self._cond.notify_all()
        if elo is not None:
            elo.parar()

    def _ao_confirmar_elo(self, _id_seguidor: int, _seq: int, _situacao: str) -> None:
        with self._cond:
            self._cond.notify_all()

    def _ao_epoca_antiga(self, _id_seguidor: int) -> None:
        with self._cond:
            self.epoca_obsoleta_detectada = True
            self._cond.notify_all()

    def aguardar_replicacao(self, seq: int, timeout: float = ESPERA_REPLICACAO_SEG) -> dict:
        t0 = time.monotonic()
        prazo = t0 + max(0.0, timeout)

        with self._cond:
            while True:
                sincronizados = [elo for elo in self._elos.values() if elo.situacao == "SYNCED"]
                if not sincronizados or all(elo.seq_confirmado >= seq for elo in sincronizados):
                    break

                restante = prazo - time.monotonic()
                if restante <= 0:
                    for elo in sincronizados:
                        if elo.seq_confirmado < seq:
                            elo.situacao = "LAGGING"
                            elo.seq_alvo_sinc = max(elo.seq_alvo_sinc, seq)
                    break

                self._cond.wait(timeout=restante)

            qtd_sinc = sum(
                1 for elo in self._elos.values() if elo.situacao == "SYNCED" and elo.seq_confirmado >= seq
            )
            qtd_atrasados = sum(1 for elo in self._elos.values() if elo.situacao == "LAGGING")

        decorrido = round(time.monotonic() - t0, 4)
        return {
            "seq": seq,
            "sincronizados": qtd_sinc,
            "atrasados": qtd_atrasados,
            "decorrido": decorrido,
        }

    def parar(self) -> None:
        with self._lock:
            elos = list(self._elos.values())
            self._elos.clear()
        for elo in elos:
            elo.parar()