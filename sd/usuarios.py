import random
import threading
import time
from typing import Callable

from sd.common import config
from sd.common.logger import MetricsLogger
from sd.common.net import send_and_recv
from sd.common.protocolo import fmt_ack, fmt_msg
from sd.estado import Estado

_SAMPLE_MESSAGES = [
    "Ola, tudo bem por ai?",
    "Confirmando recebimento do relatorio.",
    "Teste de acentuacao: acao, coracao, Sao Paulo!",
    "Mensagem com_underscores_e_simbolos_100%!",
    "Linha 1\nLinha 2 com quebra escapada",
    "Ping distribuido entre nos ativos.",
]

_RETRYABLE_ERRORS = frozenset({
    "ERROR_NOT_COORDINATOR",
    "ERROR_DEST_UNAVAILABLE",
    "ERROR_BUSY",
})


class AgenteUsuario:
    """Thread autônoma que representa um usuário hospedado no servidor local."""

    def __init__(
        self,
        user_id: int,
        state: Estado,
        logger: MetricsLogger,
        send_fn: Callable[[str, int, str, float], str] = send_and_recv,
        max_interval: float = config.MSG_MAX_INTERVAL_SEC,
        initial_delay: float = 0.0,
    ):
        self.user_id = user_id
        self.state = state
        self.logger = logger
        self.send_fn = send_fn
        self.max_interval = max_interval
        self.initial_delay = initial_delay

        self._last_msg_id = 0
        self._counter = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name=f"Usuario-{user_id}", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def join(self, timeout: float | None = None) -> None:
        self._thread.join(timeout=timeout)

    def next_msg_id(self) -> int:
        """Gera ID monotônico de 16 dígitos (ms * 1000 + contador % 1000) seguro contra migração."""
        now_ms = int(time.time() * 1000)
        candidate = now_ms * 1000 + (self._counter % 1000)
        self._counter += 1
        self._last_msg_id = max(self._last_msg_id + 1, candidate)
        return self._last_msg_id

    def _run(self) -> None:
        if self.initial_delay > 0:
            if self._stop.wait(timeout=self.initial_delay):
                return

        while not self._stop.is_set():
            wait_t = random.uniform(0.05, max(0.05, self.max_interval))
            if self._stop.wait(timeout=wait_t):
                break

            peers = [uid for uid in self.state.all_user_ids() if uid != self.user_id]
            if not peers:
                continue

            dest = random.choice(peers)
            content = random.choice(_SAMPLE_MESSAGES)
            msg_id = self.next_msg_id()
            wire = fmt_msg(msg_id, self.user_id, dest, content)
            expected_ack = fmt_ack(msg_id)

            t0 = time.monotonic()
            self.logger.log(
                "msg_enviada",
                valor=1,
                componente="usuario",
                node_id=self.user_id,
                msg_id=msg_id,
                dest=dest,
            )

            confirmed = False
            for attempt in range(1, config.MSG_MAX_ATTEMPTS + 1):
                if attempt > 1:
                    self.logger.log(
                        "msg_reenvio",
                        valor=1,
                        componente="usuario",
                        node_id=self.user_id,
                        msg_id=msg_id,
                        dest=dest,
                        tentativa=attempt,
                    )

                coord_ip, coord_port = self.state.coordinator_addr
                try:
                    reply = self.send_fn(coord_ip, coord_port, wire, config.MSG_ACK_TIMEOUT_SEC)
                except OSError:
                    reply = "ERROR_DEST_UNAVAILABLE"

                if reply == expected_ack:
                    latency = round(time.monotonic() - t0, 4)
                    self.logger.log(
                        "msg_confirmada",
                        valor=latency,
                        componente="usuario",
                        node_id=self.user_id,
                        msg_id=msg_id,
                        dest=dest,
                        tentativas=attempt,
                    )
                    confirmed = True
                    break

                if reply == "ERROR_UNKNOWN_DEST":
                    self.logger.log(
                        "msg_descartada",
                        valor=1,
                        componente="usuario",
                        node_id=self.user_id,
                        msg_id=msg_id,
                        dest=dest,
                        motivo=reply,
                    )
                    confirmed = True  # não conta como falha de entrega
                    break

                if reply not in _RETRYABLE_ERRORS:
                    break

                backoff = min(1.5, 0.15 * attempt)
                time.sleep(backoff)

            if not confirmed:
                self.logger.log(
                    "msg_falhou",
                    valor=1,
                    componente="usuario",
                    node_id=self.user_id,
                    msg_id=msg_id,
                    dest=dest,
                )


class GerenciadorUsuarios:
    """Reconcilia as threads de AgenteUsuario com os usuários hospedados em my_id."""

    def __init__(
        self,
        state: Estado,
        logger: MetricsLogger,
        wake_event: threading.Event | None = None,
        send_fn: Callable[[str, int, str, float], str] = send_and_recv,
        max_interval: float = config.MSG_MAX_INTERVAL_SEC,
    ):
        self.state = state
        self.logger = logger
        self.wake_event = wake_event or threading.Event()
        self.send_fn = send_fn
        self.max_interval = max_interval

        self._lock = threading.Lock()
        self._agents: dict[int, AgenteUsuario] = {}
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, name="GerenciadorUsuarios", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def notify(self) -> None:
        self.wake_event.set()

    def reconcile(self) -> None:
        desired = set(self.state.users_of(self.state.my_id))
        to_start: list[AgenteUsuario] = []
        to_stop: list[AgenteUsuario] = []

        with self._lock:
            current = set(self._agents.keys())
            for uid in sorted(desired - current):
                delay = random.uniform(0.05, min(0.5, self.max_interval))
                agent = AgenteUsuario(
                    user_id=uid,
                    state=self.state,
                    logger=self.logger,
                    send_fn=self.send_fn,
                    max_interval=self.max_interval,
                    initial_delay=delay,
                )
                self._agents[uid] = agent
                to_start.append(agent)

            for uid in sorted(current - desired):
                agent = self._agents.pop(uid)
                to_stop.append(agent)

        for ag in to_start:
            ag.start()
        for ag in to_stop:
            ag.stop()

    def _loop(self) -> None:
        while not self._stop.is_set():
            self.wake_event.wait(timeout=0.5)
            self.wake_event.clear()
            if self._stop.is_set():
                break
            self.reconcile()

    def stop(self, drain: bool = True, timeout: float = config.DRAIN_TIMEOUT_SEC) -> None:
        self._stop.set()
        self.wake_event.set()
        self._thread.join(timeout=1.0)

        with self._lock:
            agents = list(self._agents.values())
            self._agents.clear()

        for ag in agents:
            ag.stop()

        if drain:
            deadline = time.monotonic() + timeout
            for ag in agents:
                rem = max(0.05, deadline - time.monotonic())
                ag.join(timeout=rem)