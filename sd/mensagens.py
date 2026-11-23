import json
import random
import threading
import time
from pathlib import Path
from typing import Callable

from sd.common import config
from sd.common.logger import MetricsLogger
from sd.common.net import send_and_recv
from sd.common.protocolo import (
    fmt_ack,
    fmt_deliver,
    fmt_error,
    parse_deliver,
    parse_msg,
)
from sd.coordenador import Coordenador
from sd.estado import Estado


def default_receive_msg(content: str | None) -> bool:
    """Função exigida pelo PDF: retorna False se None, senão dorme 1 s e retorna True."""
    if content is None:
        return False
    time.sleep(config.RECEIVE_MSG_DELAY_SEC)
    return True


class Receptor:
    """Executa receive_msg no servidor de destino com deduplicação local + replicada."""

    def __init__(
        self,
        state: Estado,
        logger: MetricsLogger,
        log_dir: Path = config.LOGS_DIR,
        receive_fn: Callable[[str | None], bool] = default_receive_msg,
        chaos_drop_prob: float = config.CHAOS_DROP_ACK_PROB,
    ):
        self.state = state
        self.logger = logger
        self.receive_fn = receive_fn
        self.chaos_drop_prob = chaos_drop_prob

        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.reg_path = self.log_dir / f"registro_{state.my_id}.jsonl"
        self._reg_file = self.reg_path.open("a", encoding="utf-8")

        self._lock = threading.Lock()
        self._idle_cond = threading.Condition(self._lock)
        self._local_done: set[tuple[int, int]] = set()
        self._inflight: dict[tuple[int, int], threading.Event] = {}
        self._inflight_ok: dict[tuple[int, int], bool] = {}

    def _persist_local(self, sender: int, msg_id: int, dest: int) -> None:
        entry = {
            "ts": round(time.time(), 6),
            "sender": sender,
            "msg_id": msg_id,
            "dest": dest,
        }
        self._reg_file.write(json.dumps(entry, ensure_ascii=True, separators=(",", ":")) + "\n")
        self._reg_file.flush()

    def handle_deliver(self, line: str) -> tuple[str, bool]:
        msg_id, sender, dest, content = parse_deliver(line)

        if self.state.server_of(dest) != self.state.my_id:
            return fmt_error("NOT_HERE"), False

        key = (sender, msg_id)
        wait_evt: threading.Event | None = None

        with self._lock:
            if key in self._local_done or self.state.is_processed(key):
                action = "DONE"
            elif key in self._inflight:
                action = "WAIT"
                wait_evt = self._inflight[key]
            else:
                action = "START"
                self._inflight[key] = threading.Event()

        if action == "DONE":
            self.logger.log("ack_reenviado", valor=1, componente="servidor", sender=sender, msg_id=msg_id, dest=dest)
            return fmt_ack(msg_id), False

        if action == "WAIT":
            assert wait_evt is not None
            wait_evt.wait(timeout=config.DELIVER_TIMEOUT_SEC + 1.0)
            with self._lock:
                ok = self._inflight_ok.get(key, key in self._local_done or self.state.is_processed(key))
            if ok:
                self.logger.log("ack_reenviado", valor=1, componente="servidor", sender=sender, msg_id=msg_id, dest=dest)
                return fmt_ack(msg_id), False
            return fmt_error("PROCESSING"), False

        # action == "START" (fora de qualquer lock)
        try:
            ok = bool(self.receive_fn(content))
        except Exception:
            ok = False

        with self._idle_cond:
            self._inflight_ok[key] = ok
            if ok:
                self._local_done.add(key)
                self._persist_local(sender, msg_id, dest)
            evt = self._inflight.pop(key, None)
            if evt is not None:
                evt.set()
            if not self._inflight:
                self._idle_cond.notify_all()

        if not ok:
            return fmt_error("PROCESSING"), False

        self.logger.log("msg_processada", valor=1, componente="servidor", sender=sender, msg_id=msg_id, dest=dest)

        # Injeção controlada de falha: processou, mas fecha conexão sem devolver ACK
        if self.chaos_drop_prob > 0.0 and random.random() < self.chaos_drop_prob:
            raise ConnectionResetError("CHAOS_DROP_ACK")

        return fmt_ack(msg_id), False

    def wait_idle(self, timeout: float = config.DRAIN_TIMEOUT_SEC) -> bool:
        deadline = time.monotonic() + timeout
        with self._idle_cond:
            while self._inflight:
                rem = deadline - time.monotonic()
                if rem <= 0:
                    return False
                self._idle_cond.wait(timeout=rem)
            return True

    def close(self) -> None:
        with self._lock:
            if not self._reg_file.closed:
                self._reg_file.close()


class Roteador:
    """Roteia MSG_ no coordenador até o servidor atual do destinatário e replica MSG_DONE."""

    def __init__(
        self,
        coordenador: Coordenador,
        logger: MetricsLogger,
        deliver_fn: Callable[[str, int, str, float], str] = send_and_recv,
    ):
        self.coordenador = coordenador
        self.state = coordenador.state
        self.logger = logger
        self.deliver_fn = deliver_fn

    def handle_msg(self, line: str) -> tuple[str, bool]:
        if not self.state.is_coordinator:
            return fmt_error("NOT_COORDINATOR"), False

        msg_id, sender, dest, content = parse_msg(line)

        if not self.state.has_user(sender):
            return fmt_error("UNKNOWN_USER"), False
        if not self.state.has_user(dest):
            return fmt_error("UNKNOWN_DEST"), False

        key = (sender, msg_id)
        if self.state.is_processed(key):
            self.logger.log("ack_reenviado", valor=1, componente="servidor", sender=sender, msg_id=msg_id, dest=dest)
            return fmt_ack(msg_id), False

        deadline = time.monotonic() + config.ROUTE_DEADLINE_SEC
        expected_ack = fmt_ack(msg_id)
        deliver_wire = fmt_deliver(msg_id, sender, dest, content)
        delivered = False

        while time.monotonic() < deadline:
            target_sid = self.state.server_of(dest)
            addr = self.state.address_of(target_sid) if target_sid is not None else None
            if target_sid is None or not self.state.is_alive(target_sid) or addr is None:
                time.sleep(0.2)
                continue

            ip, port = addr
            rem = max(0.1, min(config.DELIVER_TIMEOUT_SEC, deadline - time.monotonic()))
            try:
                reply = self.deliver_fn(ip, port, deliver_wire, rem)
            except OSError:
                self.logger.log("msg_reencaminhada", valor=1, sender=sender, msg_id=msg_id, dest=dest, motivo="NETWORK")
                time.sleep(0.1)
                continue

            if reply == expected_ack:
                delivered = True
                break

            if reply == "ERROR_NOT_HERE":
                self.logger.log("msg_reencaminhada", valor=1, sender=sender, msg_id=msg_id, dest=dest, motivo="NOT_HERE")
                time.sleep(0.05)
                continue

            if reply.startswith("ERROR_"):
                return reply, False

        if not delivered:
            return fmt_error("DEST_UNAVAILABLE"), False

        is_new = self.state.mark_processed(key, dest)
        if is_new:
            with self.coordenador._op_lock:
                seq = self.coordenador._emit("MSG_DONE", sender=sender, msg_id=msg_id, dest=dest)
            self.coordenador._replicate(seq)

        return expected_ack, False