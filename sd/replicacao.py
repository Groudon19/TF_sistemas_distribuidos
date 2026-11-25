import json
import socket
import threading
import time
from pathlib import Path
from typing import Callable

from sd.common import config
from sd.common.logger import MetricsLogger
from sd.common.net import LineReader, send_line
from sd.common.protocolo import ProtocolError, fmt_digest, fmt_repl, parse_repl
from sd.estado import Estado


class OpLog:
    """Log ordenado de operações em memória e persistido em JSONL."""

    def __init__(self, node_id: int, log_dir: Path = config.LOGS_DIR):
        self.node_id = node_id
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.path = self.log_dir / f"oplog_{node_id}.jsonl"

        if self.path.exists():
            backup = self.log_dir / f"oplog_{node_id}_{int(time.time())}.bak.jsonl"
            try:
                self.path.rename(backup)
            except OSError:
                pass

        self._lock = threading.Lock()
        self._ops: list[dict] = []
        self._file = self.path.open("a", encoding="utf-8")

    def append(self, op_type: str, data: dict, epoch: int = 1) -> dict:
        with self._lock:
            seq = len(self._ops) + 1
            op = {
                "seq": seq,
                "epoch": epoch,
                "ts": round(time.time(), 6),
                "type": op_type,
                "data": data,
            }
            self._ops.append(op)
            self._file.write(json.dumps(op, ensure_ascii=True, separators=(",", ":")) + "\n")
            self._file.flush()
            return op

    def record_replicated(self, op: dict) -> None:
        """Grava no disco/memória uma operação recebida pelo seguidor."""
        with self._lock:
            self._ops.append(op)
            self._file.write(json.dumps(op, ensure_ascii=True, separators=(",", ":")) + "\n")
            self._file.flush()

    def head(self) -> int:
        with self._lock:
            return len(self._ops)

    def get_from(self, seq: int) -> list[dict]:
        with self._lock:
            if seq <= 1:
                return list(self._ops)
            return list(self._ops[seq - 1 :])

    def get_at(self, seq: int) -> dict | None:
        with self._lock:
            if 1 <= seq <= len(self._ops):
                return self._ops[seq - 1]
            return None

    def close(self) -> None:
        with self._lock:
            if not self._file.closed:
                self._file.close()


class Aplicador:
    """Aplica operações REPL_<json> no estado do seguidor na ordem exata de seq."""

    def __init__(self, state: Estado, oplog: OpLog, logger: MetricsLogger, epoch: int = 1):
        self.state = state
        self.oplog = oplog
        self.logger = logger
        self.epoch = epoch
        self.last_seq = 0
        self.first_repl_ts: float | None = None
        self.on_applied_event = threading.Event()
        self._lock = threading.Lock()

    def handle_repl(self, line: str) -> tuple[str, bool]:
        op = parse_repl(line)
        op_epoch = op["epoch"]
        seq = op["seq"]
        is_recovery = bool(op.pop("recovery", False))

        with self._lock:
            if self.first_repl_ts is None:
                self.first_repl_ts = time.monotonic()

            if op_epoch < self.epoch:
                return f"ERROR_STALE_EPOCH_{self.epoch}", False

            if seq <= self.last_seq:
                return f"ACK_REPL_{self.last_seq}", False

            if seq > self.last_seq + 1:
                return f"ERROR_GAP_{self.last_seq}", False

            if not self.state.apply_op(op):
                raise ProtocolError("INVALID_FORMAT", f"Operação inválida: {op.get('type')}")

            # Grava op limpo (sem 'recovery') para manter idêntico ao OpLog do líder
            self.oplog.record_replicated(op)
            self.last_seq = seq

        if not is_recovery:
            delay = max(0.0, round(time.time() - float(op["ts"]), 6))
            self.logger.log("atraso_replicacao", valor=delay, seq=seq, tipo=op["type"])

        self.on_applied_event.set()
        return f"ACK_REPL_{seq}", False

    def handle_digest(self, _line: str) -> tuple[str, bool]:
        with self._lock:
            seq = self.last_seq
            crc = self.state.digest()
        return fmt_digest(seq, crc), False


class ReplicaLink:
    """Thread dedicada para enviar o OpLog continuamente a um seguidor."""

    def __init__(
        self,
        follower_id: int,
        ip: str,
        port: int,
        oplog: OpLog,
        target_sync_seq: int,
        on_ack_cb: Callable[[int, int, str], None],
        on_stale_epoch_cb: Callable[[int], None],
        logger: MetricsLogger | None = None,
        send_fn: Callable[[str, int, str], str] | None = None,
    ):
        self.follower_id = follower_id
        self.ip = ip
        self.port = port
        self.oplog = oplog
        self.target_sync_seq = target_sync_seq
        self.on_ack_cb = on_ack_cb
        self.on_stale_epoch_cb = on_stale_epoch_cb
        self.logger = logger
        self.send_fn = send_fn

        self.cursor = 1
        self.acked_seq = 0
        self.status = "SYNCED" if target_sync_seq == 0 else "CATCHING_UP"
        self._unexpected_logged = False

        self._wake = threading.Event()
        self._stop = threading.Event()
        self._sock: socket.socket | None = None
        self._reader: LineReader | None = None
        self._thread = threading.Thread(
            target=self._run, name=f"ReplicaLink-{follower_id}", daemon=True
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        self._close_socket()
        self._thread.join(timeout=1.5)

    def notify(self) -> None:
        self._wake.set()

    def _close_socket(self) -> None:
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None
            self._reader = None

    def _send_wire(self, line: str) -> str:
        if self.send_fn is not None:
            return self.send_fn(self.ip, self.port, line)

        if self._sock is None or self._reader is None:
            sock = socket.create_connection((self.ip, self.port), timeout=config.REPL_LINK_TIMEOUT_SEC)
            self._sock = sock
            self._reader = LineReader(sock)

        send_line(self._sock, line)
        reply = self._reader.read_line(timeout=config.REPL_LINK_TIMEOUT_SEC)
        if reply is None:
            self._close_socket()
            sock = socket.create_connection((self.ip, self.port), timeout=config.REPL_LINK_TIMEOUT_SEC)
            self._sock = sock
            self._reader = LineReader(sock)
            send_line(self._sock, line)
            reply = self._reader.read_line(timeout=config.REPL_LINK_TIMEOUT_SEC)
            if reply is None:
                raise ConnectionResetError("EOF após reconexão de replicação")
        return reply

    def _run(self) -> None:
        while not self._stop.is_set():
            op = self.oplog.get_at(self.cursor)
            if op is None:
                self._wake.wait(timeout=0.25)
                self._wake.clear()
                continue

            # Marca operações que faziam parte do backlog inicial de recuperação
            if self.cursor <= self.target_sync_seq:
                wire_op = {**op, "recovery": True}
            else:
                wire_op = op

            try:
                reply = self._send_wire(fmt_repl(wire_op))
            except (OSError, ValueError):
                self._close_socket()
                self._stop.wait(timeout=config.REPL_RECONNECT_DELAY_SEC)
                continue

            if reply.startswith("ACK_REPL_"):
                try:
                    n = int(reply.split("_")[-1])
                except ValueError:
                    n = -1
                if n >= self.cursor:
                    self._unexpected_logged = False
                    self.cursor = n + 1
                    self.acked_seq = max(self.acked_seq, n)
                    if self.status in ("CATCHING_UP", "LAGGING") and self.acked_seq >= self.target_sync_seq:
                        self.status = "SYNCED"
                    self.on_ack_cb(self.follower_id, self.acked_seq, self.status)
                    continue

            if reply.startswith("ERROR_GAP_"):
                self._unexpected_logged = False
                try:
                    last = int(reply.split("_")[-1])
                    self.cursor = max(1, last + 1)
                except ValueError:
                    self.cursor = 1
                continue

            if reply.startswith("ERROR_STALE_EPOCH"):
                self.on_stale_epoch_cb(self.follower_id)
                break

            # Resposta inesperada ou ACK com n < cursor: evita loop de CPU e loga só 1 vez
            if not self._unexpected_logged and self.logger is not None:
                self._unexpected_logged = True
                self.logger.log(
                    "erro_replicacao_resposta",
                    valor=1,
                    seguidor=self.follower_id,
                    cursor=self.cursor,
                    resposta=reply[:80],
                )
            self._stop.wait(timeout=config.REPL_RECONNECT_DELAY_SEC)


class Replicador:
    """Gerencia o OpLog do líder e coordena os links para os seguidores."""

    def __init__(
        self,
        oplog: OpLog,
        logger: MetricsLogger,
        epoch: int = 1,
        send_fn: Callable[[str, int, str], str] | None = None,
    ):
        self.oplog = oplog
        self.logger = logger
        self.epoch = epoch
        self.send_fn = send_fn
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._links: dict[int, ReplicaLink] = {}
        self.stale_epoch_detected = False

    def append_locked(self, op_type: str, data: dict) -> int:
        op = self.oplog.append(op_type, data, epoch=self.epoch)
        with self._lock:
            links = list(self._links.values())
        for link in links:
            link.notify()
        return op["seq"]

    def add_follower(self, server_id: int, ip: str, port: int) -> None:
        with self._lock:
            old = self._links.pop(server_id, None)
        if old is not None:
            old.stop()

        head_seq = self.oplog.head()
        link = ReplicaLink(
            follower_id=server_id,
            ip=ip,
            port=port,
            oplog=self.oplog,
            target_sync_seq=head_seq,
            on_ack_cb=self._on_link_ack,
            on_stale_epoch_cb=self._on_stale_epoch,
            logger=self.logger,
            send_fn=self.send_fn,
        )
        with self._lock:
            self._links[server_id] = link
        link.start()
        link.notify()

    def remove_follower(self, server_id: int) -> None:
        with self._lock:
            link = self._links.pop(server_id, None)
            self._cond.notify_all()
        if link is not None:
            link.stop()

    def _on_link_ack(self, _follower_id: int, _acked_seq: int, _status: str) -> None:
        with self._cond:
            self._cond.notify_all()

    def _on_stale_epoch(self, _follower_id: int) -> None:
        with self._cond:
            self.stale_epoch_detected = True
            self._cond.notify_all()

    def wait_replicated(self, seq: int, timeout: float = config.REPL_WAIT_SEC) -> dict:
        t0 = time.monotonic()
        deadline = t0 + max(0.0, timeout)

        with self._cond:
            while True:
                synced_links = [lk for lk in self._links.values() if lk.status == "SYNCED"]
                if not synced_links or all(lk.acked_seq >= seq for lk in synced_links):
                    break

                rem = deadline - time.monotonic()
                if rem <= 0:
                    for lk in synced_links:
                        if lk.acked_seq < seq:
                            lk.status = "LAGGING"
                            lk.target_sync_seq = max(lk.target_sync_seq, seq)
                    break

                self._cond.wait(timeout=rem)

            synced_ok = sum(
                1 for lk in self._links.values() if lk.status == "SYNCED" and lk.acked_seq >= seq
            )
            lagging = sum(1 for lk in self._links.values() if lk.status == "LAGGING")

        elapsed = round(time.monotonic() - t0, 4)
        return {
            "seq": seq,
            "synced": synced_ok,
            "lagging": lagging,
            "elapsed": elapsed,
        }

    def stop(self) -> None:
        with self._lock:
            links = list(self._links.values())
            self._links.clear()
        for lk in links:
            lk.stop()