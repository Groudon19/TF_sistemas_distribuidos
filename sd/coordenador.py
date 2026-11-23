import threading
import time

from sd.common import config
from sd.common.logger import MetricsLogger
from sd.common.protocolo import (
    ProtocolError,
    fmt_ack_hello,
    fmt_ack_kill,
    fmt_ack_newuser,
    fmt_ack_shutdown,
    fmt_error,
    parse_command,
    parse_hello,
    parse_kill,
    parse_newuser,
    parse_shutdown,
)
from sd.estado import ServerState
from sd.placement import balance_metrics, pick_server_for_new_user, reassign_orphans, servers_needed


class Coordinator:
    """Processa comandos de protocolo de forma serializada e coordena o escalonamento."""

    def __init__(self, state: ServerState, logger: MetricsLogger, wait_sec: float = config.NEWUSER_WAIT_SEC):
        self.state = state
        self.logger = logger
        self.wait_sec = wait_sec
        self._op_lock = threading.Lock()
        self._cond = threading.Condition(self._op_lock)
        self._waiting = 0
        self.escalador = None
        self._handlers = {
            "NEWUSER": self.handle_newuser,
            "KILL": self.handle_kill,
            "HELLO": self.handle_hello,
            "SHUTDOWN": self.handle_shutdown,
        }

    def attach_scaler(self, escalador) -> None:
        self.escalador = escalador

    def _balance_snapshot(self) -> dict:
        loads = self.state.loads()
        metrics = balance_metrics(loads)
        total_u = self.state.total_users()
        needed = servers_needed(total_u, self.state.capacity)
        active_count = metrics["servidores"]

        razao = round(active_count / needed, 4) if (total_u > 0 and needed > 0) else None
        return {
            **metrics,
            "razao_servidores": razao,
            "sem_servidor": len(self.state.unassigned_users()),
        }

    def _emit_balance(self, evento: str, snap: dict) -> None:
        self.logger.log("desbalanceamento", valor=snap["desbalanceamento"], evento=evento)
        self.logger.log("desvio_padrao_usuarios", valor=snap["desvio_padrao"], evento=evento)
        if snap["razao_servidores"] is not None:
            self.logger.log("razao_servidores_ativos", valor=snap["razao_servidores"], evento=evento)
        self.logger.log("usuarios_sem_servidor", valor=snap["sem_servidor"], evento=evento)

    def _place_pending_locked(self) -> int:
        orphans = self.state.unassigned_users()
        if not orphans:
            return 0
        assignments, _ = reassign_orphans(orphans, self.state.loads(), self.state.capacity)
        if not assignments:
            return 0
        rejected = self.state.apply_assignments(assignments)
        return len(assignments) - len(rejected)

    def handle_line(self, line: str) -> tuple[str, bool]:
        try:
            cmd = parse_command(line)
            handler = self._handlers.get(cmd)
            if handler is None:
                return fmt_error("INVALID_COMMAND"), False
            return handler(line)
        except ProtocolError as exc:
            return fmt_error(exc.code), False
        except Exception as exc:
            self.logger.log("erro_interno", valor=1, erro=str(exc), linha=line.strip())
            return fmt_error("INTERNAL"), False

    def handle_newuser(self, line: str) -> tuple[str, bool]:
        user_id, nome = parse_newuser(line)
        self.logger.log("newuser_recebido", valor=1, usuario=user_id)

        if not self.state.is_coordinator:
            self.logger.log("newuser_rejeitado", valor=1, usuario=user_id, motivo="NOT_COORDINATOR")
            return fmt_error("NOT_COORDINATOR"), False

        start_wait = time.monotonic()
        deadline = start_wait + self.wait_sec
        counted_waiting = False
        snap: dict | None = None
        target: int | None = None
        total_u = 0
        active_srvs = 0
        result = "FULL"

        with self._cond:
            try:
                while True:
                    if self.state.has_user(user_id):
                        result = "DUPLICATE"
                        break

                    target = pick_server_for_new_user(self.state.loads(), self.state.capacity)
                    if target is not None:
                        result = self.state.add_user(user_id, nome, target)
                        if result == "ADDED":
                            snap = self._balance_snapshot()
                        break

                    if self.escalador is None:
                        result = "FULL"
                        break

                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        result = "TIMEOUT"
                        break

                    if not counted_waiting:
                        self._waiting += 1
                        counted_waiting = True
                        self.escalador.notify()

                    self._cond.wait(timeout=remaining)
            finally:
                if counted_waiting:
                    self._waiting -= 1

            if result in ("FULL", "TIMEOUT", "NO_SERVER"):
                total_u = self.state.total_users()
                active_srvs = len(self.state.alive_server_ids())

        waited = round(time.monotonic() - start_wait, 4)
        if counted_waiting:
            self.logger.log("newuser_aguardou", valor=waited, usuario=user_id, resultado=result)

        if result == "ADDED":
            if self.escalador is not None:
                self.escalador.notify()
            self.logger.log("newuser_adicionado", valor=1, usuario=user_id, servidor=target)
            assert snap is not None
            self._emit_balance("newuser", snap)
            return fmt_ack_newuser(user_id), False

        if result == "DUPLICATE":
            self.logger.log("newuser_duplicado", valor=1, usuario=user_id)
            return fmt_ack_newuser(user_id), False

        self.logger.log(
            "newuser_rejeitado",
            valor=1,
            usuario=user_id,
            motivo="NO_CAPACITY",
            usuarios=total_u,
            servidores_ativos=active_srvs,
        )
        return fmt_error("NO_CAPACITY"), False

    def handle_kill(self, line: str) -> tuple[str, bool]:
        server_id = parse_kill(line)
        self.logger.log("kill_recebido", valor=1, alvo=server_id)

        if server_id == self.state.my_id:
            return fmt_ack_kill(server_id), True

        if not self.state.is_coordinator:
            return fmt_error("NOT_COORDINATOR"), False

        if not self.state.is_known_server(server_id):
            return fmt_error("UNKNOWN_SERVER"), False

        # Caso especial: servidor ainda estava STARTING
        with self._op_lock:
            was_starting = self.state.abort_starting(server_id)
        if was_starting:
            if self.escalador is not None:
                self.escalador.terminate_process(server_id)
            self.logger.log("servidor_cancelado", valor=1, servidor=server_id)
            return fmt_ack_kill(server_id), False

        with self._op_lock:
            t0 = time.monotonic()
            orphans = self.state.fail_server(server_id)
            if orphans is None:
                already_dead = True
                migrated_count = 0
                pending_count = 0
                duration = 0.0
                snap = None
            else:
                already_dead = False
                assignments, pending = reassign_orphans(orphans, self.state.loads(), self.state.capacity)
                rejected = self.state.apply_assignments(assignments)
                if rejected:
                    pending = sorted(set(pending) | set(rejected))
                    migrated_count = len(assignments) - len(rejected)
                else:
                    migrated_count = len(assignments)
                pending_count = len(pending)
                duration = round(time.monotonic() - t0, 6)
                snap = self._balance_snapshot()

        if already_dead:
            self.logger.log("kill_duplicado", valor=1, alvo=server_id)
            return fmt_ack_kill(server_id), False

        self.logger.log("servidor_desativado", valor=1, servidor=server_id, motivo="kill")
        self.logger.log("usuarios_migrados", valor=migrated_count, evento="kill", servidor=server_id)
        self.logger.log("tempo_redistribuicao", valor=duration, servidor=server_id)
        self.logger.log("usuarios_pendentes", valor=pending_count, servidor=server_id)
        assert snap is not None
        self._emit_balance("kill", snap)

        if self.escalador is not None:
            self.escalador.request_shutdown(server_id)
            self.escalador.notify()

        return fmt_ack_kill(server_id), False

    def handle_hello(self, line: str) -> tuple[str, bool]:
        server_id = parse_hello(line)
        if not self.state.is_coordinator:
            return fmt_error("NOT_COORDINATOR"), False

        now = time.monotonic()
        with self._cond:
            reserved_ts = self.state.reserved_at(server_id)
            status = self.state.mark_active(server_id)
            placed = 0
            snap: dict | None = None
            if status == "ACTIVATED":
                placed = self._place_pending_locked()
                snap = self._balance_snapshot()
                self._cond.notify_all()

        if status == "ACTIVATED":
            elapsed = round(now - reserved_ts, 4) if reserved_ts is not None else 0.0
            self.logger.log("tempo_novo_servidor", valor=elapsed, servidor=server_id)
            self.logger.log("usuarios_migrados", valor=0, evento="criacao", servidor=server_id)
            self.logger.log("usuarios_recolocados", valor=placed, servidor=server_id)
            assert snap is not None
            self._emit_balance("criacao", snap)
            if self.escalador is not None:
                self.escalador.on_server_ready(server_id)
                self.escalador.notify()
            return fmt_ack_hello(server_id), False

        if status == "ALREADY_ACTIVE":
            return fmt_ack_hello(server_id), False

        return fmt_error("UNKNOWN_SERVER"), False

    def handle_shutdown(self, line: str) -> tuple[str, bool]:
        server_id = parse_shutdown(line)
        if server_id != self.state.my_id:
            return fmt_error("WRONG_SERVER"), False
        return fmt_ack_shutdown(server_id), True

    # ------------------------------------------------------------------
    # Métodos públicos para o Escalador
    # ------------------------------------------------------------------

    def scaling_view(self) -> dict:
        with self._op_lock:
            return {
                "users": self.state.total_users(),
                "waiting": self._waiting,
                "pending": len(self.state.unassigned_users()),
                "capacity": self.state.capacity,
                "active": self.state.alive_server_ids(),
                "starting": self.state.starting_server_ids(),
                "idle": self.state.idle_server_ids(),
            }

    def reserve_servers(self, n: int) -> list[int]:
        reserved: list[int] = []
        with self._op_lock:
            for _ in range(n):
                try:
                    sid = self.state.reserve_server(config.SERVER_IP, config.port_for_server)
                    reserved.append(sid)
                except ValueError:
                    break
        return reserved

    def abort_starting(self, server_id: int, motivo: str) -> bool:
        with self._op_lock:
            aborted = self.state.abort_starting(server_id)
        if aborted:
            self.logger.log("servidor_abortado", valor=1, servidor=server_id, motivo=motivo)
        return aborted

    def retire_idle(self, server_id: int, idle_for: float) -> bool:
        with self._op_lock:
            retired = self.state.retire_if_idle(server_id)
            snap = self._balance_snapshot() if retired else None
        if retired:
            self.logger.log("servidor_desativado", valor=1, servidor=server_id, motivo="ocioso")
            self.logger.log("tempo_desativacao_ocioso", valor=round(idle_for, 4), servidor=server_id)
            assert snap is not None
            self._emit_balance("desativacao", snap)
        return retired