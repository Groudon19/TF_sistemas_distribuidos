import threading
import time
from typing import Callable

from sd.common import config
from sd.common.logger import MetricsLogger
from sd.common.protocolo import (
    ProtocolError,
    fmt_ack_hello,
    fmt_ack_kill,
    fmt_ack_newuser,
    fmt_ack_shutdown,
    fmt_error,
    fmt_digest,
    parse_command,
    parse_hello,
    parse_kill,
    parse_newuser,
    parse_shutdown,
)
from sd.estado import Estado
from sd.placement import balance_metrics, pick_server_for_new_user, reassign_orphans, servers_needed


class Coordenador:
    """Processa comandos de protocolo de forma serializada e coordena escalonamento e replicação."""

    def __init__(self, state: Estado, logger: MetricsLogger, wait_sec: float = config.NEWUSER_WAIT_SEC):
        self.state = state
        self.logger = logger
        self.wait_sec = wait_sec
        self._op_lock = threading.Lock()
        self._cond = threading.Condition(self._op_lock)
        self._waiting = 0
        self.escalador = None
        self.replicador = None
        self._handlers: dict[str, Callable[[str], tuple[str, bool]]] = {
            "NEWUSER": self.handle_newuser,
            "KILL": self.handle_kill,
            "HELLO": self.handle_hello,
            "SHUTDOWN": self.handle_shutdown,
        }
        self.on_state_change_event = threading.Event()

    def register_handler(self, prefix: str, fn: Callable[[str], tuple[str, bool]]) -> None:
        self._handlers[prefix] = fn

    def attach_scaler(self, escalador) -> None:
        self.escalador = escalador

    def attach_replicator(self, replicador) -> None:
        self.replicador = replicador

    def _emit(self, op_type: str, **data) -> int:
        """Deve ser chamado sob _op_lock."""
        if self.replicador is None:
            return 0
        return self.replicador.append_locked(op_type, data)

    def _replicate(self, seq: int, budget: float = config.REPL_WAIT_SEC) -> None:
        """Deve ser chamado fora de qualquer lock."""
        if self.replicador is None or seq <= 0:
            return
        timeout = max(0.2, min(budget, config.REPL_WAIT_SEC))
        info = self.replicador.wait_replicated(seq, timeout=timeout)
        if info["synced"] > 0 or info["lagging"] > 0:
            self.logger.log(
                "replicacao_espera",
                valor=info["elapsed"],
                seq=seq,
                sincronizados=info["synced"],
                atrasados=info["lagging"],
            )

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

    def _place_pending_locked(self) -> dict[int, int]:
        """Deve ser chamado sob _op_lock. Retorna {uid: sid} efetivamente aplicados."""
        orphans = self.state.unassigned_users()
        if not orphans:
            return {}
        assignments, _ = reassign_orphans(orphans, self.state.loads(), self.state.capacity)
        if not assignments:
            return {}
        rejected = set(self.state.apply_assignments(assignments))
        return {uid: sid for uid, sid in assignments.items() if uid not in rejected}

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
        op_seq = 0

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
                            op_seq = self._emit("USER_ADD", uid=user_id, nome=nome, sid=target)
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
            remaining_budget = max(0.2, (self.wait_sec + config.REPL_WAIT_SEC) - waited)
            self._replicate(op_seq, budget=remaining_budget)
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

        with self._op_lock:
            was_starting = self.state.abort_starting(server_id)
            cancel_seq = self._emit("SRV_DEAD", id=server_id) if was_starting else 0

        if was_starting:
            self._replicate(cancel_seq)
            if self.escalador is not None:
                self.escalador.terminate_process(server_id)
            self.logger.log("servidor_cancelado", valor=1, servidor=server_id)
            return fmt_ack_kill(server_id), False

        last_seq = 0
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
                last_seq = self._emit("SRV_DEAD", id=server_id)
                assignments, pending = reassign_orphans(orphans, self.state.loads(), self.state.capacity)
                rejected = set(self.state.apply_assignments(assignments))
                applied_map = {uid: sid for uid, sid in assignments.items() if uid not in rejected}
                pending_set = set(pending) | rejected

                for uid in sorted(orphans):
                    final_sid = applied_map.get(uid)  # None se ficou pendente
                    last_seq = self._emit("USER_MOVE", uid=uid, sid=final_sid)

                migrated_count = len(applied_map)
                pending_count = len(pending_set)
                duration = round(time.monotonic() - t0, 6)
                snap = self._balance_snapshot()

        if already_dead:
            self.logger.log("kill_duplicado", valor=1, alvo=server_id)
            return fmt_ack_kill(server_id), False

        if self.replicador is not None:
            self.replicador.remove_follower(server_id)
        self._replicate(last_seq)

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
        last_seq = 0
        addr: tuple[str, int] | None = None

        with self._cond:
            reserved_ts = self.state.reserved_at(server_id)
            status = self.state.mark_active(server_id)
            placed_map: dict[int, int] = {}
            snap: dict | None = None
            if status == "ACTIVATED":
                addr = self.state.address_of(server_id)
                last_seq = self._emit("SRV_ACTIVE", id=server_id)
                placed_map = self._place_pending_locked()
                for uid, sid in sorted(placed_map.items()):
                    last_seq = self._emit("USER_MOVE", uid=uid, sid=sid)
                snap = self._balance_snapshot()
                self._cond.notify_all()

        if status == "ACTIVATED":
            if self.replicador is not None and addr is not None:
                self.replicador.add_follower(server_id, addr[0], addr[1])
            elapsed = round(now - reserved_ts, 4) if reserved_ts is not None else 0.0
            self.logger.log("tempo_novo_servidor", valor=elapsed, servidor=server_id)
            self.logger.log("usuarios_migrados", valor=0, evento="criacao", servidor=server_id)
            self.logger.log("usuarios_recolocados", valor=len(placed_map), servidor=server_id)
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
                    port = config.port_for_server(sid)
                    self._emit("SRV_RESERVED", id=sid, ip=config.SERVER_IP, port=port)
                    reserved.append(sid)
                except ValueError:
                    break
        return reserved

    def abort_starting(self, server_id: int, motivo: str) -> bool:
        seq = 0
        with self._op_lock:
            aborted = self.state.abort_starting(server_id)
            if aborted:
                seq = self._emit("SRV_DEAD", id=server_id)
        if aborted:
            self._replicate(seq)
            self.logger.log("servidor_abortado", valor=1, servidor=server_id, motivo=motivo)
        return aborted

    def retire_idle(self, server_id: int, idle_for: float) -> bool:
        seq = 0
        with self._op_lock:
            retired = self.state.retire_if_idle(server_id)
            if retired:
                seq = self._emit("SRV_DEAD", id=server_id)
                snap = self._balance_snapshot()
            else:
                snap = None
        if retired:
            if self.replicador is not None:
                self.replicador.remove_follower(server_id)
            self._replicate(seq)
            self.logger.log("servidor_desativado", valor=1, servidor=server_id, motivo="ocioso")
            self.logger.log("tempo_desativacao_ocioso", valor=round(idle_for, 4), servidor=server_id)
            assert snap is not None
            self._emit_balance("desativacao", snap)
        return retired
    
    def handle_digest(self, _line: str) -> tuple[str, bool]:
        """Retorna DIGEST_<head>_<crc> de forma atômica sob _op_lock."""
        with self._op_lock:
            head = self.replicador.oplog.head() if self.replicador is not None else 0
            crc = self.state.digest()
        return fmt_digest(head, crc), False
    
    def handle_line(self, line: str) -> tuple[str, bool]:
        try:
            cmd = parse_command(line)
            handler = self._handlers.get(cmd)
            if handler is None:
                return fmt_error("INVALID_COMMAND"), False
            return handler(line)
        except ConnectionResetError:
            raise
        except ProtocolError as exc:
            return fmt_error(exc.code), False
        except Exception as exc:
            self.logger.log("erro_interno", valor=1, erro=str(exc), linha=line.strip())
            return fmt_error("INTERNAL"), False