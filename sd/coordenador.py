import threading
import time

from sd.common.logger import MetricsLogger
from sd.common.protocolo import (
    ProtocolError,
    fmt_ack_kill,
    fmt_ack_newuser,
    fmt_error,
    parse_command,
    parse_kill,
    parse_newuser,
)
from sd.estado import Estado as ServerState
from sd.placement import balance_metrics, pick_server_for_new_user, reassign_orphans, servers_needed


class Coordenador:
    """Processa comandos de protocolo de forma serializada e emite métricas."""

    def __init__(self, state: ServerState, logger: MetricsLogger):
        self.state = state
        self.logger = logger
        self._op_lock = threading.Lock()
        self._handlers = {
            "NEWUSER": self.handle_newuser,
            "KILL": self.handle_kill,
        }

    def _balance_snapshot(self) -> dict:
        """Deve ser chamado sob _op_lock."""
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
        """Deve ser chamado fora de qualquer lock."""
        self.logger.log("desbalanceamento", valor=snap["desbalanceamento"], evento=evento)
        self.logger.log("desvio_padrao_usuarios", valor=snap["desvio_padrao"], evento=evento)
        if snap["razao_servidores"] is not None:
            self.logger.log("razao_servidores_ativos", valor=snap["razao_servidores"], evento=evento)
        self.logger.log("usuarios_sem_servidor", valor=snap["sem_servidor"], evento=evento)

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

        snap: dict | None = None
        target: int | None = None
        total_u = 0
        active_srvs = 0

        with self._op_lock:
            if self.state.has_user(user_id):
                result = "DUPLICATE"
            else:
                loads = self.state.loads()
                target = pick_server_for_new_user(loads, self.state.capacity)
                if target is None:
                    result = "FULL"
                else:
                    result = self.state.add_user(user_id, nome, target)

            if result == "ADDED":
                snap = self._balance_snapshot()
            elif result in ("FULL", "NO_SERVER"):
                total_u = self.state.total_users()
                active_srvs = len(self.state.alive_server_ids())

        if result == "ADDED":
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

        self.logger.log("usuarios_migrados", valor=migrated_count, evento="kill", servidor=server_id)
        self.logger.log("tempo_redistribuicao", valor=duration, servidor=server_id)
        self.logger.log("usuarios_pendentes", valor=pending_count, servidor=server_id)
        assert snap is not None
        self._emit_balance("kill", snap)

        return fmt_ack_kill(server_id), False