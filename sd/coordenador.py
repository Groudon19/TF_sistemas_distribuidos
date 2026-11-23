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


class Coordenador:
    """Processa comandos de protocolo recebidos pelo servidor e gera respostas."""

    def __init__(self, state: ServerState, logger: MetricsLogger):
        self.state = state
        self.logger = logger
        self._handlers = {
            "NEWUSER": self.handle_newuser,
            "KILL": self.handle_kill,
        }

    def handle_line(self, line: str) -> tuple[str, bool]:
        """
        Processa uma linha de comando.
        Retorna (linha_resposta, deve_encerrar_servidor).
        """
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

        # Linha isolada de escolha de servidor (na Fase 3 será substituída pelo placement)
        target_server = self.state.my_id

        result = self.state.add_user(user_id, nome, target_server)

        if result == "ADDED":
            self.logger.log("newuser_adicionado", valor=1, usuario=user_id, servidor=target_server)
            return fmt_ack_newuser(user_id), False

        if result == "DUPLICATE":
            self.logger.log("newuser_duplicado", valor=1, usuario=user_id)
            return fmt_ack_newuser(user_id), False

        self.logger.log("newuser_rejeitado", valor=1, usuario=user_id, motivo="NO_CAPACITY")
        return fmt_error("NO_CAPACITY"), False

    def handle_kill(self, line: str) -> tuple[str, bool]:
        server_id = parse_kill(line)
        self.logger.log("kill_recebido", valor=1, alvo=server_id)

        if server_id == self.state.my_id:
            return fmt_ack_kill(server_id), True

        return fmt_error("UNKNOWN_SERVER"), False