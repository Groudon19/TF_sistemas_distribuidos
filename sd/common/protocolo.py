import re
from urllib.parse import quote, unquote


class ProtocolError(Exception):
    """Exceção lançada quando uma mensagem viola o formato ou regras do protocolo."""

    def __init__(self, code: str, message: str = ""):
        self.code = code
        super().__init__(message or code)


# Regexes pré-compiladas com re.ASCII para rejeitar dígitos Unicode (ex: ١٢٣)
_RE_NEWUSER = re.compile(r"NEWUSER_(\d{1,18})(?:_(.*))?", re.ASCII | re.DOTALL)
_RE_KILL = re.compile(r"KILL_(\d{1,18})", re.ASCII)
_RE_ID = re.compile(r"\d{1,18}", re.ASCII)


def escape_content(s: str) -> str:
    """
    Escapa o conteúdo da mensagem para transmissão segura em uma única linha TCP.
    Mantém espaços e underscores legíveis, mas codifica '%', '\\n', '\\r' e caracteres especiais.
    """
    return quote(s, safe=" _")


def unescape_content(s: str) -> str:
    """Reverte o escape aplicado por escape_content."""
    return unquote(s)


def parse_command(line: str) -> str:
    """
    Remove espaços e quebras de linha nas pontas e extrai o prefixo do comando
    até o primeiro '_' (ou a linha inteira se não houver '_').
    """
    cleaned = line.strip()
    if not cleaned:
        raise ProtocolError("INVALID_COMMAND", "Linha vazia")
    prefix, _, _ = cleaned.partition("_")
    return prefix


def parse_newuser(line: str) -> tuple[int, str | None]:
    """
    Faz o parse de 'NEWUSER_<id>[_<nome>]'.
    Retorna (user_id, nome_normalizado_ou_None).
    """
    cleaned = line.strip()
    match = _RE_NEWUSER.fullmatch(cleaned)
    if not match:
        raise ProtocolError("INVALID_FORMAT", f"Formato inválido para NEWUSER: {line!r}")

    raw_id, raw_name = match.groups()
    user_id = int(raw_id)
    if user_id <= 0:
        raise ProtocolError("INVALID_ID", "ID do usuário deve ser maior que zero")

    if raw_name is None:
        return user_id, None

    # Troca underscores por espaço e colapsa múltiplos espaços consecutivos
    normalized_name = " ".join(raw_name.replace("_", " ").split())
    if not normalized_name or len(normalized_name) > 100:
        raise ProtocolError("INVALID_NAME", "Nome vazio ou excede 100 caracteres")

    return user_id, normalized_name


def parse_kill(line: str) -> int:
    """
    Faz o parse de 'KILL_<id>'.
    Retorna server_id (0 é válido).
    """
    cleaned = line.strip()
    match = _RE_KILL.fullmatch(cleaned)
    if not match:
        raise ProtocolError("INVALID_FORMAT", f"Formato inválido para KILL: {line!r}")

    return int(match.group(1))


def parse_msg(line: str) -> tuple[int, int, int, str]:
    """
    Faz o parse de 'MSG_<msg_id>_<sender>_<dest>_<content>'.
    Retorna (msg_id, sender_id, dest_id, unescaped_content).
    """
    cleaned = line.strip()
    if not cleaned.startswith("MSG_"):
        raise ProtocolError("INVALID_FORMAT", "Mensagem não começa com MSG_")

    resto = cleaned[4:]  # Remove 'MSG_'
    parts = resto.split("_", 3)
    if len(parts) < 4:
        raise ProtocolError("INVALID_FORMAT", "Campos insuficientes em MSG")

    raw_msg_id, raw_sender, raw_dest, raw_content = parts

    if not (_RE_ID.fullmatch(raw_msg_id) and _RE_ID.fullmatch(raw_sender) and _RE_ID.fullmatch(raw_dest)):
        raise ProtocolError("INVALID_ID", "IDs em MSG devem conter de 1 a 18 dígitos ASCII")

    msg_id = int(raw_msg_id)
    sender = int(raw_sender)
    dest = int(raw_dest)

    if msg_id <= 0 or sender <= 0 or dest <= 0:
        raise ProtocolError("INVALID_ID", "IDs em MSG devem ser maiores que zero")

    content = unescape_content(raw_content)
    if not content:
        raise ProtocolError("INVALID_CONTENT", "Conteúdo da mensagem não pode ser vazio")

    return msg_id, sender, dest, content


def _parse_cmd_id(prefix: str, line: str) -> int:
    cleaned = line.strip()
    match = re.fullmatch(rf"{re.escape(prefix)}_(\d{{1,18}})", cleaned, re.ASCII)
    if not match:
        raise ProtocolError("INVALID_FORMAT", f"Formato inválido para {prefix}: {line!r}")
    return int(match.group(1))

# =====================================================================
# Funções de Formatação (fmt_*)
# =====================================================================

def fmt_ack(msg_id: int) -> str:
    return f"ACK_{msg_id}"


def fmt_error(desc: str) -> str:
    normalized = "_".join(desc.upper().split())
    return f"ERROR_{normalized or 'UNKNOWN'}"


def fmt_ack_newuser(user_id: int) -> str:
    return f"ACK_NEWUSER_{user_id}"


def fmt_ack_kill(server_id: int) -> str:
    return f"ACK_KILL_{server_id}"


def fmt_newid(server_id: int) -> str:
    return f"NEWID_{server_id}"


def fmt_newcoordinatorip(ip: str, port: int) -> str:
    return f"NEWCOORDINATORIP_{ip}_{port}"


def fmt_msg(msg_id: int, sender: int, dest: int, content: str) -> str:
    escaped = escape_content(content)
    return f"MSG_{msg_id}_{sender}_{dest}_{escaped}"

def parse_kill(line: str) -> int:
    return _parse_cmd_id("KILL", line)


def parse_hello(line: str) -> int:
    return _parse_cmd_id("HELLO", line)


def parse_shutdown(line: str) -> int:
    return _parse_cmd_id("SHUTDOWN", line)


# Adicione nos formatadores (fmt_*):
def fmt_hello(server_id: int) -> str:
    return f"HELLO_{server_id}"


def fmt_ack_hello(server_id: int) -> str:
    return f"ACK_HELLO_{server_id}"


def fmt_shutdown(server_id: int) -> str:
    return f"SHUTDOWN_{server_id}"

def fmt_ack_shutdown(server_id: int) -> str:
    return f"ACK_SHUTDOWN_{server_id}"