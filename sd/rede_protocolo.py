import json
import re
import socket
from urllib.parse import quote, unquote

from sd.common.config import CODIFICACAO, TIMEOUT_REDE_SEG

LIMITE_BYTES_LINHA = 65536


# =====================================================================
# Camada de Rede TCP
# =====================================================================

def enviar_linha(sock: socket.socket, linha: str) -> None:
    """Envia uma linha terminada em '\\n' garantindo envio completo."""
    dados = (linha.rstrip("\r\n") + "\n").encode(CODIFICACAO)
    sock.sendall(dados)


def ler_linha(sock: socket.socket, max_bytes: int = LIMITE_BYTES_LINHA) -> str | None:
    """Lê uma linha terminada em '\\n' do socket. Retorna None no fim da conexão (EOF)."""
    buffer = bytearray()
    while True:
        byte = sock.recv(1)
        if not byte:
            return None
        if byte == b"\n":
            break
        buffer.extend(byte)
        if len(buffer) > max_bytes:
            raise ValueError(f"Linha excedeu limite de {max_bytes} bytes")
    return buffer.decode(CODIFICACAO, errors="replace").rstrip("\r")


def enviar_e_receber(host: str, porta: int, linha: str, timeout: float = TIMEOUT_REDE_SEG) -> str | None:
    """Abre conexão TCP curta, envia uma linha e aguarda a linha de resposta."""
    with socket.create_connection((host, porta), timeout=timeout) as sock:
        sock.settimeout(timeout)
        enviar_linha(sock, linha)
        return ler_linha(sock)


class LeitorLinha:
    """Leitor de linhas TCP com buffer interno para conexões persistentes."""

    def __init__(self, sock: socket.socket, max_bytes: int = LIMITE_BYTES_LINHA, tamanho_bloco: int = 4096):
        self.sock = sock
        self.max_bytes = max_bytes
        self.tamanho_bloco = tamanho_bloco
        self._buffer = bytearray()

    def ler_linha(self, timeout: float | None = None) -> str | None:
        if timeout is not None:
            self.sock.settimeout(timeout)

        while True:
            idx_quebra = self._buffer.find(b"\n")
            if idx_quebra != -1:
                bruto = bytes(self._buffer[: idx_quebra + 1])
                del self._buffer[: idx_quebra + 1]
                return bruto.decode(CODIFICACAO, errors="replace").rstrip("\r\n")

            if len(self._buffer) > self.max_bytes:
                raise ValueError(f"Linha excedeu o limite de {self.max_bytes} bytes")

            bloco = self.sock.recv(self.tamanho_bloco)
            if not bloco:
                if not self._buffer:
                    return None
                bruto = bytes(self._buffer)
                self._buffer.clear()
                return bruto.decode(CODIFICACAO, errors="replace").rstrip("\r\n")

            self._buffer.extend(bloco)


# =====================================================================
# Protocolo de Aplicação
# =====================================================================

class ErroProtocolo(Exception):
    """Exceção lançada quando uma mensagem viola o formato ou regras do protocolo."""

    def __init__(self, codigo: str, mensagem: str = ""):
        self.codigo = codigo
        super().__init__(mensagem or codigo)


_REGEX_NEWUSER = re.compile(r"NEWUSER_(\d{1,18})(?:_(.*))?", re.ASCII | re.DOTALL)
_REGEX_ID = re.compile(r"\d{1,18}", re.ASCII)

TIPOS_OPERACAO_VALIDOS = frozenset({
    "SRV_UP",
    "SRV_RESERVED",
    "SRV_ACTIVE",
    "SRV_DEAD",
    "USER_ADD",
    "USER_MOVE",
    "MSG_DONE",
})


def _extrair_id_comando(prefixo: str, linha: str) -> int:
    limpa = linha.strip()
    casamento = re.fullmatch(rf"{re.escape(prefixo)}_(\d{{1,18}})", limpa, re.ASCII)
    if not casamento:
        raise ErroProtocolo("INVALID_FORMAT", f"Formato inválido para {prefixo}: {linha!r}")
    return int(casamento.group(1))


def escapar_conteudo(texto: str) -> str:
    """Escapa caracteres especiais mantendo espaços e underscores legíveis."""
    return quote(texto, safe=" _")


def desescapar_conteudo(texto: str) -> str:
    """Reverte o escape aplicado por escapar_conteudo."""
    return unquote(texto)


def extrair_comando(linha: str) -> str:
    """Extrai o prefixo do comando até o primeiro '_'."""
    limpa = linha.strip()
    if not limpa:
        raise ErroProtocolo("INVALID_COMMAND", "Linha vazia")
    prefixo, _, _ = limpa.partition("_")
    return prefixo


def interpretar_newuser(linha: str) -> tuple[int, str | None]:
    """Interpreta 'NEWUSER_<id>[_<nome>]' e retorna (id_usuario, nome_ou_None)."""
    limpa = linha.strip("\r\n")
    casamento = _REGEX_NEWUSER.fullmatch(limpa)
    if not casamento:
        raise ErroProtocolo("INVALID_FORMAT", f"Formato inválido para NEWUSER: {linha!r}")

    id_bruto, nome_bruto = casamento.groups()
    id_usuario = int(id_bruto)
    if id_usuario <= 0:
        raise ErroProtocolo("INVALID_ID", "ID do usuário deve ser maior que zero")

    if nome_bruto is None:
        return id_usuario, None

    nome_normalizado = " ".join(nome_bruto.replace("_", " ").split())
    if not nome_normalizado or len(nome_normalizado) > 100:
        raise ErroProtocolo("INVALID_NAME", "Nome vazio ou excede 100 caracteres")

    return id_usuario, nome_normalizado


def interpretar_kill(linha: str) -> int:
    return _extrair_id_comando("KILL", linha)


def interpretar_hello(linha: str) -> int:
    return _extrair_id_comando("HELLO", linha)


def interpretar_shutdown(linha: str) -> int:
    return _extrair_id_comando("SHUTDOWN", linha)


def formatar_repl(operacao: dict) -> str:
    """Serializa uma operação de replicação em REPL_<json_compacto>."""
    carga = json.dumps(operacao, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return f"REPL_{carga}"


def interpretar_repl(linha: str) -> dict:
    """Valida e converte uma linha REPL_<json> em dicionário de operação."""
    limpa = linha.strip("\r\n")
    if not limpa.startswith("REPL_"):
        raise ErroProtocolo("INVALID_FORMAT", "Linha não começa com REPL_")

    try:
        operacao = json.loads(limpa[5:])
    except json.JSONDecodeError as exc:
        raise ErroProtocolo("INVALID_FORMAT", f"JSON inválido em REPL: {exc}") from exc

    if not isinstance(operacao, dict):
        raise ErroProtocolo("INVALID_FORMAT", "Payload REPL deve ser um objeto JSON")

    seq = operacao.get("seq")
    epoca = operacao.get("epoch")
    ts = operacao.get("ts")
    tipo = operacao.get("type")
    dados = operacao.get("data")

    if type(seq) is not int or seq <= 0:
        raise ErroProtocolo("INVALID_FORMAT", "seq inválido em REPL")
    if type(epoca) is not int or epoca <= 0:
        raise ErroProtocolo("INVALID_FORMAT", "epoch inválido em REPL")
    if not isinstance(ts, (int, float)) or isinstance(ts, bool):
        raise ErroProtocolo("INVALID_FORMAT", "ts inválido em REPL")
    if tipo not in TIPOS_OPERACAO_VALIDOS:
        raise ErroProtocolo("INVALID_FORMAT", f"Tipo de operação desconhecido: {tipo!r}")
    if not isinstance(dados, dict):
        raise ErroProtocolo("INVALID_FORMAT", "data inválido em REPL")

    return operacao


def _interpretar_envio(prefixo: str, linha: str) -> tuple[int, int, int, str]:
    """Parser compartilhado para MSG_<id>_<remetente>_<dest>_<conteudo> e DELIVER_..."""
    limpa = linha.strip("\r\n")
    prefixo_esperado = f"{prefixo}_"
    if not limpa.startswith(prefixo_esperado):
        raise ErroProtocolo("INVALID_FORMAT", f"Mensagem não começa com {prefixo_esperado}")

    resto = limpa[len(prefixo_esperado):]
    partes = resto.split("_", 3)
    if len(partes) < 4:
        raise ErroProtocolo("INVALID_FORMAT", f"Campos insuficientes em {prefixo}")

    id_msg_bruto, rem_bruto, dest_bruto, conteudo_bruto = partes

    if not (_REGEX_ID.fullmatch(id_msg_bruto) and _REGEX_ID.fullmatch(rem_bruto) and _REGEX_ID.fullmatch(dest_bruto)):
        raise ErroProtocolo("INVALID_ID", f"IDs em {prefixo} devem conter de 1 a 18 dígitos ASCII")

    id_msg = int(id_msg_bruto)
    remetente = int(rem_bruto)
    destinatario = int(dest_bruto)

    if id_msg <= 0 or remetente <= 0 or destinatario <= 0:
        raise ErroProtocolo("INVALID_ID", f"IDs em {prefixo} devem ser maiores que zero")

    conteudo = desescapar_conteudo(conteudo_bruto)
    if not conteudo:
        raise ErroProtocolo("INVALID_CONTENT", "Conteúdo da mensagem não pode ser vazio")

    return id_msg, remetente, destinatario, conteudo


def interpretar_msg(linha: str) -> tuple[int, int, int, str]:
    return _interpretar_envio("MSG", linha)


def interpretar_deliver(linha: str) -> tuple[int, int, int, str]:
    return _interpretar_envio("DELIVER", linha)


# =====================================================================
# Formatadores de Saída
# =====================================================================

def formatar_ack(id_msg: int) -> str:
    return f"ACK_{id_msg}"


def formatar_erro(descricao: str) -> str:
    normalizado = "_".join(descricao.upper().split())
    return f"ERROR_{normalizado or 'UNKNOWN'}"


def formatar_ack_newuser(id_usuario: int) -> str:
    return f"ACK_NEWUSER_{id_usuario}"


def formatar_ack_kill(id_servidor: int) -> str:
    return f"ACK_KILL_{id_servidor}"


def formatar_hello(id_servidor: int) -> str:
    return f"HELLO_{id_servidor}"


def formatar_ack_hello(id_servidor: int) -> str:
    return f"ACK_HELLO_{id_servidor}"


def formatar_shutdown(id_servidor: int) -> str:
    return f"SHUTDOWN_{id_servidor}"


def formatar_ack_shutdown(id_servidor: int) -> str:
    return f"ACK_SHUTDOWN_{id_servidor}"


def formatar_newid(id_servidor: int) -> str:
    return f"NEWID_{id_servidor}"


def formatar_newcoordinatorip(ip: str, porta: int) -> str:
    return f"NEWCOORDINATORIP_{ip}_{porta}"


def formatar_msg(id_msg: int, remetente: int, destinatario: int, conteudo: str) -> str:
    return f"MSG_{id_msg}_{remetente}_{destinatario}_{escapar_conteudo(conteudo)}"


def formatar_deliver(id_msg: int, remetente: int, destinatario: int, conteudo: str) -> str:
    return f"DELIVER_{id_msg}_{remetente}_{destinatario}_{escapar_conteudo(conteudo)}"


def formatar_digest(ultimo_seq: int, crc: str) -> str:
    return f"DIGEST_{ultimo_seq}_{crc}"

