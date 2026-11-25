import codecs
import json
import os
import re
import socket
import sys
import threading
import time
from pathlib import Path
from urllib.parse import quote, unquote

DIRETORIO_BASE = Path(__file__).resolve().parent.parent
DIRETORIO_LOGS = DIRETORIO_BASE / "logs"
LIMITE_BYTES_LINHA = 65536


def carregar_env(caminho: Path) -> dict[str, str]:
    variaveis: dict[str, str] = {}
    if caminho.exists():
        for linha in caminho.read_text(encoding="utf-8").splitlines():
            linha = linha.strip()
            if not linha or linha.startswith("#") or "=" not in linha:
                continue
            chave, valor = linha.split("=", 1)
            variaveis[chave.strip()] = valor.strip().strip('"').strip("'")
    return variaveis


_ARQUIVO_ENV = carregar_env(DIRETORIO_BASE / ".env")


def obter_cfg(nome: str, padrao=None):
    return os.environ.get(nome, _ARQUIVO_ENV.get(nome, padrao))


def obter_int(nome: str, padrao: int | None = None, minimo: int | None = None, maximo: int | None = None) -> int:
    bruto = obter_cfg(nome, padrao)
    try:
        valor = int(str(bruto).strip())
    except (TypeError, ValueError):
        sys.exit(f"Erro de configuração: {nome} deve ser inteiro (recebido: {bruto!r})")
    if minimo is not None and valor < minimo:
        sys.exit(f"Erro de configuração: {nome} deve ser >= {minimo} (recebido: {valor})")
    if maximo is not None and valor > maximo:
        sys.exit(f"Erro de configuração: {nome} deve ser <= {maximo} (recebido: {valor})")
    return valor


def obter_float(nome: str, padrao: float, minimo: float = 0.0) -> float:
    bruto = obter_cfg(nome, padrao)
    try:
        valor = float(str(bruto).strip())
    except (TypeError, ValueError):
        sys.exit(f"Erro de configuração: {nome} deve ser número (recebido: {bruto!r})")
    if valor < minimo:
        sys.exit(f"Erro de configuração: {nome} deve ser >= {minimo} (recebido: {valor})")
    return valor


# Configurações obrigatórias do .env
IP_SERVIDOR = obter_cfg("SERVER_IP")
if not IP_SERVIDOR or not obter_cfg("SERVER_PORT"):
    sys.exit("Erro de configuração: SERVER_IP e SERVER_PORT são obrigatórias no .env")

PORTA_SERVIDOR = obter_int("SERVER_PORT", None, 1, 65535)
CODIFICACAO = str(obter_cfg("ENCODING", "utf-8"))
try:
    codecs.lookup(CODIFICACAO)
except LookupError:
    sys.exit(f"Erro de configuração: ENCODING desconhecido ({CODIFICACAO!r})")

# Conexão de retorno com o Cadastro
IP_CADASTRO = str(obter_cfg("CADASTRO_IP", "127.0.0.1"))
PORTA_CONTROLE = obter_int("CONTROL_PORT", 6001, 1, 65535)
ID_SERVIDOR_INICIAL = obter_int("INITIAL_SERVER_ID", 0, 0)
IP_BIND = str(obter_cfg("BIND_IP", IP_SERVIDOR))

# Constantes de capacidade e tempo
MAX_USUARIOS_POR_SERVIDOR = 5
ATRASO_RECUPERACAO_SEG = 5.0
TIMEOUT_REDE_SEG = obter_float("NET_TIMEOUT_SEC", 3.0, 0.1)

_MAX_SERVIDORES_BRUTO = obter_int("MAX_SERVERS", 20, 1)
MAX_SERVIDORES = min(_MAX_SERVIDORES_BRUTO, 65535 - PORTA_SERVIDOR + 1)

# Escalonamento e processos
TIMEOUT_CRIACAO_SEG = obter_float("SPAWN_TIMEOUT_SEC", 10.0, 0.5)
LIMITE_FALHAS_CRIACAO = obter_int("SPAWN_FAIL_LIMIT", 3, 1)
BACKOFF_CRIACAO_SEG = obter_float("SPAWN_BACKOFF_SEC", 5.0, 0.1)
TEMPO_OCIOSO_SEG = obter_float("IDLE_GRACE_SEC", 3.0, 0.0)
INTERVALO_ESCALADOR_SEG = obter_float("SCALER_INTERVAL_SEC", 0.5, 0.05)
TIMEOUT_INTERNO_SEG = obter_float("INTERNAL_TIMEOUT_SEC", 1.0, 0.1)
TENTATIVAS_HELLO = obter_int("HELLO_RETRIES", 20, 1)
ATRASO_TENTATIVA_HELLO_SEG = obter_float("HELLO_RETRY_DELAY", 0.25, 0.01)

# Replicação e mensagens
ESPERA_REPLICACAO_SEG = obter_float("REPL_WAIT_SEC", 0.5, 0.05)
ATRASO_RECONEXAO_REPL_SEG = obter_float("REPL_RECONNECT_DELAY_SEC", 0.5, 0.05)
TIMEOUT_LINK_REPL_SEG = obter_float("REPL_LINK_TIMEOUT_SEC", 1.0, 0.1)

_TIMEOUT_ENVIO_CADASTRO = obter_float("SEND_TIMEOUT", 3.0, 0.1)
_ESPERA_NEWUSER_BRUTA = obter_float("NEWUSER_WAIT_SEC", 2.0, 0.0)
ESPERA_NEWUSER_SEG = max(0.0, min(_ESPERA_NEWUSER_BRUTA, _TIMEOUT_ENVIO_CADASTRO - 1.0 - ESPERA_REPLICACAO_SEG))

INTERVALO_MAX_MSG_SEG = obter_float("MSG_MAX_INTERVAL_SEC", 10.0, 0.05)
TIMEOUT_ACK_MSG_SEG = obter_float("MSG_ACK_TIMEOUT_SEC", 5.0, 0.2)
MAX_TENTATIVAS_MSG = obter_int("MSG_MAX_ATTEMPTS", 10, 1)
PRAZO_ROTEAMENTO_SEG = obter_float("ROUTE_DEADLINE_SEC", 3.5, 0.2)
TIMEOUT_ENTREGA_SEG = obter_float("DELIVER_TIMEOUT_SEC", 2.0, 0.1)
ATRASO_RECEIVE_MSG_SEG = obter_float("RECEIVE_MSG_DELAY_SEC", 1.0, 0.0)
TIMEOUT_DRENAGEM_SEG = obter_float("DRAIN_TIMEOUT_SEC", 3.0, 0.1)
PROB_PERDA_ACK_CAOS = obter_float("CHAOS_DROP_ACK_PROB", 0.0, 0.0)


def porta_do_servidor(id_servidor: int) -> int:
    """Calcula a porta TCP de um servidor dado seu ID (0 -> PORTA_SERVIDOR, 1 -> +1, etc.)."""
    if id_servidor < 0:
        raise ValueError("id_servidor deve ser >= 0")
    porta = PORTA_SERVIDOR + id_servidor
    if porta > 65535:
        raise ValueError(f"Porta calculada fora do limite: {porta}")
    return porta


# =====================================================================
# Logger de Métricas (JSON Lines)
# =====================================================================

class LoggerMetricas:
    """Registra eventos e métricas de desempenho em arquivos .jsonl por nó."""

    def __init__(self, componente: str, id_no: int, dir_logs: Path = DIRETORIO_LOGS):
        self.componente = componente
        self.id_no = id_no
        self.dir_logs = dir_logs
        self.dir_logs.mkdir(parents=True, exist_ok=True)
        self.caminho_arquivo = self.dir_logs / f"servidor_{id_no}.jsonl"
        self._lock = threading.Lock()

    def definir_componente(self, componente: str) -> None:
        with self._lock:
            self.componente = componente

    def registrar(
        self,
        metrica: str,
        valor: int | float = 1,
        *,
        componente: str | None = None,
        id_no: int | None = None,
        **extras,
    ) -> None:
        with self._lock:
            registro = {
                **extras,
                "ts": round(time.time(), 3),
                "componente": componente or self.componente,
                "id": self.id_no if id_no is None else id_no,
                "metrica": metrica,
                "valor": valor,
            }
            try:
                linha = json.dumps(registro, ensure_ascii=False) + "\n"
                with self.caminho_arquivo.open("a", encoding="utf-8") as arq:
                    arq.write(linha)
            except (OSError, TypeError, ValueError) as exc:
                print(f"[aviso] falha ao gravar métrica {metrica!r}: {exc}")


# =====================================================================
# Comunicação TCP
# =====================================================================

def enviar_linha(sock: socket.socket, linha: str) -> None:
    dados = (linha.rstrip("\r\n") + "\n").encode(CODIFICACAO)
    sock.sendall(dados)


def ler_linha(sock: socket.socket, max_bytes: int = LIMITE_BYTES_LINHA) -> str | None:
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
# Protocolo de Mensagens
# =====================================================================

class ErroProtocolo(Exception):
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
    return quote(texto, safe=" _")


def desescapar_conteudo(texto: str) -> str:
    return unquote(texto)


def extrair_comando(linha: str) -> str:
    limpa = linha.strip()
    if not limpa:
        raise ErroProtocolo("INVALID_COMMAND", "Linha vazia")
    prefixo, _, _ = limpa.partition("_")
    return prefixo


def interpretar_newuser(linha: str) -> tuple[int, str | None]:
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
    carga = json.dumps(operacao, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return f"REPL_{carga}"


def interpretar_repl(linha: str) -> dict:
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