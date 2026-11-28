import sys
import math
import codecs
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
STATE_DIR = BASE_DIR / "estado"

def load_env(path: Path) -> dict:
    env = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env[key.strip()] = value.strip().strip('"').strip("'")
    return env


_ENV_FILE = load_env(BASE_DIR / ".env")

def cfg(name: str, default=None):
    return os.environ.get(name, _ENV_FILE.get(name, default))


def fail(message: str):
    sys.exit(f"Erro de configuração: {message}")


def cfg_int(name: str, default, minimo=None, maximo=None) -> int:
    raw = cfg(name, default)
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        fail(f"{name} deve ser um número inteiro (recebido: {raw!r}).")
    if minimo is not None and value < minimo:
        fail(f"{name} deve ser >= {minimo} (recebido: {value}).")
    if maximo is not None and value > maximo:
        fail(f"{name} deve ser <= {maximo} (recebido: {value}).")
    return value


def cfg_float(name: str, default, minimo=None) -> float:
    raw = cfg(name, default)
    try:
        value = float(str(raw).strip())
    except (TypeError, ValueError):
        fail(f"{name} deve ser um número (recebido: {raw!r}).")
    if not math.isfinite(value):
        fail(f"{name} deve ser um número finito (recebido: {raw!r}).")
    if minimo is not None and value < minimo:
        fail(f"{name} deve ser >= {minimo} (recebido: {value}).")
    return value


def cfg_bool(name: str, default: str) -> bool:
    return str(cfg(name, default)).strip().lower() in ("1", "true", "sim", "yes", "on")


SERVER_IP = cfg("SERVER_IP")
if not SERVER_IP or not cfg("SERVER_PORT"):
    fail("SERVER_IP e SERVER_PORT são obrigatórias (.env ou variáveis de ambiente).")
SERVER_PORT = cfg_int("SERVER_PORT", None, 1, 65535)

# Endereço do Cadastro (para envio de NEWID / NEWCOORDINATORIP nas próximas etapas)
CADASTRO_IP = cfg("CADASTRO_IP", SERVER_IP)
CONTROL_HOST = cfg("CONTROL_HOST", "0.0.0.0")
CONTROL_PORT = cfg_int("CONTROL_PORT", "6001", 1, 65535)

ENCODING = cfg("ENCODING", "utf-8")
try:
    codecs.lookup(ENCODING)
except LookupError:
    fail(f"ENCODING desconhecido: {ENCODING!r}.")

# ID do primeiro servidor (o coordenador inicial), que já começa na lista.
INITIAL_SERVER_ID = cfg_int("INITIAL_SERVER_ID", "0", 0)

SEND_TIMEOUT = cfg_float("SEND_TIMEOUT", "3", 0.1)
SEND_RETRIES = cfg_int("SEND_RETRIES", "3", 1)
RETRY_DELAY = cfg_float("RETRY_DELAY", "1", 0)

MAX_NAME_LEN = 100

MAX_USERS = cfg_int("MAX_USERS", "5", 1)
MAX_MSG_INTERVAL = cfg_float("MAX_MSG_INTERVAL", "10", 0.1)

RECEIVE_MSG_DELAY = cfg_float("RECEIVE_MSG_DELAY", "1", 0)
ACK_TIMEOUT = cfg_float("ACK_TIMEOUT", "3", 0.1)
MAX_ACK_RETRIES = cfg_int("MAX_ACK_RETRIES", "3", 1)

BUFFER_SIZE = 4096
HEARTBEAT_INTERVAL = 2.0
HEARTBEAT_TIMEOUT = 6.0