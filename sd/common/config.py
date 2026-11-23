import codecs
import os
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent


def load_env(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
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


def cfg_int(name: str, default: int | None = None, minimo: int | None = None, maximo: int | None = None) -> int:
    raw = cfg(name, default)
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        sys.exit(f"Erro de configuração: {name} deve ser inteiro (recebido: {raw!r})")
    if minimo is not None and value < minimo:
        sys.exit(f"Erro de configuração: {name} deve ser >= {minimo} (recebido: {value})")
    if maximo is not None and value > maximo:
        sys.exit(f"Erro de configuração: {name} deve ser <= {maximo} (recebido: {value})")
    return value


def cfg_float(name: str, default: float, minimo: float = 0.0) -> float:
    raw = cfg(name, default)
    try:
        value = float(str(raw).strip())
    except (TypeError, ValueError):
        sys.exit(f"Erro de configuração: {name} deve ser número (recebido: {raw!r})")
    if value < minimo:
        sys.exit(f"Erro de configuração: {name} deve ser >= {minimo} (recebido: {value})")
    return value


# Obrigatórias no .env
SERVER_IP = cfg("SERVER_IP")
if not SERVER_IP or not cfg("SERVER_PORT"):
    sys.exit("Erro de configuração: SERVER_IP e SERVER_PORT são obrigatórias no .env")

SERVER_PORT = cfg_int("SERVER_PORT", None, 1, 65535)
ENCODING = str(cfg("ENCODING", "utf-8"))
try:
    codecs.lookup(ENCODING)
except LookupError:
    sys.exit(f"Erro de configuração: ENCODING desconhecido ({ENCODING!r})")

# Conexão de retorno com o Cadastro (NEWID / NEWCOORDINATORIP)
CADASTRO_IP = str(cfg("CADASTRO_IP", "127.0.0.1"))
CONTROL_PORT = cfg_int("CONTROL_PORT", 6001, 1, 65535)
INITIAL_SERVER_ID = cfg_int("INITIAL_SERVER_ID", 0, 0)
BIND_IP = str(cfg("BIND_IP", SERVER_IP))

# Constantes do domínio (PDF)
MAX_USERS_PER_SERVER = 5
RECOVERY_DELAY_SEC = 5.0
NET_TIMEOUT_SEC = cfg_float("NET_TIMEOUT_SEC", 3.0, 0.1)

LOGS_DIR = BASE_DIR / "logs"


def port_for_server(server_id: int) -> int:
    """
    Calcula a porta TCP de um servidor dado seu ID.
    Servidor 0 -> SERVER_PORT (5001), Servidor 1 -> 5002, etc.
    """
    if server_id < 0:
        raise ValueError("server_id deve ser >= 0")
    port = SERVER_PORT + server_id
    if port > 65535:
        raise ValueError(f"Porta calculada fora do limite: {port}")
    return port


# Adicione ao final de sd/common/config.py:
_RAW_MAX_SERVERS = cfg_int("MAX_SERVERS", 20, 1)
MAX_SERVERS = min(_RAW_MAX_SERVERS, 65535 - SERVER_PORT + 1)

_CADASTRO_SEND_TIMEOUT = cfg_float("SEND_TIMEOUT", 3.0, 0.1)
_RAW_NEWUSER_WAIT = cfg_float("NEWUSER_WAIT_SEC", 2.0, 0.0)
NEWUSER_WAIT_SEC = max(0.0, min(_RAW_NEWUSER_WAIT, _CADASTRO_SEND_TIMEOUT - 1.0))

SPAWN_TIMEOUT_SEC = cfg_float("SPAWN_TIMEOUT_SEC", 10.0, 0.5)
SPAWN_FAIL_LIMIT = cfg_int("SPAWN_FAIL_LIMIT", 3, 1)
SPAWN_BACKOFF_SEC = cfg_float("SPAWN_BACKOFF_SEC", 5.0, 0.1)
IDLE_GRACE_SEC = cfg_float("IDLE_GRACE_SEC", 3.0, 0.0)
SCALER_INTERVAL_SEC = cfg_float("SCALER_INTERVAL_SEC", 0.5, 0.05)
INTERNAL_TIMEOUT_SEC = cfg_float("INTERNAL_TIMEOUT_SEC", 1.0, 0.1)
HELLO_RETRIES = cfg_int("HELLO_RETRIES", 20, 1)
HELLO_RETRY_DELAY = cfg_float("HELLO_RETRY_DELAY", 0.25, 0.01)