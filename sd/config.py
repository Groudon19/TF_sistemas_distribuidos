import codecs
import json
import os
import sys
import threading
import time
from pathlib import Path

DIRETORIO_BASE = Path(__file__).resolve().parent.parent.parent
DIRETORIO_LOGS = DIRETORIO_BASE / "logs"


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

# Conexão de retorno com o Cadastro (NEWID / NEWCOORDINATORIP)
IP_CADASTRO = str(obter_cfg("CADASTRO_IP", "127.0.0.1"))
PORTA_CONTROLE = obter_int("CONTROL_PORT", 6001, 1, 65535)
ID_SERVIDOR_INICIAL = obter_int("INITIAL_SERVER_ID", 0, 0)
IP_BIND = str(obter_cfg("BIND_IP", IP_SERVIDOR))

# Constantes do domínio
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

# Garante que espera de vaga + replicação cabe nos 3s de timeout do Cadastro
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


class LoggerMetricas:
    """Logger thread-safe em formato JSON Lines (um registro JSON por linha)."""

    def __init__(self, componente: str, id_no: int, dir_logs: Path = DIRETORIO_LOGS):
        self.componente = componente
        self.id_no = id_no
        self.dir_logs = dir_logs
        self.dir_logs.mkdir(parents=True, exist_ok=True)
        self.caminho_arquivo = self.dir_logs / f"servidor_{id_no}.jsonl"
        self._lock = threading.Lock()

    def definir_componente(self, componente: str) -> None:
        """Alterna entre 'servidor' e 'coordenador' (por exemplo, após eleição)."""
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