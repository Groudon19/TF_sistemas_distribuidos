
import json
import time
import threading

from pathlib import Path

_log_lock = threading.Lock()

_arquivo = None
_componente = None
_id = None

def init_log(arquivo: Path, componente: str, id: int):
    global _arquivo, _componente, _id
    _arquivo = Path(arquivo)
    _componente = componente
    _id = id
    _arquivo.parent.mkdir(parents=True, exist_ok=True)

def log_event( metrica: str, valor=1, **dados):
    """
    Registra em JSON Lines no formato do enunciado:
    {"ts", "componente", "id", "metrica", "valor"} + campos de detalhe.
    """
    registro = {"ts": round(time.time(), 3), "componente": _componente, "id": _id,
                "metrica": metrica, "valor": valor, **dados}
    linha = json.dumps(registro, ensure_ascii=False) + "\n"
    with _log_lock:
        try:
            with _arquivo.open("a", encoding="utf-8") as f:
                f.write(linha)
        except OSError as exc:
            print(f"[aviso] não foi possível gravar o log: {exc}")