
import json
import time
import threading
import config

_log_lock = threading.Lock()

def log_event(componente: str, id: int, metrica: str, valor=1, **dados):
    """
    Registra em JSON Lines no formato do enunciado:
    {"ts", "componente", "id", "metrica", "valor"} + campos de detalhe.
    """
    registro = {"ts": round(time.time(), 3), "componente": componente, "id": id,
                "metrica": metrica, "valor": valor, **dados}
    linha = json.dumps(registro, ensure_ascii=False) + "\n"
    with _log_lock:
        try:
            with config.LOG_FILE.open("a", encoding="utf-8") as f:
                f.write(linha)
        except OSError as exc:
            print(f"[aviso] não foi possível gravar o log: {exc}")