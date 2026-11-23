import json
import threading
import time
from pathlib import Path
from sd.common.config import LOGS_DIR


class MetricsLogger:
    """Logger thread-safe em formato JSON Lines (um objeto JSON por linha)."""

    def __init__(self, component: str, node_id: int, log_dir: Path = LOGS_DIR):
        self.component = component
        self.node_id = node_id
        self.log_dir = log_dir
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.file_path = self.log_dir / f"servidor_{node_id}.jsonl"
        self._lock = threading.Lock()

    def set_component(self, component: str) -> None:
        """Permite alternar entre 'servidor' e 'coordenador' após eleição."""
        with self._lock:
            self.component = component

    def log(self, metrica: str, valor: int | float = 1, *,
        componente: str | None = None, node_id: int | None = None,
        **extra) -> None:
        """
        Registra uma métrica em JSON Lines.
        - componente / node_id: sobrescrevem o padrão do logger para esta chamada
        (ex.: um usuário-thread emite componente="usuario", node_id=<id do usuário>).
        - extra: campos adicionais; nunca sobrescrevem os 5 campos obrigatórios.
        """
        with self._lock:
            registro = {
                **extra,  # extras primeiro, os obrigatórios vêm por cima
                "ts": round(time.time(), 3),
                "componente": componente or self.component,
                "id": self.node_id if node_id is None else node_id,
                "metrica": metrica,
                "valor": valor,
            }
            try:
                linha = json.dumps(registro, ensure_ascii=False) + "\n"
                with self.file_path.open("a", encoding="utf-8") as f:
                    f.write(linha)
            except (OSError, TypeError, ValueError) as exc:
                # O log nunca pode derrubar o servidor.
                print(f"[aviso] falha ao gravar métrica {metrica!r}: {exc}")