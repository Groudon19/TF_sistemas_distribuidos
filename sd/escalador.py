import subprocess
import threading
import time
from typing import Callable

from sd.common import config
from sd.common.logger import MetricsLogger
from sd.coordenador import Coordinator
from sd.placement import desired_servers, pick_server_to_retire, servers_to_spawn


class Escalador:
    """Controlador reativo e periódico de elasticidade (criação e remoção de servidores)."""

    def __init__(
        self,
        coordenador: Coordinator,
        logger: MetricsLogger,
        spawn_fn: Callable[[int, str, int], subprocess.Popen],
        newid_fn: Callable[[int, MetricsLogger], bool],
        shutdown_fn: Callable[[str, int, int], bool],
        terminate_fn: Callable[[subprocess.Popen], None],
        now_fn: Callable[[], float] = time.monotonic,
    ):
        self.coordenador = coordenador
        self.logger = logger
        self.spawn_fn = spawn_fn
        self.newid_fn = newid_fn
        self.shutdown_fn = shutdown_fn
        self.terminate_fn = terminate_fn
        self.now_fn = now_fn

        self._lock = threading.Lock()
        self._procs: dict[int, subprocess.Popen] = {}
        self._idle_since: dict[int, float] = {}
        self._consecutive_failures = 0
        self._pause_until = 0.0

        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="Escalador", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)

    def notify(self) -> None:
        self._wake.set()

    def on_server_ready(self, server_id: int) -> None:
        with self._lock:
            self._consecutive_failures = 0
            self._pause_until = 0.0

    def terminate_process(self, server_id: int) -> None:
        with self._lock:
            proc = self._procs.pop(server_id, None)
        if proc is not None:
            self.terminate_fn(proc)

    def request_shutdown(self, server_id: int) -> None:
        def _worker():
            addr = self.coordenador.state.address_of(server_id)
            ok = False
            if addr is not None:
                ip, port = addr
                try:
                    ok = self.shutdown_fn(ip, port, server_id)
                except Exception:
                    ok = False
            if not ok:
                self.terminate_process(server_id)
            else:
                with self._lock:
                    self._procs.pop(server_id, None)

        threading.Thread(target=_worker, daemon=True).start()

    def _loop(self) -> None:
        while not self._stop.is_set():
            self._wake.wait(timeout=config.SCALER_INTERVAL_SEC)
            self._wake.clear()
            if self._stop.is_set():
                break
            try:
                self.evaluate()
            except Exception as exc:
                self.logger.log("erro_escalador", valor=1, erro=str(exc))

    def _record_spawn_failure(self, now: float) -> None:
        with self._lock:
            self._consecutive_failures += 1
            if self._consecutive_failures >= config.SPAWN_FAIL_LIMIT:
                self._pause_until = now + config.SPAWN_BACKOFF_SEC
                self.logger.log(
                    "spawn_pausado",
                    valor=config.SPAWN_BACKOFF_SEC,
                    falhas=self._consecutive_failures,
                )

    def evaluate(self) -> None:
        now = self.now_fn()

        # 1. Coleta status de processos filhos para evitar zumbis
        with self._lock:
            procs_snapshot = list(self._procs.items())
        dead_pids: set[int] = set()
        for sid, proc in procs_snapshot:
            if proc.poll() is not None:
                dead_pids.add(sid)

        view = self.coordenador.scaling_view()

        # 2. Vigia servidores em STARTING (morte prematura ou timeout)
        for sid in view["starting"]:
            if sid in dead_pids:
                if self.coordenador.abort_starting(sid, "processo_morreu"):
                    self.terminate_process(sid)
                    self.logger.log("spawn_falhou", valor=1, servidor=sid)
                    self._record_spawn_failure(now)
                continue

            res_at = self.coordenador.state.reserved_at(sid)
            if res_at is not None and (now - res_at) > config.SPAWN_TIMEOUT_SEC:
                if self.coordenador.abort_starting(sid, "timeout"):
                    self.terminate_process(sid)
                    self.logger.log("spawn_timeout", valor=round(now - res_at, 4), servidor=sid)
                    self._record_spawn_failure(now)

        # Recarrega visão após eventuais abortos
        view = self.coordenador.scaling_view()
        atual = len(view["active"]) + len(view["starting"])
        desejado = desired_servers(view["users"], view["waiting"], view["capacity"])

        # 3. Criação de novos servidores
        with self._lock:
            paused = now < self._pause_until

        if not paused:
            n = servers_to_spawn(desejado, atual, config.MAX_SERVERS)
            if n > 0:
                if view["pending"] > 0:
                    motivo = "pendentes"
                elif view["waiting"] > 0:
                    motivo = "demanda"
                else:
                    motivo = "limite"

                coord_port = config.port_for_server(self.coordenador.state.my_id)
                new_ids = self.coordenador.reserve_servers(n)
                for sid in new_ids:
                    try:
                        proc = self.spawn_fn(sid, config.SERVER_IP, coord_port)
                        with self._lock:
                            self._procs[sid] = proc
                        threading.Thread(
                            target=self.newid_fn, args=(sid, self.logger), daemon=True
                        ).start()
                        self.logger.log("servidor_criado", valor=1, servidor=sid, motivo=motivo)
                    except Exception as exc:
                        self.coordenador.abort_starting(sid, "erro_popen")
                        self.logger.log("spawn_falhou", valor=1, servidor=sid, erro=str(exc))
                        self._record_spawn_failure(now)
            elif desejado > config.MAX_SERVERS and atual >= config.MAX_SERVERS:
                self.logger.log("limite_servidores", valor=config.MAX_SERVERS)

        # 4. Desativação de servidores ociosos
        current_idle = set(view["idle"])
        with self._lock:
            for sid in current_idle:
                self._idle_since.setdefault(sid, now)
            for sid in list(self._idle_since.keys()):
                if sid not in current_idle:
                    del self._idle_since[sid]
            idle_copy = dict(self._idle_since)

        if desejado < atual and idle_copy:
            candidate = pick_server_to_retire(idle_copy, now, config.IDLE_GRACE_SEC)
            if candidate is not None:
                idle_for = now - idle_copy[candidate]
                if self.coordenador.retire_idle(candidate, idle_for):
                    with self._lock:
                        self._idle_since.pop(candidate, None)
                    self.request_shutdown(candidate)