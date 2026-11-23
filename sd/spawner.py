import subprocess
import sys
from sd.common.config import BASE_DIR, LOGS_DIR


def spawn_server(server_id: int, coord_ip: str, coord_port: int) -> subprocess.Popen:
    """Inicia um novo processo de servidor com stdout/stderr redirecionados para arquivo."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = LOGS_DIR / f"servidor_{server_id}.out"
    out_file = out_path.open("a", encoding="utf-8")

    cmd = [
        sys.executable,
        "-m",
        "sd.servidor",
        "--id",
        str(server_id),
        "--coord-ip",
        coord_ip,
        "--coord-port",
        str(coord_port),
    ]
    try:
        return subprocess.Popen(
            cmd,
            cwd=BASE_DIR,
            stdout=out_file,
            stderr=subprocess.STDOUT,
        )
    finally:
        out_file.close()


def terminate_process(proc: subprocess.Popen) -> None:
    """Encerra suavemente um processo filho (SIGTERM) com fallback para SIGKILL após 1s."""
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=1.0)
    except subprocess.TimeoutExpired:
        try:
            proc.kill()
            proc.wait(timeout=1.0)
        except OSError:
            pass
    except OSError:
        pass