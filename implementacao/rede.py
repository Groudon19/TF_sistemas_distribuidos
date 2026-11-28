import socket
import threading
import time
import traceback
from typing import Callable

import config
import logs


def _atender_conexao(conn: socket.socket, addr: tuple, tratador: Callable[[str], str]):
    origem = f"{addr[0]}:{addr[1]}"
    with conn:
        try:
            arquivo = conn.makefile("rb")
            for linha in arquivo:
                texto = linha.decode(config.ENCODING, errors="replace").strip()
                if not texto:
                    continue
                try:
                    resposta = tratador(texto)
                except Exception as exc:
                    logs.log_event("erro_interno", origem=origem, linha=texto, erro=f"{type(exc).__name__}: {exc}")
                    traceback.print_exc()
                    resposta = "ERROR_INTERNAL"
                conn.sendall((resposta + "\n").encode(config.ENCODING))
        except OSError as exc:
            logs.log_event("erro_conexao", origem=origem, erro=str(exc))

# Encapsula a camada TCP com, despacha cada conexao p uma thread

def servir(porta: int, tratador: Callable[[str], str], stop_event: threading.Event | None = None):
    """Escuta conexões TCP em 0.0.0.0:porta criando uma thread por conexão."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", porta))
        srv.listen()
        srv.settimeout(1.0)
        print(f"[ok] Escutando em 0.0.0.0:{porta} (Ctrl+C para encerrar)")

        while stop_event is None or not stop_event.is_set():
            try:
                conn, addr = srv.accept()
            except socket.timeout:
                continue
            t = threading.Thread(
                target=_atender_conexao,
                args=(conn, addr, tratador),
                daemon=True,
            )
            t.start()


def _ler_linha(sock: socket.socket) -> str:
    buf = b""
    try:
        while b"\n" not in buf and len(buf) < 4096:
            chunk = sock.recv(4096)
            if not chunk:
                break
            buf += chunk
    except socket.timeout:
        pass
    return buf.split(b"\n", 1)[0].decode(config.ENCODING, errors="replace").strip()


#abre conexão cliente e realiza retentativas em caso de falha
def enviar(
    ip: str,
    porta: int,
    linha: str,
    timeout: float | None = None,
    tentativas: int | None = None,
    atraso_retry: float | None = None,
) -> tuple[bool, str]:
    """Envia uma linha via TCP e aguarda uma linha de resposta. Retorna (ok, resposta_ou_erro)."""
    timeout = config.SEND_TIMEOUT if timeout is None else timeout
    tentativas = config.SEND_RETRIES if tentativas is None else tentativas
    atraso_retry = config.RETRY_DELAY if atraso_retry is None else atraso_retry

    payload = (linha.strip() + "\n").encode(config.ENCODING)
    ultimo_erro = "nenhuma tentativa realizada"

    for tentativa in range(1, tentativas + 1):
        try:
            with socket.create_connection((ip, porta), timeout=timeout) as sock:
                sock.settimeout(timeout)
                sock.sendall(payload)
                resposta = _ler_linha(sock)
        except OSError as exc:
            ultimo_erro = f"{ip}:{porta} indisponível ({exc})"
            logs.log_event(
                "falha_envio",
                destino=f"{ip}:{porta}",
                mensagem=linha.strip(),
                tentativa=tentativa,
                erro=str(exc),
            )
            if tentativa < tentativas:
                time.sleep(atraso_retry)
            continue

        if not resposta:
            ultimo_erro = f"sem resposta de {ip}:{porta}"
            if tentativa < tentativas:
                time.sleep(atraso_retry)
            continue

        if resposta.startswith("ERROR_"):
            return False, resposta
        return True, resposta

    return False, ultimo_erro