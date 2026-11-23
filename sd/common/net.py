import socket
from sd.common.config import ENCODING, NET_TIMEOUT_SEC

MAX_LINE_BYTES = 65536


def send_line(sock: socket.socket, line: str) -> None:
    """Envia uma string terminada em '\\n' garantindo envio completo."""
    payload = (line.rstrip("\r\n") + "\n").encode(ENCODING)
    sock.sendall(payload)


def recv_line(sock: socket.socket, max_bytes: int = MAX_LINE_BYTES) -> str | None:
    """
    Lê uma linha terminada em '\\n' do socket.
    Retorna a linha sem '\\r\\n', ou None se a conexão foi fechada (EOF) sem dados.
    """
    buf = bytearray()
    while True:
        chunk = sock.recv(1)
        if not chunk:
            return None
            
        if chunk == b"\n":
            break
        buf.extend(chunk)
        if len(buf) > max_bytes:
            raise ValueError(f"Linha excedeu limite de {max_bytes} bytes")
    return buf.decode(ENCODING, errors="replace").rstrip("\r")


def send_and_recv(host: str, port: int, line: str, timeout: float = NET_TIMEOUT_SEC) -> str | None:
    """Abre conexão TCP curta, envia uma linha e aguarda uma linha de resposta."""
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        send_line(sock, line)
        return recv_line(sock)