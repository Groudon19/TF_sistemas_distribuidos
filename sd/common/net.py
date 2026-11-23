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
    
class LineReader:
    """Leitor de linhas TCP com buffer interno para evitar recv(1) por byte."""

    def __init__(self, sock: socket.socket, max_bytes: int = MAX_LINE_BYTES, chunk_size: int = 4096):
        self.sock = sock
        self.max_bytes = max_bytes
        self.chunk_size = chunk_size
        self._buf = bytearray()

    def read_line(self, timeout: float | None = None) -> str | None:
        if timeout is not None:
            self.sock.settimeout(timeout)

        while True:
            nl_idx = self._buf.find(b"\n")
            if nl_idx != -1:
                raw = bytes(self._buf[: nl_idx + 1])
                del self._buf[: nl_idx + 1]
                return raw.decode("utf-8", errors="replace").rstrip("\r\n")

            if len(self._buf) > self.max_bytes:
                raise ValueError(f"Linha excedeu o limite de {self.max_bytes} bytes")

            chunk = self.sock.recv(self.chunk_size)
            if not chunk:
                if not self._buf:
                    return None
                raw = bytes(self._buf)
                self._buf.clear()
                return raw.decode("utf-8", errors="replace").rstrip("\r\n")

            self._buf.extend(chunk)