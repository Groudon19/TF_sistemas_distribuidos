import socket
import sys
import time

from sd.common import config


def query_digest(ip: str, port: int, timeout: float = 1.0) -> tuple[int, str] | None:
    try:
        with socket.create_connection((ip, port), timeout=timeout) as sock:
            sock.settimeout(timeout)
            sock.sendall(b"DIGEST\n")
            data = sock.recv(256).decode("utf-8", errors="replace").strip()
            if data.startswith("DIGEST_"):
                parts = data.split("_", 2)
                if len(parts) == 3:
                    return int(parts[1]), parts[2]
    except OSError:
        return None
    return None


def main() -> int:
    time.sleep(0.3)  # pequena margem para replicação em voo assentar
    rows: list[tuple[int, int, str]] = []

    for sid in range(config.MAX_SERVERS + 5):
        try:
            port = config.port_for_server(sid)
        except ValueError:
            break
        res = query_digest(config.SERVER_IP, port)
        if res is not None:
            rows.append((sid, res[0], res[1]))

    if not rows:
        print("Nenhum servidor ativo encontrado.")
        return 1

    print(f"{'ID':>4} | {'LAST_SEQ':>10} | {'CRC32':>10}")
    print("-" * 30)
    for sid, seq, crc in rows:
        print(f"{sid:>4} | {seq:>10} | {crc:>10}")

    by_seq: dict[int, set[str]] = {}
    for _, seq, crc in rows:
        by_seq.setdefault(seq, set()).add(crc)

    divergent = {seq: crcs for seq, crcs in by_seq.items() if len(crcs) > 1}
    if divergent:
        print(f"\n[ERRO] Divergência de CRC detectada: {divergent}")
        return 1

    print("\n[OK] Todos os servidores com mesmo seq possuem CRC idêntico.")
    return 0


if __name__ == "__main__":
    sys.exit(main())