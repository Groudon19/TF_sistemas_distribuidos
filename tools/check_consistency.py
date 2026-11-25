import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))

from sd.config import IP_SERVIDOR, MAX_SERVIDORES, enviar_e_receber, porta_do_servidor


def verificar_consistencia(ids_servidores: list[int]) -> bool:
    digests: dict[int, str] = {}
    for sid in ids_servidores:
        porta = porta_do_servidor(sid)
        try:
            resp = enviar_e_receber(IP_SERVIDOR, porta, "DIGEST", timeout=1.5)
            if resp and resp.startswith("DIGEST_"):
                digests[sid] = resp
        except OSError:
            pass

    if not digests:
        print("Nenhum servidor ativo respondeu ao DIGEST.")
        return False

    for sid, dig in sorted(digests.items()):
        print(f"Servidor {sid}: {dig}")

    valores = set(digests.values())
    if len(valores) == 1:
        print("CONSISTENTE: todos os servidores possuem exatamente o mesmo estado.")
        return True

    print("DIVERGENCIA DETECTADA entre os servidores ativos!")
    return False


if __name__ == "__main__":
    ids = [int(x) for x in sys.argv[1:]] if len(sys.argv) > 1 else list(range(MAX_SERVIDORES))
    sys.exit(0 if verificar_consistencia(ids) else 1)