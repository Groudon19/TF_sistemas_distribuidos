import json
import statistics
import threading
import time
import zlib

from sd.config import IP_SERVIDOR, MAX_USUARIOS_POR_SERVIDOR, PORTA_SERVIDOR


# =====================================================================
# Funções Auxiliares de Alocação e Balanceamento
# =====================================================================

def escolher_servidor_para_usuario(cargas: dict[int, int], capacidade: int) -> int | None:
    """Escolhe o servidor ativo com menor carga (< capacidade); empate -> menor ID."""
    candidatos = [(carga, sid) for sid, carga in cargas.items() if carga < capacidade]
    if not candidatos:
        return None
    _, escolhido = min(candidatos)
    return escolhido


def realocar_orfaos(
    orfaos: list[int], cargas: dict[int, int], capacidade: int
) -> tuple[dict[int, int], list[int]]:
    """Redistribui usuários órfãos entre servidores ativos respeitando a capacidade."""
    cargas_temp = dict(cargas)
    atribuicoes: dict[int, int] = {}
    pendentes: list[int] = []

    for id_usuario in sorted(orfaos):
        destino = escolher_servidor_para_usuario(cargas_temp, capacidade)
        if destino is None:
            pendentes.append(id_usuario)
        else:
            atribuicoes[id_usuario] = destino
            cargas_temp[destino] += 1

    return atribuicoes, pendentes


def calcular_metricas_balanceamento(cargas: dict[int, int]) -> dict:
    if not cargas:
        return {
            "desbalanceamento": 0,
            "desvio_padrao": 0.0,
            "servidores": 0,
            "usuarios": 0,
        }
    valores = list(cargas.values())
    return {
        "desbalanceamento": max(valores) - min(valores),
        "desvio_padrao": round(statistics.pstdev(valores), 4),
        "servidores": len(valores),
        "usuarios": sum(valores),
    }


def servidores_necessarios(total_usuarios: int, capacidade: int) -> int:
    if total_usuarios <= 0 or capacidade <= 0:
        return 0
    return -(-total_usuarios // capacidade)


def servidores_desejados(total_usuarios: int, aguardando: int, capacidade: int) -> int:
    if capacidade <= 0:
        return 1
    return max(1, (total_usuarios + aguardando) // capacidade + 1)


def servidores_a_criar(desejado: int, atual: int, max_servidores: int) -> int:
    return max(0, min(desejado, max_servidores) - atual)


def escolher_servidor_ocioso(ocioso_desde: dict[int, float], agora: float, carencia: float) -> int | None:
    elegiveis = [sid for sid, desde in ocioso_desde.items() if (agora - desde) >= carencia]
    return max(elegiveis) if elegiveis else None


# =====================================================================
# Estado Replicado do Nó
# =====================================================================

class Estado:
    """Mantém o estado em memória de um nó (servidores, usuários e mensagens confirmadas)."""

    def __init__(self, meu_id: int, eh_coordenador: bool = False, capacidade: int = MAX_USUARIOS_POR_SERVIDOR):
        self.lock = threading.RLock()
        self.meu_id = meu_id
        self.eh_coordenador = eh_coordenador
        self.capacidade = capacidade
        self.servidores: dict[int, dict] = {}
        self.usuario_servidor: dict[int, int | None] = {}
        self.nomes_usuarios: dict[int, str | None] = {}
        self.processadas: dict[tuple[int, int], int] = {}
        self._endereco_coordenador: tuple[str, int] = (IP_SERVIDOR, PORTA_SERVIDOR)

    @property
    def endereco_coordenador(self) -> tuple[str, int]:
        with self.lock:
            return self._endereco_coordenador

    @endereco_coordenador.setter
    def endereco_coordenador(self, endereco: tuple[str, int]) -> None:
        with self.lock:
            self._endereco_coordenador = endereco

    def registrar_servidor(self, id_servidor: int, ip: str | None = None, porta: int | None = None) -> None:
        with self.lock:
            registro = self.servidores.get(id_servidor, {})
            registro["ativo"] = True
            registro["iniciando"] = False
            registro["reservado_em"] = None
            if ip is not None:
                registro["ip"] = ip
            if porta is not None:
                registro["porta"] = porta
            self.servidores[id_servidor] = registro

    def reservar_servidor(self, ip: str, obter_porta) -> int:
        with self.lock:
            proximo_id = max(self.servidores.keys(), default=-1) + 1
            porta = obter_porta(proximo_id)
            self.servidores[proximo_id] = {
                "ativo": False,
                "iniciando": True,
                "ip": ip,
                "porta": porta,
                "reservado_em": time.monotonic(),
            }
            return proximo_id

    def esta_iniciando(self, id_servidor: int) -> bool:
        with self.lock:
            info = self.servidores.get(id_servidor)
            return bool(info and info.get("iniciando"))

    def ids_servidores_iniciando(self) -> list[int]:
        with self.lock:
            return sorted(sid for sid, info in self.servidores.items() if info.get("iniciando"))

    def instante_reserva(self, id_servidor: int) -> float | None:
        with self.lock:
            info = self.servidores.get(id_servidor)
            return info.get("reservado_em") if info else None

    def marcar_ativo(self, id_servidor: int) -> str:
        with self.lock:
            info = self.servidores.get(id_servidor)
            if info is None:
                return "UNKNOWN"
            if info.get("ativo"):
                return "ALREADY_ACTIVE"
            if info.get("iniciando"):
                info["ativo"] = True
                info["iniciando"] = False
                return "ACTIVATED"
            return "DEAD"

    def abortar_inicializacao(self, id_servidor: int) -> bool:
        with self.lock:
            info = self.servidores.get(id_servidor)
            if info and info.get("iniciando"):
                info["iniciando"] = False
                info["ativo"] = False
                return True
            return False

    def ids_servidores_ativos(self) -> list[int]:
        with self.lock:
            return sorted(sid for sid, info in self.servidores.items() if info.get("ativo"))

    def servidor_conhecido(self, id_servidor: int) -> bool:
        with self.lock:
            return id_servidor in self.servidores

    def servidor_ativo(self, id_servidor: int) -> bool:
        with self.lock:
            info = self.servidores.get(id_servidor)
            return bool(info and info.get("ativo"))

    def endereco_de(self, id_servidor: int) -> tuple[str, int] | None:
        with self.lock:
            info = self.servidores.get(id_servidor)
            if not info or "ip" not in info or "porta" not in info:
                return None
            return info["ip"], info["porta"]

    def possui_usuario(self, id_usuario: int) -> bool:
        with self.lock:
            return id_usuario in self.usuario_servidor

    def servidor_de(self, id_usuario: int) -> int | None:
        with self.lock:
            return self.usuario_servidor.get(id_usuario)

    def usuarios_do_servidor(self, id_servidor: int) -> list[int]:
        with self.lock:
            return [uid for uid, sid in self.usuario_servidor.items() if sid == id_servidor]

    def todos_ids_usuarios(self) -> list[int]:
        with self.lock:
            return sorted(self.usuario_servidor.keys())

    def carga_de(self, id_servidor: int) -> int:
        with self.lock:
            return sum(1 for sid in self.usuario_servidor.values() if sid == id_servidor)

    def cargas(self) -> dict[int, int]:
        with self.lock:
            cargas_ativas = {sid: 0 for sid, info in self.servidores.items() if info.get("ativo")}
            for sid in self.usuario_servidor.values():
                if sid is not None and sid in cargas_ativas:
                    cargas_ativas[sid] += 1
            return cargas_ativas

    def ids_servidores_ociosos(self) -> list[int]:
        with self.lock:
            return sorted(
                sid for sid, carga in self.cargas().items() if sid != self.meu_id and carga == 0
            )

    def desativar_se_ocioso(self, id_servidor: int) -> bool:
        with self.lock:
            if id_servidor == self.meu_id:
                return False
            info = self.servidores.get(id_servidor)
            if not info or not info.get("ativo") or self.carga_de(id_servidor) > 0:
                return False
            info["ativo"] = False
            info["iniciando"] = False
            return True

    def total_usuarios(self) -> int:
        with self.lock:
            return len(self.usuario_servidor)

    def usuarios_sem_servidor(self) -> list[int]:
        with self.lock:
            return sorted(uid for uid, sid in self.usuario_servidor.items() if sid is None)

    def adicionar_usuario(self, id_usuario: int, nome: str | None, id_servidor: int) -> str:
        with self.lock:
            if id_usuario in self.usuario_servidor:
                return "DUPLICATE"
            srv = self.servidores.get(id_servidor)
            if not srv or not srv.get("ativo"):
                return "NO_SERVER"
            if self.carga_de(id_servidor) >= self.capacidade:
                return "FULL"
            self.usuario_servidor[id_usuario] = id_servidor
            self.nomes_usuarios[id_usuario] = nome
            return "ADDED"

    def derrubar_servidor(self, id_servidor: int) -> list[int] | None:
        with self.lock:
            srv = self.servidores.get(id_servidor)
            if not srv or not srv.get("ativo"):
                return None
            srv["ativo"] = False
            srv["iniciando"] = False
            orfaos = sorted(uid for uid, sid in self.usuario_servidor.items() if sid == id_servidor)
            for uid in orfaos:
                self.usuario_servidor[uid] = None
            return orfaos

    def aplicar_atribuicoes(self, atribuicoes: dict[int, int]) -> list[int]:
        with self.lock:
            rejeitados: list[int] = []
            cargas_atuais = self.cargas()
            for uid in sorted(atribuicoes):
                sid = atribuicoes[uid]
                if sid not in cargas_atuais or cargas_atuais[sid] >= self.capacidade:
                    rejeitados.append(uid)
                    continue
                if uid in self.usuario_servidor:
                    self.usuario_servidor[uid] = sid
                    cargas_atuais[sid] += 1
                else:
                    rejeitados.append(uid)
            return rejeitados

    def ja_processada(self, chave: tuple[int, int]) -> bool:
        with self.lock:
            return chave in self.processadas

    def marcar_processada(self, chave: tuple[int, int], destinatario: int) -> bool:
        with self.lock:
            if chave in self.processadas:
                return False
            self.processadas[chave] = destinatario
            return True

    def aplicar_operacao(self, operacao: dict) -> bool:
        """Aplica de forma idempotente uma decisão replicada no log."""
        tipo = operacao.get("type")
        dados = operacao.get("data", {})

        with self.lock:
            if tipo == "SRV_UP":
                sid = int(dados["id"])
                reg = self.servidores.get(sid, {})
                reg["ativo"] = True
                reg["iniciando"] = False
                reg["reservado_em"] = None
                if dados.get("ip") is not None:
                    reg["ip"] = dados["ip"]
                if dados.get("port") is not None:
                    reg["port"] = int(dados["port"])
                self.servidores[sid] = reg
                return True

            if tipo == "SRV_RESERVED":
                sid = int(dados["id"])
                reg = self.servidores.get(sid, {})
                if not reg.get("ativo") and "ativo" not in reg:
                    reg["ativo"] = False
                    reg["iniciando"] = True
                    reg["reservado_em"] = None
                if dados.get("ip") is not None:
                    reg["ip"] = dados["ip"]
                if dados.get("port") is not None:
                    reg["port"] = int(dados["port"])
                self.servidores[sid] = reg
                return True

            if tipo == "SRV_ACTIVE":
                sid = int(dados["id"])
                reg = self.servidores.get(sid, {})
                reg["ativo"] = True
                reg["iniciando"] = False
                reg["reservado_em"] = None
                if dados.get("ip") is not None:
                    reg["ip"] = dados["ip"]
                if dados.get("port") is not None:
                    reg["port"] = int(dados["port"])
                self.servidores[sid] = reg
                return True

            if tipo == "SRV_DEAD":
                sid = int(dados["id"])
                reg = self.servidores.get(sid, {})
                reg["ativo"] = False
                reg["iniciando"] = False
                self.servidores[sid] = reg
                return True

            if tipo == "USER_ADD":
                uid = int(dados["uid"])
                sid = dados["sid"]
                self.usuario_servidor[uid] = int(sid) if sid is not None else None
                self.nomes_usuarios[uid] = dados.get("nome")
                return True

            if tipo == "USER_MOVE":
                uid = int(dados["uid"])
                sid = dados["sid"]
                self.usuario_servidor[uid] = int(sid) if sid is not None else None
                return True

            if tipo == "MSG_DONE":
                remetente = int(dados["sender"])
                id_msg = int(dados["msg_id"])
                destinatario = int(dados["dest"])
                self.processadas[(remetente, id_msg)] = destinatario
                return True

            return False

    def resumo_crc(self) -> str:
        """Retorna o CRC32 hexadecimal de 8 dígitos do estado replicado canônico."""
        with self.lock:
            servidores_canonicos = {
                str(sid): {k: v for k, v in sorted(info.items()) if k != "reservado_em"}
                for sid, info in sorted(self.servidores.items())
            }
            usuarios_canonicos = {
                str(uid): sid for uid, sid in sorted(self.usuario_servidor.items())
            }
            nomes_canonicos = {
                str(uid): nome for uid, nome in sorted(self.nomes_usuarios.items())
            }
            mensagens_canonicas = {
                f"{rem}:{id_msg}": dest
                for (rem, id_msg), dest in sorted(self.processadas.items())
            }
            carga = json.dumps(
                {
                    "servers": servidores_canonicos,
                    "user_server": usuarios_canonicos,
                    "user_names": nomes_canonicos,
                    "processed": mensagens_canonicas,
                },
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode("utf-8")
            crc = zlib.crc32(carga) & 0xFFFFFFFF
            return f"{crc:08x}"