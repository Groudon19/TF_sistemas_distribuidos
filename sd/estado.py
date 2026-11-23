import threading
from sd.common.config import MAX_USERS_PER_SERVER


class Estado:
    """Estado em memória de um nó do SD, protegido por RLock."""

    def __init__(self, my_id: int, is_coordinator: bool = False, capacity: int = MAX_USERS_PER_SERVER):
        self.lock = threading.RLock()
        self.my_id = my_id
        self.is_coordinator = is_coordinator
        self.capacity = capacity
        self.servers: dict[int, dict] = {}
        self.user_server: dict[int, int | None] = {}
        self.user_names: dict[int, str | None] = {}

    def register_server(self, server_id: int, ip: str | None = None, port: int | None = None) -> None:
        with self.lock:
            entry = self.servers.get(server_id, {})
            entry["alive"] = True
            if ip is not None:
                entry["ip"] = ip
            if port is not None:
                entry["port"] = port
            self.servers[server_id] = entry

    def alive_server_ids(self) -> list[int]:
        with self.lock:
            return sorted(sid for sid, info in self.servers.items() if info.get("alive"))

    def is_known_server(self, server_id: int) -> bool:
        with self.lock:
            return server_id in self.servers

    def is_alive(self, server_id: int) -> bool:
        with self.lock:
            info = self.servers.get(server_id)
            return bool(info and info.get("alive"))

    def has_user(self, user_id: int) -> bool:
        with self.lock:
            return user_id in self.user_server

    def server_of(self, user_id: int) -> int | None:
        with self.lock:
            return self.user_server.get(user_id)

    def users_of(self, server_id: int) -> list[int]:
        with self.lock:
            return [uid for uid, sid in self.user_server.items() if sid == server_id]

    def load_of(self, server_id: int) -> int:
        with self.lock:
            return sum(1 for sid in self.user_server.values() if sid == server_id)

    def loads(self) -> dict[int, int]:
        """Retorna {server_id: carga} para todos os servidores ativos."""
        with self.lock:
            active_loads = {sid: 0 for sid, info in self.servers.items() if info.get("alive")}
            for sid in self.user_server.values():
                if sid is not None and sid in active_loads:
                    active_loads[sid] += 1
            return active_loads

    def total_users(self) -> int:
        with self.lock:
            return len(self.user_server)

    def unassigned_users(self) -> list[int]:
        """Retorna IDs ordenados de usuários que existem mas estão sem servidor (None)."""
        with self.lock:
            return sorted(uid for uid, sid in self.user_server.items() if sid is None)

    def add_user(self, user_id: int, name: str | None, server_id: int) -> str:
        """
        Insere um usuário de forma atômica no servidor especificado.
        Retorna: 'ADDED' | 'DUPLICATE' | 'FULL' | 'NO_SERVER'.
        """
        with self.lock:
            if user_id in self.user_server:
                return "DUPLICATE"

            srv = self.servers.get(server_id)
            if not srv or not srv.get("alive"):
                return "NO_SERVER"

            if self.load_of(server_id) >= self.capacity:
                return "FULL"

            self.user_server[user_id] = server_id
            self.user_names[user_id] = name
            return "ADDED"

    def fail_server(self, server_id: int) -> list[int] | None:
        """
        Marca o servidor como morto (mantendo-o no dicionário para idempotência),
        define user_server[uid] = None para seus usuários e retorna os órfãos ordenados.
        Retorna None se o servidor for desconhecido ou já estiver morto.
        """
        with self.lock:
            srv = self.servers.get(server_id)
            if not srv or not srv.get("alive"):
                return None

            srv["alive"] = False
            orphans = sorted(uid for uid, sid in self.user_server.items() if sid == server_id)
            for uid in orphans:
                self.user_server[uid] = None
            return orphans

    def apply_assignments(self, assignments: dict[int, int]) -> list[int]:
        """
        Aplica atribuições {user_id: server_id} respeitando servidor vivo e capacidade.
        Retorna lista ordenada dos user_ids que não puderam ser aplicados.
        """
        with self.lock:
            rejected: list[int] = []
            current_loads = self.loads()

            for uid in sorted(assignments):
                sid = assignments[uid]
                if sid not in current_loads or current_loads[sid] >= self.capacity:
                    rejected.append(uid)
                    continue
                if uid in self.user_server:
                    self.user_server[uid] = sid
                    current_loads[sid] += 1
                else:
                    rejected.append(uid)

            return rejected

    def snapshot(self) -> dict:
        """Retorna cópia rasa dos dicionários (valores simples) para replicação ou inspeção."""
        with self.lock:
            return {
                "my_id": self.my_id,
                "is_coordinator": self.is_coordinator,
                "capacity": self.capacity,
                "servers": {sid: dict(info) for sid, info in self.servers.items()},
                "user_server": dict(self.user_server),
                "user_names": dict(self.user_names),
            }