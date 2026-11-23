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
        self.user_server: dict[int, int] = {}
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
                if sid in active_loads:
                    active_loads[sid] += 1
            return active_loads

    def total_users(self) -> int:
        with self.lock:
            return len(self.user_server)

    def add_user(self, user_id: int, name: str | None, server_id: int) -> str:
        """
        Insere um usuário de forma atômica no servidor especificado.
        Retorna: 'ADDED', 'DUPLICATE' ou 'FULL'.
        """
        with self.lock:
            if user_id in self.user_server:
                return "DUPLICATE"

            srv = self.servers.get(server_id)
            if not srv or not srv.get("alive"):
                return "FULL"

            if self.load_of(server_id) >= self.capacity:
                return "FULL"

            self.user_server[user_id] = server_id
            self.user_names[user_id] = name
            return "ADDED"

    def snapshot(self) -> dict:
        """Retorna cópia profunda do estado para replicação ou inspeção."""
        with self.lock:
            return {
                "my_id": self.my_id,
                "is_coordinator": self.is_coordinator,
                "capacity": self.capacity,
                "servers": {sid: dict(info) for sid, info in self.servers.items()},
                "user_server": dict(self.user_server),
                "user_names": dict(self.user_names),
            }