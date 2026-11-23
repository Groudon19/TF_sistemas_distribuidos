import threading
import time
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
            entry["starting"] = False
            entry["reserved_at"] = None
            if ip is not None:
                entry["ip"] = ip
            if port is not None:
                entry["port"] = port
            self.servers[server_id] = entry

    def reserve_server(self, ip: str, port_of) -> int:
        """
        Reserva atomicamente o próximo ID disponível (max + 1 ou 0),
        registra como starting e grava reserved_at.
        """
        with self.lock:
            next_id = max(self.servers.keys(), default=-1) + 1
            port = port_of(next_id)  # pode levantar ValueError se > 65535
            self.servers[next_id] = {
                "alive": False,
                "starting": True,
                "ip": ip,
                "port": port,
                "reserved_at": time.monotonic(),
            }
            return next_id

    def is_starting(self, server_id: int) -> bool:
        with self.lock:
            info = self.servers.get(server_id)
            return bool(info and info.get("starting"))

    def starting_server_ids(self) -> list[int]:
        with self.lock:
            return sorted(sid for sid, info in self.servers.items() if info.get("starting"))

    def reserved_at(self, server_id: int) -> float | None:
        with self.lock:
            info = self.servers.get(server_id)
            return info.get("reserved_at") if info else None

    def mark_active(self, server_id: int) -> str:
        """
        Transiciona STARTING -> ACTIVE.
        Retorna: 'ACTIVATED' | 'ALREADY_ACTIVE' | 'UNKNOWN' | 'DEAD'.
        """
        with self.lock:
            info = self.servers.get(server_id)
            if info is None:
                return "UNKNOWN"
            if info.get("alive"):
                return "ALREADY_ACTIVE"
            if info.get("starting"):
                info["alive"] = True
                info["starting"] = False
                return "ACTIVATED"
            return "DEAD"

    def abort_starting(self, server_id: int) -> bool:
        """Transiciona STARTING -> DEAD. Retorna True se estava starting."""
        with self.lock:
            info = self.servers.get(server_id)
            if info and info.get("starting"):
                info["starting"] = False
                info["alive"] = False
                return True
            return False

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

    def address_of(self, server_id: int) -> tuple[str, int] | None:
        with self.lock:
            info = self.servers.get(server_id)
            if not info or "ip" not in info or "port" not in info:
                return None
            return info["ip"], info["port"]

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
        """Retorna {server_id: carga} apenas dos servidores ativos."""
        with self.lock:
            active_loads = {sid: 0 for sid, info in self.servers.items() if info.get("alive")}
            for sid in self.user_server.values():
                if sid is not None and sid in active_loads:
                    active_loads[sid] += 1
            return active_loads

    def free_slots(self) -> int:
        with self.lock:
            return sum(self.capacity - load for load in self.loads().values())

    def idle_server_ids(self) -> list[int]:
        """Retorna IDs ordenados de servidores ativos com carga 0, excluindo my_id."""
        with self.lock:
            active_loads = self.loads()
            return sorted(
                sid for sid, load in active_loads.items() if sid != self.my_id and load == 0
            )

    def retire_if_idle(self, server_id: int) -> bool:
        """Marca como morto atomicamente apenas se ativo, != my_id e carga == 0."""
        with self.lock:
            if server_id == self.my_id:
                return False
            info = self.servers.get(server_id)
            if not info or not info.get("alive"):
                return False
            if self.load_of(server_id) > 0:
                return False
            info["alive"] = False
            info["starting"] = False
            return True

    def total_users(self) -> int:
        with self.lock:
            return len(self.user_server)

    def unassigned_users(self) -> list[int]:
        with self.lock:
            return sorted(uid for uid, sid in self.user_server.items() if sid is None)

    def add_user(self, user_id: int, name: str | None, server_id: int) -> str:
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
        with self.lock:
            srv = self.servers.get(server_id)
            if not srv or not srv.get("alive"):
                return None

            srv["alive"] = False
            srv["starting"] = False
            orphans = sorted(uid for uid, sid in self.user_server.items() if sid == server_id)
            for uid in orphans:
                self.user_server[uid] = None
            return orphans

    def apply_assignments(self, assignments: dict[int, int]) -> list[int]:
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
        """Retorna cópia rasa dos dicionários (omitindo reserved_at local)."""
        with self.lock:
            servers_copy = {}
            for sid, info in self.servers.items():
                entry = {k: v for k, v in info.items() if k != "reserved_at"}
                servers_copy[sid] = entry
            return {
                "my_id": self.my_id,
                "is_coordinator": self.is_coordinator,
                "capacity": self.capacity,
                "servers": servers_copy,
                "user_server": dict(self.user_server),
                "user_names": dict(self.user_names),
            }