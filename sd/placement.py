import statistics


def pick_server_for_new_user(loads: dict[int, int], capacity: int) -> int | None:
    """
    Escolhe o servidor ativo com menor carga (estritamente < capacity).
    Em caso de empate na carga, escolhe o de menor server_id.
    Retorna None se não houver servidores ou se todos estiverem cheios.
    """
    candidates = [(load, sid) for sid, load in loads.items() if load < capacity]
    if not candidates:
        return None
    _, chosen_id = min(candidates)
    return chosen_id


def reassign_orphans(
    orphans: list[int], loads: dict[int, int], capacity: int
) -> tuple[dict[int, int], list[int]]:
    """
    Redistribui usuários órfãos entre os servidores sobreviventes (loads),
    respeitando a capacidade máxima de cada servidor.
    Retorna (atribuicoes {user_id: server_id}, pendentes [user_id]).
    """
    working_loads = dict(loads)
    assignments: dict[int, int] = {}
    pending: list[int] = []

    for uid in sorted(orphans):
        target = pick_server_for_new_user(working_loads, capacity)
        if target is None:
            pending.append(uid)
        else:
            assignments[uid] = target
            working_loads[target] += 1

    return assignments, pending


def balance_metrics(loads: dict[int, int]) -> dict:
    """
    Calcula métricas de balanceamento sobre os servidores ativos.
    Nota: utiliza statistics.pstdev (desvio padrão populacional), pois 'loads'
    representa a população completa dos servidores ativos no sistema.
    """
    if not loads:
        return {
            "desbalanceamento": 0,
            "desvio_padrao": 0.0,
            "servidores": 0,
            "usuarios": 0,
        }

    values = list(loads.values())
    return {
        "desbalanceamento": max(values) - min(values),
        "desvio_padrao": round(statistics.pstdev(values), 4),
        "servidores": len(values),
        "usuarios": sum(values),
    }


def servers_needed(total_users: int, capacity: int) -> int:
    """Calcula teto(total_users / capacity) usando aritmética inteira."""
    if total_users <= 0 or capacity <= 0:
        return 0
    return -(-total_users // capacity)