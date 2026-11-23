from sd.placement import (
    balance_metrics,
    pick_server_for_new_user,
    reassign_orphans,
    servers_needed,
)


def test_pick_server_for_new_user():
    assert pick_server_for_new_user({0: 5, 1: 5, 2: 3}, 5) == 2
    assert pick_server_for_new_user({0: 3, 1: 1, 2: 2}, 5) == 1
    assert pick_server_for_new_user({0: 2, 1: 2}, 5) == 0
    assert pick_server_for_new_user({0: 5, 1: 5}, 5) is None
    assert pick_server_for_new_user({}, 5) is None


def test_reassign_orphans_equilibrio_e_sem_pendentes():
    loads = {0: 2, 1: 2}
    orphans = [10, 20, 30, 40, 50]
    assignments, pending = reassign_orphans(orphans, loads, capacity=5)

    assert pending == []
    assert len(assignments) == 5
    # Cargas finais devem ficar em {0: 5, 1: 4} -> diferença <= 1
    final_0 = loads[0] + sum(1 for s in assignments.values() if s == 0)
    final_1 = loads[1] + sum(1 for s in assignments.values() if s == 1)
    assert abs(final_0 - final_1) <= 1


def test_reassign_orphans_com_pendentes_e_imutabilidade():
    loads_orig = {0: 5, 1: 4, 2: 4}
    orphans = [5, 1, 3, 2, 4]
    assignments, pending = reassign_orphans(orphans, loads_orig, capacity=5)

    # Apenas 2 vagas livres (1 no srv 1, 1 no srv 2). Órfãos 1 e 2 são atendidos primeiro.
    assert assignments == {1: 1, 2: 2}
    assert pending == [3, 4, 5]
    # Garante que o dicionário original não foi mutado
    assert loads_orig == {0: 5, 1: 4, 2: 4}


def test_reassign_orphans_determinismo_independente_da_ordem():
    loads = {0: 1, 1: 3, 2: 2}
    res_a = reassign_orphans([10, 20, 30, 40], loads, capacity=5)
    res_b = reassign_orphans([40, 10, 30, 20], loads, capacity=5)
    res_c = reassign_orphans([40, 10, 30, 20], loads, capacity=5)

    assert res_a == res_b == res_c


def test_balance_metrics():
    m = balance_metrics({0: 5, 1: 5, 2: 3})
    assert m["desbalanceamento"] == 2
    assert m["servidores"] == 3
    assert m["usuarios"] == 13
    assert m["desvio_padrao"] > 0

    vazio = balance_metrics({})
    assert vazio == {
        "desbalanceamento": 0,
        "desvio_padrao": 0.0,
        "servidores": 0,
        "usuarios": 0,
    }


def test_servers_needed():
    assert servers_needed(0, 5) == 0
    assert servers_needed(1, 5) == 1
    assert servers_needed(5, 5) == 1
    assert servers_needed(6, 5) == 2
    assert servers_needed(10, 5) == 2
    assert servers_needed(11, 5) == 3