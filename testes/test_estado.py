from concurrent.futures import ThreadPoolExecutor
from sd.estado import Estado


def test_fail_server_e_idempotencia():
    st = Estado(my_id=0, is_coordinator=True, capacity=5)
    st.register_server(0)
    st.register_server(1)

    st.add_user(10, "Ana", 1)
    st.add_user(20, "Bia", 1)
    st.add_user(30, "Caio", 0)

    orphans = st.fail_server(1)
    assert orphans == [10, 20]
    assert not st.is_alive(1)
    assert st.is_known_server(1)
    assert st.unassigned_users() == [10, 20]
    assert st.server_of(10) is None
    assert st.server_of(30) == 0

    # Segunda chamada deve retornar None
    assert st.fail_server(1) is None
    # Servidor nunca registrado também retorna None
    assert st.fail_server(99) is None


def test_add_user_em_servidor_morto_ou_inexistente():
    st = Estado(my_id=0, capacity=5)
    st.register_server(0)
    st.register_server(1)
    st.fail_server(1)

    assert st.add_user(1, "Ana", 1) == "NO_SERVER"
    assert st.add_user(2, "Bia", 99) == "NO_SERVER"


def test_apply_assignments_respeita_capacidade():
    st = Estado(my_id=0, capacity=2)
    st.register_server(0)
    st.register_server(1)

    st.add_user(1, "U1", 0)
    st.add_user(2, "U2", 1)
    st.add_user(3, "U3", 1)
    st.fail_server(1)  # 2 e 3 viram órfãos; srv 0 tem apenas 1 vaga livre

    rejected = st.apply_assignments({2: 0, 3: 0})
    assert rejected == [3]
    assert st.server_of(2) == 0
    assert st.unassigned_users() == [3]


def test_concorrencia_add_user_nao_estoura_capacidade():
    st = Estado(my_id=0, capacity=5)
    st.register_server(0)

    def worker(uid: int) -> str:
        return st.add_user(uid, f"User{uid}", 0)

    with ThreadPoolExecutor(max_workers=20) as pool:
        results = list(pool.map(worker, range(1, 51)))

    assert results.count("ADDED") == 5
    assert results.count("FULL") == 45
    assert st.load_of(0) == 5