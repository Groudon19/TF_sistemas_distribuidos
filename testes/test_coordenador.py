from concurrent.futures import ThreadPoolExecutor
from sd.common.logger import MetricsLogger
from sd.coordenador import Coordenador
from sd.estado import Estado


def _make_coord(tmp_path, capacity=5) -> tuple[Estado, Coordenador]:
    logger = MetricsLogger("coordenador", 0, log_dir=tmp_path)
    state = Estado(my_id=0, is_coordinator=True, capacity=capacity)
    for sid in (0, 1, 2):
        state.register_server(sid)
    return state, Coordenador(state, logger)


def test_distribuicao_e_migracao_minima(tmp_path):
    state, coord = _make_coord(tmp_path, capacity=5)

    # 6 usuários -> 2 em cada servidor (0, 1, 2)
    for uid in range(1, 7):
        resp, stop = coord.handle_line(f"NEWUSER_{uid}_User{uid}")
        assert resp == f"ACK_NEWUSER_{uid}"
        assert not stop

    assert state.loads() == {0: 2, 1: 2, 2: 2}
    users_on_0 = set(state.users_of(0))
    users_on_2 = set(state.users_of(2))

    # Mata servidor 1 -> só os usuários dele migram para 0 e 2
    resp, stop = coord.handle_line("KILL_1")
    assert resp == "ACK_KILL_1"
    assert not stop
    assert users_on_0.issubset(set(state.users_of(0)))
    assert users_on_2.issubset(set(state.users_of(2)))
    assert state.load_of(0) == 3
    assert state.load_of(2) == 3

    # KILL_1 repetido é idempotente
    resp2, _ = coord.handle_line("KILL_1")
    assert resp2 == "ACK_KILL_1"

    # KILL de servidor desconhecido
    resp3, _ = coord.handle_line("KILL_9")
    assert resp3 == "ERROR_UNKNOWN_SERVER"


def test_kill_com_servidores_cheios_gera_pendentes(tmp_path):
    state, coord = _make_coord(tmp_path, capacity=2)

    # 6 usuários lotam os 3 servidores (capacidade 2 cada)
    for uid in range(1, 7):
        coord.handle_line(f"NEWUSER_{uid}")

    resp, _ = coord.handle_line("KILL_2")
    assert resp == "ACK_KILL_2"
    assert len(state.unassigned_users()) == 2


def test_concorrencia_mesmo_newuser_em_20_threads(tmp_path):
    state, coord = _make_coord(tmp_path, capacity=5)

    with ThreadPoolExecutor(max_workers=20) as pool:
        resps = list(pool.map(lambda _: coord.handle_line("NEWUSER_42_Arthur"), range(20)))

    assert all(r == ("ACK_NEWUSER_42", False) for r in resps)
    assert state.total_users() == 1