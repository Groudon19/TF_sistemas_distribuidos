import threading
import time
from unittest.mock import MagicMock

from sd.common.config import port_for_server
from sd.common.logger import MetricsLogger
from sd.coordenador import Coordenador
from sd.escalador import Escalador
from sd.estado import Estado
from sd import spawner


class FakeClock:
    def __init__(self, start: float = 100.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, dt: float) -> None:
        self.now += dt


def _setup(tmp_path, capacity=5, wait_sec=0.3):
    clock = FakeClock()
    logger = MetricsLogger("coordenador", 0, log_dir=tmp_path)
    state = Estado(my_id=0, is_coordinator=True, capacity=capacity)
    state.register_server(0, "127.0.0.1", 5001)

    coord = Coordenador(state=state, logger=logger, wait_sec=wait_sec)

    spawned = []
    procs = {}

    def fake_spawn(sid, ip, port):
        spawned.append(sid)
        p = MagicMock()
        p.poll.return_value = None
        procs[sid] = p
        return p

    esc = Escalador(
        coordenador=coord,
        logger=logger,
        spawn_fn=fake_spawn,
        newid_fn=lambda sid, lg: True,
        shutdown_fn=lambda ip, port, sid: True,
        terminate_fn=spawner.terminate_process,
        now_fn=clock,
    )
    coord.attach_scaler(esc)
    return clock, state, coord, esc, spawned, procs


def test_ciclo_criacao_hello_e_substituicao(tmp_path, monkeypatch):
    clock, state, coord, esc, spawned, _ = _setup(tmp_path, capacity=5)
    monkeypatch.setattr(time, "monotonic", clock)

    # 4 usuários -> nada criado
    for uid in range(1, 5):
        assert coord.handle_line(f"NEWUSER_{uid}") == (f"ACK_NEWUSER_{uid}", False)
    esc.evaluate()
    assert spawned == []

    # 5º usuário -> cria servidor 1 (STARTING)
    assert coord.handle_line("NEWUSER_5") == ("ACK_NEWUSER_5", False)
    esc.evaluate()
    assert spawned == [1]
    assert state.is_starting(1)

    # evaluate repetido sem HELLO não cria duplicata
    esc.evaluate()
    assert spawned == [1]

    # HELLO_1 ativa o servidor e é idempotente
    assert coord.handle_line("HELLO_1") == ("ACK_HELLO_1", False)
    assert state.is_alive(1)
    assert coord.handle_line("HELLO_1") == ("ACK_HELLO_1", False)
    assert coord.handle_line("HELLO_9") == ("ERROR_UNKNOWN_SERVER", False)

    # Mais 5 usuários (total 10) -> cria servidor 2
    for uid in range(6, 11):
        coord.handle_line(f"NEWUSER_{uid}")
    esc.evaluate()
    assert spawned == [1, 2]
    coord.handle_line("HELLO_2")

    # Mata servidor 1 (que tem 5 usuários); servidor 0 está cheio e servidor 2 tem 5 vagas -> recoloca todos
    assert coord.handle_line("KILL_1") == ("ACK_KILL_1", False)
    assert state.unassigned_users() == []


def test_kill_com_pendentes_e_hello_do_substituto(tmp_path, monkeypatch):
    clock, state, coord, esc, spawned, _ = _setup(tmp_path, capacity=2)
    monkeypatch.setattr(time, "monotonic", clock)

    # 4 usuários lotam 0 e 1 (2 cada), e cria servidor 2
    for uid in range(1, 5):
        coord.handle_line(f"NEWUSER_{uid}")
        esc.evaluate()
        for sid in state.starting_server_ids():
            coord.handle_line(f"HELLO_{sid}")

    # Mata servidor 1 enquanto servidor 2 ainda não nasceu (aborta 2 ou lota)
    # Vamos lotar 0, 1 e 2 com 6 usuários e matar o 1
    coord.handle_line("NEWUSER_5")
    coord.handle_line("NEWUSER_6")
    assert coord.handle_line("KILL_1") == ("ACK_KILL_1", False)
    assert len(state.unassigned_users()) == 2

    # Escalador cria substituto (ID 3) por motivo "pendentes"
    esc.evaluate()
    assert 3 in spawned
    assert coord.handle_line("HELLO_3") == ("ACK_HELLO_3", False)
    assert state.unassigned_users() == []


def test_falhas_spawn_timeout_e_backoff(tmp_path, monkeypatch):
    clock, state, coord, esc, spawned, procs = _setup(tmp_path, capacity=1)
    monkeypatch.setattr(time, "monotonic", clock)

    coord.handle_line("NEWUSER_1")
    esc.evaluate()
    assert spawned == [1]

    # Timeout do servidor 1 -> abortado, próximo nasce como ID 2
    clock.advance(15.0)
    esc.evaluate()
    assert not state.is_starting(1)
    esc.evaluate()
    assert spawned == [1, 2]

    # Processo 2 morre prematuramente -> abortado na hora
    procs[2].poll.return_value = 1
    esc.evaluate()
    assert not state.is_starting(2)

    # 3ª falha seguida ativa backoff (pausa criação)
    esc.evaluate()
    assert spawned == [1, 2, 3]
    procs[3].poll.return_value = 1
    esc.evaluate()
    esc.evaluate()  # durante o backoff não deve criar ID 4
    assert spawned == [1, 2, 3]


def test_kill_de_servidor_starting(tmp_path, monkeypatch):
    clock, state, coord, esc, spawned, procs = _setup(tmp_path, capacity=1)
    monkeypatch.setattr(time, "monotonic", clock)

    coord.handle_line("NEWUSER_1")
    esc.evaluate()
    assert state.is_starting(1)

    assert coord.handle_line("KILL_1") == ("ACK_KILL_1", False)
    assert not state.is_starting(1)
    procs[1].terminate.assert_called_once()


def test_desativacao_ociosos_respeita_carencia_e_maior_id(tmp_path, monkeypatch):
    clock, state, coord, esc, _, _ = _setup(tmp_path, capacity=5)
    monkeypatch.setattr(time, "monotonic", clock)

    for sid in (1, 2, 3):
        state.register_server(sid, "127.0.0.1", 5001 + sid)

    # Registra início da ociosidade
    esc.evaluate()
    clock.advance(2.0)  # menor que IDLE_GRACE_SEC (3.0)
    esc.evaluate()
    assert state.alive_server_ids() == [0, 1, 2, 3]

    # Passou da carência: desativa 1 por vez, do maior ID para o menor, nunca o 0
    clock.advance(2.0)
    esc.evaluate()
    assert state.alive_server_ids() == [0, 1, 2]
    esc.evaluate()
    assert state.alive_server_ids() == [0, 1]
    esc.evaluate()
    assert state.alive_server_ids() == [0]
    esc.evaluate()
    assert state.alive_server_ids() == [0]


def test_espera_newuser_com_hello_e_timeout(tmp_path):
    _, state, coord, esc, _, _ = _setup(tmp_path, capacity=1, wait_sec=0.35)
    coord.handle_line("NEWUSER_1")  # lota servidor 0

    # Caso 1: HELLO chega durante a espera -> thread recebe ACK_NEWUSER_2
    state.reserve_server("127.0.0.1", port_for_server)
    results = []

    def send_u2():
        results.append(coord.handle_line("NEWUSER_2"))

    t = threading.Thread(target=send_u2)
    t.start()
    time.sleep(0.08)
    coord.handle_line("HELLO_1")
    t.join(timeout=1.0)
    assert results == [("ACK_NEWUSER_2", False)]

    # Caso 2: Sem HELLO -> estoura wait_sec e devolve ERROR_NO_CAPACITY
    t0 = time.monotonic()
    resp, _ = coord.handle_line("NEWUSER_3")
    elapsed = time.monotonic() - t0
    assert resp == "ERROR_NO_CAPACITY"
    assert elapsed >= 0.3