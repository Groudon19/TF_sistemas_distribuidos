import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from sd.common.logger import MetricsLogger
from sd.common.protocolo import (
    ProtocolError,
    fmt_deliver,
    fmt_msg,
    fmt_repl,
    parse_command,
    parse_deliver,
    parse_repl,
)
from sd.coordenador import Coordenador
from sd.estado import Estado
from sd.mensagens import Receptor, Roteador
from sd.replicacao import Aplicador, OpLog, Replicador
from sd.usuarios import AgenteUsuario


def test_protocolo_repl_deliver_digest():
    op = {"seq": 1, "epoch": 1, "ts": 123.45, "type": "USER_ADD", "data": {"uid": 1, "nome": 'João "A"\nB', "sid": 0}}
    wire = fmt_repl(op)
    assert "\n" not in wire
    assert parse_repl(wire) == op

    for bad in ["REPL_{quebrado", 'REPL_{"type":"USER_ADD","data":{}}', 'REPL_{"seq":1,"epoch":1,"ts":1,"type":"FOO","data":{}}']:
        with pytest.raises(ProtocolError):
            parse_repl(bad)

    d_wire = fmt_deliver(10, 1, 2, "texto_com_underscore_e_acentos_ç")
    assert parse_deliver(d_wire) == (10, 1, 2, "texto_com_underscore_e_acentos_ç")
    assert parse_command("DIGEST") == "DIGEST"


def test_equivalencia_replay_e_idempotencia_apply_op(tmp_path):
    logger = MetricsLogger("coordenador", 0, log_dir=tmp_path)
    oplog = OpLog(0, log_dir=tmp_path)
    rep = Replicador(oplog=oplog, logger=logger)

    st_leader = Estado(my_id=0, is_coordinator=True, capacity=2)
    st_leader.register_server(0, "127.0.0.1", 5001)
    rep.append_locked("SRV_UP", {"id": 0, "ip": "127.0.0.1", "port": 5001})

    coord = Coordenador(state=st_leader, logger=logger)
    coord.attach_replicator(rep)

    # Reserva e ativa servidor 1, adiciona 4 usuários e mata servidor 1
    sids = coord.reserve_servers(1)
    assert sids == [1]
    coord.handle_line("HELLO_1")
    for uid in range(1, 5):
        coord.handle_line(f"NEWUSER_{uid}_User{uid}")
    coord.handle_line("KILL_1")

    # Reaplica todo o log num Estado limpo
    st_replica = Estado(my_id=1, is_coordinator=False, capacity=2)
    for op in oplog.get_from(1):
        assert st_replica.apply_op(op) is True

    assert st_replica.digest() == st_leader.digest()

    # Aplicar novamente (idempotência) preserva o digest; tipo desconhecido retorna False
    for op in oplog.get_from(1):
        assert st_replica.apply_op(op) is True
    assert st_replica.digest() == st_leader.digest()
    assert st_replica.apply_op({"type": "UNKNOWN_OP", "data": {}}) is False
    oplog.close()


def test_aplicador_ordem_duplicata_lacuna_e_epoca(tmp_path):
    logger = MetricsLogger("servidor", 1, log_dir=tmp_path)
    oplog = OpLog(1, log_dir=tmp_path)
    st = Estado(my_id=1, is_coordinator=False)
    app = Aplicador(state=st, oplog=oplog, logger=logger, epoch=2)

    op1 = {"seq": 1, "epoch": 2, "ts": time.time(), "type": "SRV_UP", "data": {"id": 0, "ip": "127.0.0.1", "port": 5001}}
    op3 = {"seq": 3, "epoch": 2, "ts": time.time(), "type": "SRV_DEAD", "data": {"id": 0}}
    op_old = {"seq": 2, "epoch": 1, "ts": time.time(), "type": "SRV_DEAD", "data": {"id": 0}}

    assert app.handle_repl(fmt_repl(op1)) == ("ACK_REPL_1", False)
    assert app.handle_repl(fmt_repl(op1)) == ("ACK_REPL_1", False)  # duplicata
    assert app.handle_repl(fmt_repl(op3)) == ("ERROR_GAP_1", False)  # lacuna
    assert app.handle_repl(fmt_repl(op_old)) == ("ERROR_STALE_EPOCH_2", False)  # época antiga
    oplog.close()


def test_replicador_lento_reconexao_e_novo_seguidor(tmp_path):
    logger = MetricsLogger("coordenador", 0, log_dir=tmp_path)
    oplog = OpLog(0, log_dir=tmp_path)

    st_f1 = Estado(my_id=1)
    app_f1 = Aplicador(st_f1, OpLog(1, log_dir=tmp_path), logger)

    st_f3 = Estado(my_id=3)
    app_f3 = Aplicador(st_f3, OpLog(3, log_dir=tmp_path), logger)

    reconnect_attempts = 0

    def send_fn(ip, port, line):
        nonlocal reconnect_attempts
        if port == 5002:  # seguidor 1 rápido
            return app_f1.handle_repl(line)[0]
        if port == 5003:  # seguidor 2 lento
            time.sleep(0.35)
            return "ACK_REPL_1"
        if port == 5004:  # seguidor 3 que reconecta e pega o log desde o seq 1
            reconnect_attempts += 1
            if reconnect_attempts == 1:
                raise OSError("falha temporaria")
            return app_f3.handle_repl(line)[0]
        raise OSError("offline")

    rep = Replicador(oplog=oplog, logger=logger, send_fn=send_fn)
    # Adiciona seguidores 1 e 2 antes do append -> nascem SYNCED
    rep.add_follower(1, "127.0.0.1", 5002)
    rep.add_follower(2, "127.0.0.1", 5003)

    seq1 = rep.append_locked("SRV_UP", {"id": 0, "ip": "127.0.0.1", "port": 5001})
    res = rep.wait_replicated(seq1, timeout=0.15)
    assert res["lagging"] == 1  # seguidor 2 estoura o prazo e vira LAGGING sem atrasar os demais

    seq2 = rep.append_locked("USER_ADD", {"uid": 10, "nome": "Ana", "sid": 0})
    res2 = rep.wait_replicated(seq2, timeout=0.2)
    assert res2["synced"] == 1
    assert st_f1.has_user(10)

    # Novo seguidor 3 entra depois: reconecta na 1ª falha e recebe todo o log desde seq 1
    rep.add_follower(3, "127.0.0.1", 5004)
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline and not st_f3.has_user(10):
        time.sleep(0.05)
    assert st_f3.has_user(10)

    rep.stop()
    oplog.close()


def test_receptor_concorrencia_20_entregas_iguais(tmp_path):
    logger = MetricsLogger("servidor", 1, log_dir=tmp_path)
    st = Estado(my_id=1)
    st.register_server(1)
    st.add_user(20, "Dest", 1)

    exec_count = 0
    count_lock = threading.Lock()

    def fast_receive(content):
        nonlocal exec_count
        if content is None:
            return False
        time.sleep(0.05)
        with count_lock:
            exec_count += 1
        return True

    rec = Receptor(state=st, logger=logger, log_dir=tmp_path, receive_fn=fast_receive, chaos_drop_prob=0.0)
    wire = fmt_deliver(999, 10, 20, "ola")

    with ThreadPoolExecutor(max_workers=20) as pool:
        results = list(pool.map(lambda _: rec.handle_deliver(wire), range(20)))

    assert exec_count == 1
    assert all(r == ("ACK_999", False) for r in results)

    # Novo envio após DONE não reexecuta
    assert rec.handle_deliver(wire) == ("ACK_999", False)
    assert exec_count == 1

    # Destinatário de outro servidor -> ERROR_NOT_HERE
    assert rec.handle_deliver(fmt_deliver(1000, 10, 99, "ola")) == ("ERROR_NOT_HERE", False)
    # Conteúdo None -> ERROR_PROCESSING
    assert rec.receive_fn(None) is False
    rec.close()


def test_roteador_migracao_e_chave_por_remetente(tmp_path):
    logger = MetricsLogger("coordenador", 0, log_dir=tmp_path)
    st = Estado(my_id=0, is_coordinator=True)
    st.register_server(0, "127.0.0.1", 5001)
    st.register_server(1, "127.0.0.1", 5002)
    st.register_server(2, "127.0.0.1", 5003)
    st.add_user(1, "U1", 0)
    st.add_user(2, "U2", 0)
    st.add_user(10, "Dest", 1)

    attempts = []

    def fake_deliver(ip, port, line, timeout):
        attempts.append(port)
        if port == 5002:
            # Simula migração do destinatário para o servidor 2 no meio da entrega
            st.user_server[10] = 2
            return "ERROR_NOT_HERE"
        return "ACK_50"

    coord = Coordenador(state=st, logger=logger)
    rot = Roteador(coordenador=coord, logger=logger, deliver_fn=fake_deliver)

    # Remetente 1 envia msg_id=50 -> migra de 5002 para 5003 e confirma
    assert rot.handle_msg(fmt_msg(50, 1, 10, "oi")) == ("ACK_50", False)
    assert attempts == [5002, 5003]

    # Remetente 2 envia o MESMO msg_id=50 -> NÃO é duplicata porque sender é diferente!
    attempts.clear()
    assert rot.handle_msg(fmt_msg(50, 2, 10, "oi2")) == ("ACK_50", False)
    assert attempts == [5003]

    # Remetente 1 reenvia msg_id=50 -> agora já está em processed, responde sem encaminhar
    attempts.clear()
    assert rot.handle_msg(fmt_msg(50, 1, 10, "oi")) == ("ACK_50", False)
    assert attempts == []


def test_agente_msg_id_monotonico(tmp_path):
    logger = MetricsLogger("servidor", 0, log_dir=tmp_path)
    st = Estado(my_id=0)
    ag = AgenteUsuario(user_id=1, state=st, logger=logger)
    ids = [ag.next_msg_id() for _ in range(50)]
    assert ids == sorted(set(ids))
    assert all(len(str(i)) >= 16 for i in ids)