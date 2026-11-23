import time
from sd.common.config import CADASTRO_IP, CONTROL_PORT, INTERNAL_TIMEOUT_SEC
from sd.common.logger import MetricsLogger
from sd.common.net import send_and_recv
from sd.common.protocolo import fmt_newcoordinatorip, fmt_newid

_RETRIES = 3
_RETRY_DELAY = 0.5


def _send_control(
    msg: str,
    expected_ack: str,
    logger: MetricsLogger,
    ok_metric: str,
    fail_metric: str,
    **extra,
) -> bool:
    t0 = time.monotonic()
    last_err = ""
    for attempt in range(1, _RETRIES + 1):
        try:
            reply = send_and_recv(CADASTRO_IP, CONTROL_PORT, msg, timeout=INTERNAL_TIMEOUT_SEC)
            if reply == expected_ack:
                latency = round(time.monotonic() - t0, 4)
                logger.log(ok_metric, valor=latency, tentativas=attempt, **extra)
                return True
            last_err = f"resposta inesperada: {reply!r}"
        except OSError as exc:
            last_err = str(exc)

        if attempt < _RETRIES:
            time.sleep(_RETRY_DELAY)

    latency = round(time.monotonic() - t0, 4)
    logger.log(fail_metric, valor=latency, tentativas=_RETRIES, erro=last_err, **extra)
    return False


def send_newid(server_id: int, logger: MetricsLogger) -> bool:
    """Informa NEWID_<id> ao Cadastro sem nunca levantar exceção."""
    try:
        return _send_control(
            fmt_newid(server_id),
            f"ACK_NEWID_{server_id}",
            logger,
            ok_metric="newid_enviado",
            fail_metric="newid_falhou",
            servidor=server_id,
        )
    except Exception as exc:
        logger.log("newid_falhou", valor=0.0, tentativas=0, erro=str(exc), servidor=server_id)
        return False


def send_newcoordinatorip(ip: str, port: int, logger: MetricsLogger) -> bool:
    """Informa NEWCOORDINATORIP_<ip>_<port> ao Cadastro (usado na fase 7)."""
    try:
        return _send_control(
            fmt_newcoordinatorip(ip, port),
            f"ACK_NEWCOORDINATORIP_{ip}_{port}",
            logger,
            ok_metric="newcoordinatorip_enviado",
            fail_metric="newcoordinatorip_falhou",
            ip=ip,
            porta=port,
        )
    except Exception as exc:
        logger.log("newcoordinatorip_falhou", valor=0.0, tentativas=0, erro=str(exc))
        return False