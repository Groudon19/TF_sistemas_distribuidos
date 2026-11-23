import argparse
import signal
import socket
import socketserver
import sys
import threading

from sd.common import config
from sd.common.logger import MetricsLogger
from sd.common.net import recv_line, send_line
from sd.common.protocolo import fmt_error
from sd.coordenador import Coordenador
from sd.estado import Estado

IDLE_CONN_TIMEOUT_SEC = 30.0


class NodeTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True
    request_queue_size = 128

    def __init__(
        self,
        server_address: tuple[str, int],
        handler_cls,
        coordinator: Coordenador,
        logger: MetricsLogger,
        shutdown_event: threading.Event,
    ):
        super().__init__(server_address, handler_cls)
        self.coordinator = coordinator
        self.logger = logger
        self.shutdown_event = shutdown_event


class ClientConnectionHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        srv: NodeTCPServer = self.server  # type: ignore[assignment]
        peer = f"{self.client_address[0]}:{self.client_address[1]}"
        self.request.settimeout(IDLE_CONN_TIMEOUT_SEC)

        while not srv.shutdown_event.is_set():
            try:
                line = recv_line(self.request)
            except ValueError:
                srv.logger.log("erro_rede", valor=1, origem=peer, motivo="LINE_TOO_LONG")
                try:
                    send_line(self.request, fmt_error("LINE_TOO_LONG"))
                except OSError:
                    pass
                break
            except (socket.timeout, ConnectionResetError, BrokenPipeError, OSError) as exc:
                srv.logger.log("conexao_encerrada", valor=1, origem=peer, motivo=type(exc).__name__)
                break

            if line is None:
                break

            if not line.strip():
                continue

            reply, should_shutdown = srv.coordinator.handle_line(line)

            try:
                send_line(self.request, reply)
            except UnicodeEncodeError:
                srv.logger.log("erro_rede", valor=1, origem=peer, motivo="ENCODING")
                try:
                    send_line(self.request, fmt_error("ENCODING"))
                except OSError:
                    pass
                break
            except (socket.timeout, ConnectionResetError, BrokenPipeError, OSError) as exc:
                srv.logger.log("conexao_encerrada", valor=1, origem=peer, motivo=type(exc).__name__)
                break

            if should_shutdown:
                srv.logger.log("servidor_encerrando", valor=1, motivo="KILL")
                srv.shutdown_event.set()
                break


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Servidor do Sistema Distribuído")
    parser.add_argument("--id", type=int, required=True, help="Identificador inteiro >= 0 do servidor")
    parser.add_argument(
        "--virtual-servers",
        type=int,
        default=0,
        help="Quantidade de servidores simulados (apenas para teste da fase 3)",
    )
    args = parser.parse_args()
    if args.id < 0:
        parser.error("--id deve ser >= 0")
    if args.virtual_servers < 0:
        parser.error("--virtual-servers deve ser >= 0")
    return args


def main() -> None:
    args = parse_args()
    node_id: int = args.id
    port = config.port_for_server(node_id)
    is_coord = node_id == config.INITIAL_SERVER_ID

    logger = MetricsLogger("coordenador" if is_coord else "servidor", node_id)
    state = Estado(my_id=node_id, is_coordinator=is_coord)
    state.register_server(node_id, ip=config.SERVER_IP, port=port)

    if args.virtual_servers > 0:
        print(f"[aviso] Registrando {args.virtual_servers} servidor(es) virtual(is) para teste da fase 3.")
        for i in range(args.virtual_servers + 1):
            if i != node_id:
                state.register_server(i, ip=config.SERVER_IP, port=config.port_for_server(i))

    coordinator = Coordenador(state=state, logger=logger)
    shutdown_event = threading.Event()

    def _signal_handler(signum, _frame):
        logger.log("sinal_recebido", valor=signum)
        shutdown_event.set()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    try:
        tcp_server = NodeTCPServer(
            (config.BIND_IP, port),
            ClientConnectionHandler,
            coordinator=coordinator,
            logger=logger,
            shutdown_event=shutdown_event,
        )
    except OSError as exc:
        sys.exit(f"Erro ao abrir socket em {config.BIND_IP}:{port}: {exc}")

    server_thread = threading.Thread(
        target=lambda: tcp_server.serve_forever(poll_interval=0.1),
        daemon=True,
    )
    server_thread.start()

    role = "COORDENADOR" if is_coord else "SERVIDOR"
    print(f"[ok] Servidor {node_id} ({role}) escutando em {config.BIND_IP}:{port} (anunciado: {config.SERVER_IP}:{port})")
    logger.log("servidor_iniciado", valor=1, porta=port, papel=role)

    try:
        while not shutdown_event.is_set():
            shutdown_event.wait(timeout=0.5)
    finally:
        tcp_server.shutdown()
        tcp_server.server_close()
        logger.log("servidor_finalizado", valor=1)
        print(f"[ok] Servidor {node_id} encerrado.")

    sys.exit(0)


if __name__ == "__main__":
    main()