import threading
import subprocess
import sys

from pathlib import Path
import config
import logs
import mensagens
import rede

class Coordenador:
    def __init__(self, servidor):
        # Recebe a instância do servidor
        self.servidor = servidor
        self.lock = threading.Lock()
        # tabela dos usuarios
        self.servidores: dict[int, tuple[str, int, str]] = {
            servidor.id: (servidor.ip, servidor.port, "ativo")
        }
        
        # Contagem de usuários alocados/reservados por servidor ativo: server_id -> qtd
        self.ocupacao: dict[int, int] = {servidor.id: 0}
        # Mapa global de usuários já confirmados: user_id -> server_id
        self.usuarios_alocados: dict[int, int] = {}
        
        self._eventos_registro: dict[int, threading.Event] = {}
        self._processos_filhos: list[subprocess.Popen] = []
        
        
    def registrar_servidor(self, msg:mensagens.Register) -> str:
        with self.lock:
            estado_atual = self.servidores.get(msg.server_id)
            if estado_atual is not None and estado_atual[2] in ("ativo", "ativando"):
                estado = estado_atual[2]
            else:
                estado = "reserva"
                
            self.servidores[msg.server_id] = (msg.ip, msg.port, estado)
            self.ocupacao.setdefault(msg.server_id, 0)
            total = len(self.servidores)
            
        logs.log_event(
            "servidor_registrado",
            servidor_id = msg.server_id,
            ip = msg.ip,
            porta = msg.port,
            estado = estado,
            total_servidores = total,
        )
        
        with self.lock:
            evt = self._eventos_registro.pop(msg.server_id, None)
        if evt is not None:
            evt.set()
        
        print(f"[coordenador] Servidor {msg.server_id} registrado ({msg.ip}:{msg.port}) como '{estado}'")
        return str(mensagens.Ack(f"REGISTER_{msg.server_id}"))
    
    def listar_servidores(self) -> dict[int, tuple[str, int, str]]:
        with self.lock:
            return dict(self.servidores)
        
    def _enviar_para_servidor(self, server_id: int, ip: str, porta: int, mensagem: mensagens.Mensagem) -> tuple[bool, str]:
        """Entrega a mensagem localmente (se server_id == self.servidor.id) ou via TCP."""
        if server_id == self.servidor.id:
            resp = self.servidor.adicionar_usuario(mensagem) if isinstance(mensagem, mensagens.NewUser) else self.servidor.responder(mensagem)
            return (not resp.startswith("ERROR_")), resp
        return rede.enviar(ip, porta, str(mensagem))    
    
    
    def _escolher_servidor_para_novo_usuario(self) -> tuple[str, int | None, tuple[str, int] | None]:
        """
        Regra determinística: menor ID ativo com ocupacao < MAX_USERS.
        Se todos os ativos estiverem cheios, reserva o servidor 'reserva' de menor ID
        marcando-o como 'ativando'.

        Retorna:
          - ("ativo", server_id, (ip, porta)) -> vaga já reservada em servidor ativo
          - ("ativar", server_id, (ip, porta)) -> reserva escolhido; chamador deve notificar Cadastro
          - ("cheio", None, None) -> nenhum servidor ativo com vaga nem reserva disponível
        """
        with self.lock:
            ativos_ordenados = sorted(
                sid for sid, (_, _, estado) in self.servidores.items() if estado == "ativo"
            )
            for sid in ativos_ordenados:
                if self.ocupacao.get(sid, 0) < config.MAX_USERS:
                    self.ocupacao[sid] = self.ocupacao.get(sid, 0) + 1
                    ip, porta, _ = self.servidores[sid]
                    return "ativo", sid, (ip, porta)

            reservas_ordenados = sorted(
                sid for sid, (_, _, estado) in self.servidores.items() if estado == "reserva"
            )
            if reservas_ordenados:
                sid = reservas_ordenados[0]
                ip, porta, _ = self.servidores[sid]
                self.servidores[sid] = (ip, porta, "ativando")
                return "ativar", sid, (ip, porta)

            return "criar", None, None
    
    def _criar_servidor_sob_demanda(self) -> tuple[bool, int | None, tuple[str, int] | None]:
        """Lança um novo processo servidor.py localmente e aguarda seu REGISTER."""
        with self.lock:
            novo_id = max(self.servidores.keys(), default=self.servidor.id) + 1
            portas_em_uso = {porta for _, porta, _ in self.servidores.values()}
            nova_porta = self.servidor.port + novo_id
            while nova_porta in portas_em_uso:
                nova_porta += 1

            novo_ip = self.servidor.ip
            # Pré-reserva o ID como 'ativando' para nenhuma outra thread duplicar
            self.servidores[novo_id] = (novo_ip, nova_porta, "ativando")
            self.ocupacao[novo_id] = 0
            evt = threading.Event()
            self._eventos_registro[novo_id] = evt

        script_servidor = Path(__file__).resolve().parent / "servidor.py"
        cmd = [
            sys.executable,
            str(script_servidor),
            "--id", str(novo_id),
            "--ip", novo_ip,
            "--porta", str(nova_porta),
        ]

        try:
            proc = subprocess.Popen(cmd)
            with self.lock:
                self._processos_filhos.append(proc)
        except OSError as exc:
            with self.lock:
                self.servidores.pop(novo_id, None)
                self.ocupacao.pop(novo_id, None)
                self._eventos_registro.pop(novo_id, None)
            logs.log_event("falha_criacao_servidor", servidor_id=novo_id, erro=str(exc))
            return False, None, None

        logs.log_event("servidor_criado_sob_demanda", servidor_id=novo_id, ip=novo_ip, porta=nova_porta, pid=proc.pid)
        print(f"[coordenador] Criando servidor {novo_id} sob demanda em {novo_ip}:{nova_porta} (PID {proc.pid})...")

        if not evt.wait(timeout=5.0):
            with self.lock:
                self.servidores.pop(novo_id, None)
                self.ocupacao.pop(novo_id, None)
                self._eventos_registro.pop(novo_id, None)
            logs.log_event("timeout_registro_sob_demanda", servidor_id=novo_id)
            return False, None, None

        return True, novo_id, (novo_ip, nova_porta)

    def encerrar_filhos(self):
        with self.lock:
            filhos = list(self._processos_filhos)
        for proc in filhos:
            if proc.poll() is None:
                proc.terminate()
    
    
    def _ativar_servidor_reserva(self, server_id: int, ip: str, porta: int) -> bool:
        """Notifica o Cadastro com NEWID_<id> e efetiva a transição 'ativando' -> 'ativo'."""
        msg_newid = str(mensagens.NewId(server_id))
        ok, info = rede.enviar(config.CADASTRO_IP, config.CONTROL_PORT, msg_newid)

        with self.lock:
            if not ok:
                # Rollback do estado intermediário
                if server_id in self.servidores and self.servidores[server_id][2] == "ativando":
                    self.servidores[server_id] = (ip, porta, "reserva")
                logs.log_event(
                    "falha_ativacao_servidor",
                    servidor_id=server_id,
                    cadastro=f"{config.CADASTRO_IP}:{config.CONTROL_PORT}",
                    erro=info,
                )
                print(f"[coordenador] Falha ao ativar servidor {server_id} no Cadastro: {info}")
                return False

            self.servidores[server_id] = (ip, porta, "ativo")
            self.ocupacao[server_id] = 1  # Já reserva a 1ª vaga para o usuário que disparou o scale-up
            ativos_total = sum(1 for _, _, st in self.servidores.values() if st == "ativo")

        logs.log_event(
            "servidor_ativado",
            servidor_id=server_id,
            ip=ip,
            porta=porta,
            ativos_total=ativos_total,
        )
        print(f"[coordenador] Servidor {server_id} ATIVADO ({ip}:{porta}) | Total ativos: {ativos_total}")
        return True
    
    
    def _liberar_vaga(self, server_id: int):
        """Rollback da reserva de vaga caso o encaminhamento do NEWUSER falhe."""
        with self.lock:
            atual = self.ocupacao.get(server_id, 0)
            if atual > 0:
                self.ocupacao[server_id] = atual - 1
        
    def tratar_mensagem(self, mensagem: mensagens.Mensagem) -> str | None:
        """
        Trata mensagens exclusivas do papel de coordenador.
        Retorna a resposta ou None se a mensagem deve ser tratada pela camada comum do servidor.
        """
        match mensagem:
            case mensagens.NewUser(user_id, nome):
                logs.log_event("usuario_recebido", user_id=user_id, nome=nome)
                return self.processar_novo_usuario(mensagem)

            case mensagens.Kill(server_id):
                logs.log_event("kill_recebido", server_id=server_id)
                return str(mensagens.Ack(f"KILL_{server_id}"))

            case mensagens.Register():
                return self.registrar_servidor(mensagem)

            case _:
                return None
            
    def processar_novo_usuario(self, new_user: mensagens.NewUser) -> str:
        user_id = new_user.user_id

        # 1. Idempotência: usuário já alocado anteriormente
        with self.lock:
            servidor_existente = self.usuarios_alocados.get(user_id)
            dados_existente = self.servidores.get(servidor_existente) if servidor_existente is not None else None

        if servidor_existente is not None and dados_existente is not None:
            ip_dest, porta_dest, _ = dados_existente
            ok, resp = self._enviar_para_servidor(servidor_existente, ip_dest, porta_dest, new_user)
            return resp if resp.startswith(("ACK_", "ERROR_")) else str(mensagens.Error("INTERNAL"))

        # 2. Alocação determinística ou scale-up
        acao, server_id, addr = self._escolher_servidor_para_novo_usuario()

        if acao == "criar":
            ok_criacao, server_id, addr = self._criar_servidor_sob_demanda()
            if not ok_criacao or server_id is None or addr is None:
                logs.log_event("usuario_rejeitado_cluster_cheio", user_id=user_id, nome=new_user.nome)
                return str(mensagens.Error("SERVER_FULL"))
            ip_dest, porta_dest = addr
            if not self._ativar_servidor_reserva(server_id, ip_dest, porta_dest):
                return str(mensagens.Error("CADASTRO_UNREACHABLE"))
        else:
            if acao == "cheio" or server_id is None or addr is None:
                logs.log_event("usuario_rejeitado_cluster_cheio", user_id=user_id, nome=new_user.nome)
                return str(mensagens.Error("SERVER_FULL"))
            ip_dest, porta_dest = addr
            if acao == "ativar":
                if not self._ativar_servidor_reserva(server_id, ip_dest, porta_dest):
                    return str(mensagens.Error("CADASTRO_UNREACHABLE"))
        # 3. Envia o usuário ao servidor responsável
        ok, resp = self._enviar_para_servidor(server_id, ip_dest, porta_dest, new_user)
        if not ok:
            self._liberar_vaga(server_id)
            logs.log_event(
                "falha_roteamento_usuario",
                user_id=user_id,
                servidor_destino=server_id,
                erro=resp,
            )
            return resp if resp.startswith("ERROR_") else str(mensagens.Error("SERVER_UNREACHABLE"))

        # 4. Consolida o mapeamento definitivo
        with self.lock:
            self.usuarios_alocados[user_id] = server_id

        logs.log_event(
            "usuario_roteado",
            user_id=user_id,
            nome=new_user.nome,
            servidor_destino=server_id,
        )
        return resp