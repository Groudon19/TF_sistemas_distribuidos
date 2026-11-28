import subprocess
import sys
import threading
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
        # Tabela: id -> (ip, porta, estado)
        self.servidores: dict[int, tuple[str, int, str]] = {
            servidor.id: (servidor.ip, servidor.port, "ativo")
        }

        # Contagem de usuários alocados/reservados por servidor ativo: server_id -> qtd
        self.ocupacao: dict[int, int] = {servidor.id: 0}
        # Mapa global de usuários já confirmados: user_id -> server_id
        self.usuarios_alocados: dict[int, int] = {}
        # Nomes cadastrados dos usuários para permitir migração completa: user_id -> nome
        self.nomes_usuarios: dict[int, str | None] = {}

        self._eventos_registro: dict[int, threading.Event] = {}
        self._processos_filhos: list[subprocess.Popen] = []

    def registrar_servidor(self, msg: mensagens.Register) -> str:
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
            servidor_id=msg.server_id,
            ip=msg.ip,
            porta=msg.port,
            estado=estado,
            total_servidores=total,
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
            if isinstance(mensagem, mensagens.NewUser):
                resp = self.servidor.adicionar_usuario(mensagem)
            elif isinstance(mensagem, mensagens.Msg):
                resp = self.servidor.receber_mensagem(mensagem)
            else:
                resp = self.servidor.responder(mensagem)
            return (not resp.startswith("ERROR_")), resp
        return rede.enviar(ip, porta, str(mensagem))

    def _difundir_lista_usuarios(self):
        """Envia USERS_<id1>,<id2>,... aos servidores ativos para que conheçam os destinatários válidos."""
        with self.lock:
            todos_ids = sorted(self.usuarios_alocados.keys())
            destinos = [
                (sid, ip, porta)
                for sid, (ip, porta, estado) in self.servidores.items()
                if estado == "ativo" and sid != self.servidor.id
            ]

        if not todos_ids:
            return

        self.servidor.atualizar_usuarios_conhecidos(todos_ids)
        if not destinos:
            return

        payload = str(mensagens.UserList(todos_ids))

        def _enviar_para_todos():
            for _, ip, porta in destinos:
                rede.enviar(ip, porta, payload, timeout=1.5, tentativas=1)

        threading.Thread(target=_enviar_para_todos, daemon=True).start()

    def _escolher_servidor_para_novo_usuario(self) -> tuple[str, int | None, tuple[str, int] | None]:
        """
        Regra determinística: menor ID ativo com ocupacao < MAX_USERS.
        Se todos os ativos estiverem cheios, reserva o servidor 'reserva' de menor ID
        marcando-o como 'ativando', ou sinaliza criação sob demanda.
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
            self.ocupacao[server_id] = 1
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

    def _alocar_e_enviar_usuario(self, new_user: mensagens.NewUser) -> tuple[bool, int | None, str]:
        """Escolhe o servidor destino (com scale-up se necessário) e envia NEWUSER."""
        user_id = new_user.user_id
        acao, server_id, addr = self._escolher_servidor_para_novo_usuario()

        if acao == "criar":
            ok_criacao, server_id, addr = self._criar_servidor_sob_demanda()
            if not ok_criacao or server_id is None or addr is None:
                logs.log_event("usuario_rejeitado_cluster_cheio", user_id=user_id, nome=new_user.nome)
                return False, None, str(mensagens.Error("SERVER_FULL"))
            ip_dest, porta_dest = addr
            if not self._ativar_servidor_reserva(server_id, ip_dest, porta_dest):
                return False, None, str(mensagens.Error("CADASTRO_UNREACHABLE"))
        else:
            if acao == "cheio" or server_id is None or addr is None:
                logs.log_event("usuario_rejeitado_cluster_cheio", user_id=user_id, nome=new_user.nome)
                return False, None, str(mensagens.Error("SERVER_FULL"))
            ip_dest, porta_dest = addr
            if acao == "ativar":
                if not self._ativar_servidor_reserva(server_id, ip_dest, porta_dest):
                    return False, None, str(mensagens.Error("CADASTRO_UNREACHABLE"))

        ok, resp = self._enviar_para_servidor(server_id, ip_dest, porta_dest, new_user)
        if not ok:
            self._liberar_vaga(server_id)
            logs.log_event(
                "falha_roteamento_usuario",
                user_id=user_id,
                servidor_destino=server_id,
                erro=resp,
            )
            erro = resp if resp.startswith("ERROR_") else str(mensagens.Error("SERVER_UNREACHABLE"))
            return False, None, erro

        return True, server_id, resp

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
        ok, server_id, resp = self._alocar_e_enviar_usuario(new_user)
        if not ok or server_id is None:
            return resp

        # 3. Consolida o mapeamento definitivo e difunde a lista de usuários conhecidos
        with self.lock:
            self.usuarios_alocados[user_id] = server_id
            self.nomes_usuarios[user_id] = new_user.nome

        logs.log_event(
            "usuario_roteado",
            user_id=user_id,
            nome=new_user.nome,
            servidor_destino=server_id,
        )
        self._difundir_lista_usuarios()
        return resp

    def _desativar_servidores_ociosos(self) -> list[int]:
        """Retorna servidores ativos sem usuários (ocupacao == 0) para o estado 'reserva'."""
        desativados: list[int] = []
        with self.lock:
            for sid, (ip, porta, estado) in list(self.servidores.items()):
                if sid == self.servidor.id:
                    continue
                if estado == "ativo" and self.ocupacao.get(sid, 0) == 0:
                    self.servidores[sid] = (ip, porta, "reserva")
                    desativados.append(sid)

        for sid in desativados:
            logs.log_event("servidor_ocioso_desativado", servidor_id=sid, novo_estado="reserva")
            print(f"[coordenador] Scale-down: Servidor {sid} ocioso voltou para 'reserva'.")

        return desativados

    def processar_kill(self, server_id: int) -> str:
        """Desliga o servidor alvo, redistribui seus usuários e desativa servidores ociosos."""
        if server_id == self.servidor.id:
            logs.log_event("kill_coordenador_bloqueado", servidor_id=server_id)
            return str(mensagens.Error("CANNOT_KILL_COORDINATOR"))

        with self.lock:
            dados = self.servidores.get(server_id)
            if dados is None:
                logs.log_event("kill_servidor_desconhecido", servidor_id=server_id)
                return str(mensagens.Error("SERVER_NOT_FOUND"))

            ip_alvo, porta_alvo, estado_atual = dados
            if estado_atual == "morto":
                logs.log_event("kill_duplicado", servidor_id=server_id)
                return str(mensagens.Ack(f"KILL_{server_id}"))

            # Marca imediatamente como morto para que nenhuma nova alocação ou MSG o escolha
            self.servidores[server_id] = (ip_alvo, porta_alvo, "morto")
            self.ocupacao[server_id] = 0

            usuarios_afetados = sorted(
                uid for uid, sid in self.usuarios_alocados.items() if sid == server_id
            )
            for uid in usuarios_afetados:
                self.usuarios_alocados.pop(uid, None)

        # 1. Envia ordem de desligamento ao servidor alvo
        ok_kill, resp_kill = rede.enviar(ip_alvo, porta_alvo, str(mensagens.Kill(server_id)))
        logs.log_event(
            "kill_enviado_servidor",
            servidor_id=server_id,
            ok=ok_kill,
            resposta=resp_kill,
            usuarios_afetados=len(usuarios_afetados),
        )
        print(
            f"[coordenador] KILL_{server_id} executado ({resp_kill}) | "
            f"Redistribuindo {len(usuarios_afetados)} usuário(s)..."
        )

        # 2. Redistribui cada usuário afetado para os servidores disponíveis (ou cria/ativa reserva)
        migrados = 0
        nao_migrados: list[int] = []
        for uid in usuarios_afetados:
            with self.lock:
                nome = self.nomes_usuarios.get(uid)
            new_user = mensagens.NewUser(uid, nome)
            ok_mig, novo_sid, resp_mig = self._alocar_e_enviar_usuario(new_user)
            if ok_mig and novo_sid is not None:
                with self.lock:
                    self.usuarios_alocados[uid] = novo_sid
                migrados += 1
                logs.log_event(
                    "usuario_migrado",
                    user_id=uid,
                    de_servidor=server_id,
                    para_servidor=novo_sid,
                )
            else:
                nao_migrados.append(uid)
                logs.log_event(
                    "falha_migracao_usuario",
                    user_id=uid,
                    de_servidor=server_id,
                    erro=resp_mig,
                )

        # 3. Scale-down: servidores ativos sem usuários voltam para 'reserva'
        desativados = self._desativar_servidores_ociosos()

        logs.log_event(
            "kill_processado",
            servidor_id=server_id,
            usuarios_migrados=migrados,
            usuarios_nao_migrados=len(nao_migrados),
            servidores_desativados=desativados,
        )
        print(
            f"[coordenador] Migração concluída para KILL_{server_id}: "
            f"{migrados}/{len(usuarios_afetados)} usuário(s) migrado(s)."
        )

        self._difundir_lista_usuarios()
        return str(mensagens.Ack(f"KILL_{server_id}"))

    def rotear_mensagem(self, msg: mensagens.Msg) -> str:
        """Consulta a tabela de alocação e encaminha MSG ao servidor do destinatário."""
        with self.lock:
            server_dest = self.usuarios_alocados.get(msg.dest_id)
            dados_dest = self.servidores.get(server_dest) if server_dest is not None else None

        if server_dest is None or dados_dest is None:
            logs.log_event(
                "msg_destino_desconhecido",
                msg_id=msg.msg_id,
                sender_id=msg.sender_id,
                dest_id=msg.dest_id,
            )
            return str(mensagens.Error("USER_NOT_FOUND"))

        ip_dest, porta_dest, estado = dados_dest
        if estado != "ativo":
            logs.log_event(
                "msg_servidor_inativo",
                msg_id=msg.msg_id,
                sender_id=msg.sender_id,
                dest_id=msg.dest_id,
                servidor_destino=server_dest,
                estado=estado,
            )
            return str(mensagens.Error("SERVER_UNAVAILABLE"))

        logs.log_event(
            "msg_roteada",
            msg_id=msg.msg_id,
            sender_id=msg.sender_id,
            dest_id=msg.dest_id,
            servidor_destino=server_dest,
        )

        if server_dest == self.servidor.id:
            return self.servidor.receber_mensagem(msg)

        ok, resp = rede.enviar(
            ip_dest,
            porta_dest,
            str(msg),
            timeout=config.ACK_TIMEOUT,
            tentativas=config.MAX_ACK_RETRIES,
        )
        if not ok:
            logs.log_event(
                "falha_entrega_msg",
                msg_id=msg.msg_id,
                sender_id=msg.sender_id,
                dest_id=msg.dest_id,
                servidor_destino=server_dest,
                erro=resp,
            )
            return resp if resp.startswith("ERROR_") else str(mensagens.Error("DELIVERY_FAILED"))

        return resp

    def tratar_mensagem(self, mensagem: mensagens.Mensagem) -> str | None:
        """
        Trata mensagens exclusivas do papel de coordenador.
        Retorna a resposta ou None se a mensagem deve ser tratada pela camada comum do servidor.
        """
        match mensagem:
            case mensagens.NewUser(user_id, nome):
                logs.log_event("usuario_recebido", user_id=user_id, nome=nome)
                return self.processar_novo_usuario(mensagem)

            case mensagens.Msg():
                return self.rotear_mensagem(mensagem)

            case mensagens.Kill(server_id):
                logs.log_event("kill_recebido", server_id=server_id)
                return self.processar_kill(server_id)

            case mensagens.Register():
                return self.registrar_servidor(mensagem)

            case _:
                return None