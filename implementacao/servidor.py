import argparse
import random
import threading
import time

import config
import coordenador
import logs
import mensagens
import rede        

class Servidor:
    def __init__(self, id: int, ip: str, port: int, usuarios: dict = None):
        self.id = id
        self.ip = ip
        self.port = port
        self.usuarios: dict[int, str | None] = {} if usuarios is None else usuarios
        
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        
        # Troca de mensagens
        self.usuarios_conhecidos: set[int] = set(self.usuarios.keys())
        self._threads_usuarios: dict[int, threading.Thread] = {}
        self.mensagens_recebidas: set[int] = set()
        self._seq_msg = 0
        
        
        self.coordenador_addr = (config.SERVER_IP, config.SERVER_PORT)
        self.papel_coordenador: coordenador.Coordenador | None = None
        
        logs.init_log(config.BASE_DIR / "logs" / f"servidor_{self.id}.jsonl", "Servidor", self.id)
        
        if self.id == config.INITIAL_SERVER_ID:
            self.assumir_coordenacao()

        for uid in list(self.usuarios.keys()):
            self._iniciar_thread_usuario(uid)
    
    def assumir_coordenacao(self):
        self.papel_coordenador = coordenador.Coordenador(self)
        logs.log_event("coordenador_assumido", ip=self.ip, porta=self.port)
        print(f"[ok] Servidor {self.id} atuando como COORDENADOR em {self.ip}:{self.port}")
   
    # Definição do envio de mensagens
    def atualizar_usuarios_conhecidos(self, user_ids: list[int]):
        with self.lock:
            self.usuarios_conhecidos.update(user_ids)

    def _proximo_msg_id(self, sender_id: int) -> int:
        with self.lock:
            self._seq_msg += 1
            return sender_id * 1_000_000 + self._seq_msg
    
    def _iniciar_thread_usuario(self, user_id: int):
        with self.lock:
            if user_id in self._threads_usuarios:
                return
            t = threading.Thread(
                target=self._loop_usuario,
                args=(user_id,),
                name=f"user-{user_id}-sender",
                daemon=True,
            )
            self._threads_usuarios[user_id] = t
        t.start()
    
    def _loop_usuario(self, user_id: int):
        """Envia MSG periódicas em intervalos aleatórios de até MAX_MSG_INTERVAL para outros usuários."""
        while not self.stop_event.is_set():
            intervalo = random.uniform(0.5, config.MAX_MSG_INTERVAL)
            if self.stop_event.wait(intervalo):
                break

            with self.lock:
                if user_id not in self.usuarios:
                    break
                candidatos = [uid for uid in self.usuarios_conhecidos if uid != user_id]

            if not candidatos:
                continue

            dest_id = random.choice(candidatos)
            msg_id = self._proximo_msg_id(user_id)
            conteudo = f"Ola do usuario {user_id} para {dest_id} ({msg_id})"
            msg = mensagens.Msg(msg_id, user_id, dest_id, conteudo)

            coord_ip, coord_port = self.coordenador_addr
            logs.log_event("msg_enviada",msg_id=msg_id,sender_id=user_id,dest_id=dest_id,conteudo=conteudo,)

            ok, resp = rede.enviar(coord_ip,coord_port,str(msg),timeout=config.ACK_TIMEOUT,tentativas=config.MAX_ACK_RETRIES,)

            if ok:
                logs.log_event("ack_recebido",msg_id=msg_id,sender_id=user_id,dest_id=dest_id,resposta=resp,)
            else:
                logs.log_event("falha_envio_msg",msg_id=msg_id,sender_id=user_id,dest_id=dest_id,erro=resp,)
    
    def receive_msg(self, msg: mensagens.Msg) -> bool:
        """Simula o processamento da mensagem no servidor do destinatário."""
        time.sleep(config.RECEIVE_MSG_DELAY)
        return True
    
    def receber_mensagem(self, msg: mensagens.Msg) -> str:
        with self.lock:
            if msg.dest_id not in self.usuarios:
                logs.log_event("msg_rejeitada_usuario_inexistente",msg_id=msg.msg_id,sender_id=msg.sender_id,dest_id=msg.dest_id,)
                return str(mensagens.Error("USER_NOT_FOUND"))

            if msg.msg_id in self.mensagens_recebidas:
                logs.log_event("msg_duplicada",msg_id=msg.msg_id,sender_id=msg.sender_id,dest_id=msg.dest_id,)
                return str(mensagens.Ack(msg.msg_id))

        logs.log_event("msg_recebida",msg_id=msg.msg_id,sender_id=msg.sender_id,dest_id=msg.dest_id,conteudo=msg.conteudo,)

        if self.receive_msg(msg):
            with self.lock:
                self.mensagens_recebidas.add(msg.msg_id)
            logs.log_event("ack_enviado",msg_id=msg.msg_id,sender_id=msg.sender_id,dest_id=msg.dest_id,)
            return str(mensagens.Ack(msg.msg_id))

        return str(mensagens.Error("RECEIVE_FAILED"))
    
        
    def adicionar_usuario(self, new_user: mensagens.NewUser):
        user_id, nome = new_user.user_id, new_user.nome
        
        with self.lock:
            if user_id in self.usuarios:
                if nome == self.usuarios[user_id]:
                    logs.log_event("newuser_duplicado", user_id=user_id, nome=nome)
                    return str(mensagens.Ack(f"NEWUSER_{user_id}"))
                logs.log_event("usuario_rejeitado", user_id=user_id, nome=nome, motivo="USER_EXISTS", nome_atual=self.usuarios[user_id])
                return str(mensagens.Error("USER_EXISTS"))
            
            if len(self.usuarios) >= config.MAX_USERS:
                logs.log_event("usuario_rejeitado", user_id=user_id, nome=nome, motivo="SERVER_FULL", total=len(self.usuarios))
                return str(mensagens.Error("SERVER_FULL"))
            
            self.usuarios[user_id] = nome
            self.usuarios_conhecidos.add(user_id)
            total = len(self.usuarios)
            
        logs.log_event("usuario_adicionado", user_id=user_id, nome=nome, total=len(self.usuarios))
        self._iniciar_thread_usuario(user_id)
        return str(mensagens.Ack(f"ACK_NEWUSER_{user_id}"))
        
    def responder(self, mensagem: mensagens.Mensagem) -> str:
        if self.papel_coordenador is not None:
            resposta = self.papel_coordenador.tratar_mensagem(mensagem)
            if resposta is not None:
                return resposta

        match mensagem:
            case mensagens.NewUser():
                return self.adicionar_usuario(mensagem)
            case mensagens.Msg():
                return self.receber_mensagem(mensagem)
            case mensagens.UserList(user_ids):
                self.atualizar_usuarios_conhecidos(user_ids)
                return str(mensagens.Ack("USERS"))
            case _:
                logs.log_event("mensagem_inesperada", mensagem=str(mensagem))
                return str(mensagens.Error("UNEXPECTED_MESSAGE"))
            
    def processar(self, texto: str) -> str:
        try:
            mensagem = mensagens.normalize_message(texto)
        except mensagens.InvalidMessage as e:
            logs.log_event("mensagem_invalida", linha=texto, erro=str(e))
            return str(mensagens.Error("INVALID_COMMAND"))
        return self.responder(mensagem)
            
    def registrar_no_coordenador(self):
        """Envia REGISTER_<ID>_<IP>_<PORTA> ao coordenador inicial após abrir o socket."""
        time.sleep(0.2)
        coord_ip, coord_port = self.coordenador_addr
        msg = str(mensagens.Register(self.id, self.ip, self.port))
        ok, info = rede.enviar(coord_ip, coord_port, msg)
        if ok:
            logs.log_event("registrado_no_coordenador", coordenador=f"{coord_ip}:{coord_port}", resposta=info)
            print(f"[ok] Servidor {self.id} registrado no coordenador {coord_ip}:{coord_port} ({info})")
        else:
            logs.log_event("falha_registro_coordenador", coordenador=f"{coord_ip}:{coord_port}", erro=info)
            print(f"[erro] Falha ao registrar no coordenador {coord_ip}:{coord_port}: {info}")
    
        
    def iniciar(self):
        if self.papel_coordenador is None:
            threading.Thread(target=self.registrar_no_coordenador, daemon=True).start()

        try:
            rede.servir(self.port, self.processar, self.stop_event)
        except KeyboardInterrupt:
            self.stop_event.set()
            if self.papel_coordenador is not None:
                self.papel_coordenador.encerrar_filhos()
            print(f"\n[ok] Servidor {self.id} encerrado.")
                        
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Servidor do Sistema Distribuído")
    parser.add_argument("--id", type=int, required=True, help="ID numérico do servidor (ex: 0, 1, 2)")
    parser.add_argument("--ip", type=str, default=config.SERVER_IP, help="IP anunciado aos demais nós")
    parser.add_argument("--porta", type=int, default=config.SERVER_PORT, help="Porta TCP de escuta")
    args = parser.parse_args()

    if args.id < 0:
        parser.error("--id deve ser >= 0")
    if not (1 <= args.porta <= 65535):
        parser.error("--porta deve estar entre 1 e 65535")

    servidor = Servidor(args.id, args.ip, args.porta)
    servidor.iniciar()