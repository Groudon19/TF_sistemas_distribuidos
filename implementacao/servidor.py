import argparse
import socket
import traceback
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
        
        self.coordenador_addr = (config.SERVER_IP, config.SERVER_PORT)
        self.papel_coordenador: coordenador.Coordenador | None = None
        
        logs.init_log(config.BASE_DIR / "logs" / f"servidor_{self.id}.jsonl", "Servidor", self.id)
        
        if self.id == config.INITIAL_SERVER_ID:
            self.assumir_coordenacao()
    
    
    def assumir_coordenacao(self):
        self.papel_coordenador = coordenador.Coordenador(self)
        logs.log_event("coordenador_assumido", ip=self.ip, porta=self.port)
        print(f"[ok] Servidor {self.id} atuando como COORDENADOR em {self.ip}:{self.port}")
        
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
            total = len(self.usuarios)
            
        logs.log_event("usuario_adicionado", user_id=user_id, nome=nome, total=len(self.usuarios))
        return f"ACK_NEWUSER_{user_id}"
        
    def responder(self, mensagem: mensagens.Mensagem) -> str:
        if self.papel_coordenador is not None:
            resposta = self.papel_coordenador.tratar_mensagem(mensagem)
            if resposta is not None:
                return resposta

        match mensagem:
            case mensagens.NewUser():
                return self.adicionar_usuario(mensagem)
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
        
    def atender(self, conn: socket.socket, addr: tuple):
        with conn:
            try:
                arquivo = conn.makefile("rb")
                for linha in arquivo:
                    texto = linha.decode(config.ENCODING, errors="replace").strip()
                    if not texto:
                        continue
                    try:
                        resposta = self.processar(texto)
                    except Exception as e:
                        logs.log_event("erro_interno", linha=texto, erro=f"{type(e).__name__}: {e}")
                        traceback.print_exec()
                        resposta = str(mensagens.Error("INTERNAL"))
                    conn.sendall((resposta + "\n").encode(config.ENCODING))
            except OSError as e:
                logs.log_event("erro_conexao", origem=f"{addr[0]}:{addr[1]}", erro=str(e))
    
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