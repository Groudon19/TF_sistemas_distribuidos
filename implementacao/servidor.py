import socket
import traceback
import config
import logs
import mensagens        

class Servidor:
    def __init__(self, id: int, ip: str, port: int, usuarios: dict = None):
        self.id = id
        self.ip = ip
        self.port = port
        self.usuarios: dict[int, str | None] = {} if usuarios is None else usuarios
        
        logs.init_log(config.BASE_DIR / "logs" / f"servidor_{self.id}.jsonl", "Servidor", self.id)
        
    def adicionar_usuario(self, new_user: mensagens.NewUser):
        user_id, nome = new_user.user_id, new_user.nome
        
        if user_id in self.usuarios:
            if nome == self.usuarios[user_id]:
                logs.log_event("newuser_duplicado", user_id=user_id, nome=nome)
                return f"ACK_NEWUSER_{user_id}"
            logs.log_event("usuario_rejeitado", user_id=user_id, nome=nome, motivo="USER_EXISTS", nome_atual=self.usuarios[user_id])
            return str(mensagens.Error("USER_EXISTS"))
        
        if len(self.usuarios) >= config.MAX_USERS:
            logs.log_event("usuario_rejeitado", user_id=user_id, nome=nome, motivo="SERVER_FULL", total=len(self.usuarios))
            return str(mensagens.Error("SERVER_FULL"))
        
        self.usuarios[user_id] = nome
        logs.log_event("usuario_adicionado", user_id=user_id, nome=nome, total=len(self.usuarios))
        return f"ACK_NEWUSER_{user_id}"
        
    def responder(self, mensagem: mensagens.Mensagem) -> str:
        match mensagem:
            case mensagens.NewUser(user_id, nome):
                logs.log_event("usuario_recebido", user_id=user_id, nome=nome)
                return self.adicionar_usuario(mensagem)
            case mensagens.Kill(server_id):
                logs.log_event("kill_recebido", server_id=server_id)
                return f"ACK_KILL_{server_id}"
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
        
    def iniciar(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            srv.bind(("0.0.0.0", self.port))
            srv.listen()
            while True:
                conn, addr = srv.accept()
                self.atender(conn, addr)
                        

    
if __name__ == "__main__":
    servidor = Servidor(0, config.SERVER_IP, config.SERVER_PORT)
    servidor.iniciar()