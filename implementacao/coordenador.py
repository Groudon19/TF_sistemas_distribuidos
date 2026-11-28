import threading
import logs
import mensagens

class Coordenador:
    def __init__(self, servidor):
        # Recebe a instância do servidor
        self.servidor = servidor
        self.lock = threading.Lock()
        # tabela dos usuarios
        self.servidores: dict[int, tuple[str, int, str]] = {
            servidor.id: (servidor.ip, servidor.port, "ativo")
        }
        
    def registrar_servidor(self, msg:mensagens.Register) -> str:
        with self.lock:
            estado_atual = self.servidores.get(msg.server_id)
            if estado_atual is not None and estado_atual[2] == "ativo":
                estado = "ativo"
            else:
                estado = "reserva"
                
            self.servidores[msg.server_id] = (msg.ip, msg.port, estado)
            total = len(self.servidores)
            
        logs.log_event(
            "servidor_registrado",
            servidor_id = msg.server_id,
            ip = msg.ip,
            porta = msg.port,
            estado = estado,
            total_servidores = total,
        )
        
        print(f"[coordenador] Servidor {msg.server_id} registrado ({msg.ip}:{msg.port}) como '{estado}'")
        return str(mensagens.Ack(f"REGISTER_{msg.server_id}"))
    
    def listar_servidores(self) -> dict[int, tuple[str, int, str]]:
        with self.lock:
            return dict(self.servidores)
        
    def tratar_mensagem(self, mensagem: mensagens.Mensagem) -> str | None:
        """
        Trata mensagens exclusivas do papel de coordenador.
        Retorna a resposta ou None se a mensagem deve ser tratada pela camada comum do servidor.
        """
        match mensagem:
            case mensagens.NewUser(user_id, nome):
                logs.log_event("usuario_recebido", user_id=user_id, nome=nome)
                return self.servidor.adicionar_usuario(mensagem)

            case mensagens.Kill(server_id):
                logs.log_event("kill_recebido", server_id=server_id)
                return str(mensagens.Ack(f"KILL_{server_id}"))

            case mensagens.Register():
                return self.registrar_servidor(mensagem)

            case _:
                return None