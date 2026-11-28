import re
import config

NEWUSER_RE = re.compile(r"NEWUSER_(\d{1,18})(?:_(.*))?", re.ASCII | re.DOTALL)
KILL_RE = re.compile(r"KILL_(\d{1,18})", re.ASCII)
NEWID_RE = re.compile(r"NEWID_(\d{1,18})", re.ASCII)
NEWCOORDINATORIP_RE = re.compile(r"NEWCOORDINATORIP_([A-Za-z0-9.:\-]+)_(\d{1,5})", re.ASCII)
MSG_RE = re.compile(r"MSG_(\d{1,18})_(\d{1,18})_(\d{1,18})_(.+)", re.ASCII)
ACK_RE = re.compile(r"ACK_([A-Z0-9_]+)", re.ASCII)
ERROR_RE = re.compile(r"ERROR_([A-Z0-9_]+)", re.ASCII)
REGISTER_RE = re.compile(r"REGISTER_(\d{1,18})_([A-Za-z0-9.:\-]+)_(\d{1,5})", re.ASCII) 
USERS_RE = re.compile(r"USERS_(\d{1,18}(?:,\d{1,18})*)", re.ASCII)  

class Mensagem():
    pass

class NewUser(Mensagem):
    __match_args__ = ("user_id", "nome")
    
    def __init__(self, user_id: int, nome: str | None = None):
        self.user_id = user_id
        self.nome = nome
        
    def set_nome(self, nome: str):
        self.nome = nome

    def __str__(self):
        if self.nome is None:
            return f"NEWUSER_{self.user_id}"
        else:
            return f"NEWUSER_{self.user_id}_{self.nome}"

class Kill(Mensagem):
    __match_args__ = ("server_id",)

    def __init__(self, server_id: int):
        self.server_id = server_id
        
    def __str__(self):
        return f"KILL_{self.server_id}"
        
class NewId(Mensagem):
    __match_args__ = ("server_id",)
    
    def __init__(self, server_id: int):
        self.server_id = server_id
        
    def __str__(self):
        return f"NEWID_{self.server_id}"

class NewCoordinatorIp(Mensagem):
    __match_args__ = ("ip", "port")
    
    def __init__(self, ip: str, port: int):
        self.ip = ip
        self.port = port
        
    def __str__(self):
        return f"NEWCOORDINATORIP_{self.ip}_{self.port}"
    
class Register(Mensagem):
    __match_args__ = ("server_id", "ip", "port")
    
    def __init__(self, server_id: int, ip: str, port: int):
        self.server_id = server_id
        self.ip = ip
        self.port = port
        
    def __str__(self):
        return f"REGISTER_{self.server_id}_{self.ip}_{self.port}"
    
class UserList(Mensagem):
    __match_args__ = ("user_ids",)
    
    def __init__(self, user_ids: list[int]):
        self.user_ids = list(user_ids)
        
    def __str__(self):
        ids_str = ",".join(str(uid) for uid in self.user_ids)
        return f"USERS_{ids_str}"

       
class Msg(Mensagem):
    __match_args__ = ("msg_id", "sender_id", "dest_id", "conteudo")
    
    def __init__(self, msg_id: int, sender_id: int, dest_id: int, conteudo: str):
        self.msg_id = msg_id
        self.sender_id = sender_id
        self.dest_id = dest_id
        self.conteudo = conteudo
        
    def __str__(self):
        return f"MSG_{self.msg_id}_{self.sender_id}_{self.dest_id}_{self.conteudo}"
    
class Ack(Mensagem):
    __match_args__ = ("referencia",)
    
    def __init__(self, referencia: int | str):
        self.referencia = referencia
        
    def __str__(self):
        return f"ACK_{self.referencia}"

class Error(Mensagem):
    __match_args__ = ("descricao",)
    
    def __init__(self, descricao: str):
        self.descricao = descricao
    
    def __str__(self):
        return f"ERROR_{self.descricao}"
        
class InvalidMessage(ValueError): 
    pass

def sanitize_name(name: str) -> str:
    """Troca "_" e caracteres de controle (inclusive quebras de linha) por espaço."""
    name = "".join(ch if ch.isprintable() else " " for ch in name.replace("_", " "))
    return " ".join(name.split())

def sanitize_content(content:str) -> str:
    """Remove quebras de linha e caracteres de controle do conteúdo da mensagem."""
    cleaned = "".join(ch if ch.isprintable() else " " for ch in content)
    return " ".join(cleaned.split())
    
def normalize_message(raw: str) -> Mensagem:
    """
    Valida e normaliza uma mensagem NEWUSER/KILL/NEWID/NEWCOORDINATORIP/MSG/ACK/ERROR.
    Retorna uma Mensagem ou lança InvalidMessage.
    """
    text = raw.strip()

    m = KILL_RE.fullmatch(text)
    if m:
        server_id = int(m.group(1))
        return Kill(server_id)

    m = NEWUSER_RE.fullmatch(text)
    if m:
        new_user = NewUser(int(m.group(1)))
        if new_user.user_id <= 0:
            raise InvalidMessage("O ID do usuário deve ser maior que zero")
        if m.group(2) is None:
            return new_user
        name = sanitize_name(m.group(2))
        if not name:
            raise InvalidMessage("O nome não pode ficar vazio (ou envie NEWUSER_<ID>)")
        if len(name) > config.MAX_NAME_LEN:
            raise InvalidMessage(f"O nome pode ter no máximo {config.MAX_NAME_LEN} caracteres")
        new_user.set_nome(name)
        try:
            str(new_user).encode(config.ENCODING)
        except UnicodeEncodeError:
            raise InvalidMessage(f"O nome tem caracteres que não existem em {config.ENCODING}")
        return new_user

    m = NEWID_RE.fullmatch(text)
    if m:
        server_id = int(m.group(1))
        return NewId(server_id)
    
    m = NEWCOORDINATORIP_RE.fullmatch(text)
    if m:
        ip = m.group(1)
        port = int(m.group(2))
        if not (1 <= port <= 65535):
            raise InvalidMessage(f"A porta deve estar entre 1 e 65535 (recebido: {port})")
        
        return NewCoordinatorIp(ip, port)
    
    m = REGISTER_RE.fullmatch(text)
    if m:
        server_id = int(m.group(1))
        ip = m.group(2)
        port = int(m.group(3))
        if not (1 <= port <= 65535):
            raise InvalidMessage(f"A porta deve estar entre 1 e 65535 (recebido: {port})")
        
        return Register(server_id, ip, port)
    
    m = USERS_RE.fullmatch(text)
    if m:
        user_ids = [int(x) for x in m.group(1).split(",")]
        if any(uid <= 0 for uid in user_ids):
            raise InvalidMessage("Todos os IDs de usuário devem ser maiores que zero")
        return UserList(user_ids)
    
    m = MSG_RE.fullmatch(text)
    if m:
        msg_id, sender_id, dest_id = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if msg_id <= 0 or sender_id <= 0 or dest_id <= 0:
            raise InvalidMessage("Os IDs da mensagem, remetente e destinatário devem ser maiores que zero")
        conteudo = sanitize_content(m.group(4))
        if not conteudo:
            raise InvalidMessage("O conteúdo da mensagem não pode ficar vazio")
        try:
            conteudo.encode(config.ENCODING)
        except UnicodeEncodeError:
            raise InvalidMessage(f"O conteúdo tem caracteres que não existem em {config.ENCODING}")
        return Msg(msg_id, sender_id, dest_id, conteudo)
    
    m = ACK_RE.fullmatch(text)
    if m:
        return Ack(m.group(1))
    
    m = ERROR_RE.fullmatch(text)
    if m:
        descricao = m.group(1)
        
        #TODO: fazer validacões de erros
        
        return Error(descricao)
        

    raise InvalidMessage(
        "Formato inválido. Use NEWUSER_<ID>[_<NOME>], KILL_<ID>, NEWID_<ID>, "
        "NEWCOORDINATORIP_<IP>_<PORT>, REGISTER_<ID>_<IP>_<PORT>, USERS_<IDS>, "
        "MSG_<MSG_ID>_<SENDER_ID>_<DEST_ID>_<CONTEUDO>, ACK_<REF> ou ERROR_<DESCRICAO>"
    )