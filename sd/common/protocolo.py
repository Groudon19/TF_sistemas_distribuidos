import re
from urllib.parse import quote, unquote

class ProtcolError(Exception):
    
    def __init__(self, code:str, message:str = ""):
        self.code = code
        super().__init__(message or code)
        
        

# Regexes para manter o padrão, que nem no cadastro
_RE_NEWUSER = re.compile(r"NEWUSER_(\d{1,18})(?:_(.*))?", re.ASCII | re.DOTALL)
_RE_KILL = re.compile(r"KILL_(\d{1,18})", re.ASCII)
_RE_ID = re.compile(r"\d{1,18}", re.ASCII)

def escapa_conteudo(s: str) -> str:
    
    return quote(s, safe="")