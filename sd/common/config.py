import os
from dotenv import load_dotenv




def carrega_env():
    """
    Carrega as variáveis de ambiente do arquivo .env
    """
    

    # Carrega o arquivo .env
    load_dotenv()

    # Retorna as variáveis de ambiente como um dicionário
    return {key: os.getenv(key) for key in os.environ.keys()}
    