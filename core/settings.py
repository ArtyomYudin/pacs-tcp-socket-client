import json
import os
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

load_dotenv()

class Settings(BaseSettings):
    # Debug mode
    DEBUG_MODE:bool = bool(os.getenv("DEBUG_MODE", "False").lower())

    # RabbitMQ
    RMQ_HOST: str = os.getenv("RMQ_HOST", "rabbitmq")
    RMQ_PORT:int = int(os.getenv("RMQ_PORT", 5672))
    RMQ_VIRTUAL_HOST: str  = os.getenv("RMQ_VIRTUAL_HOST", "/")
    RMQ_USER: str  = os.getenv("RMQ_USER", "guest")
    RMQ_PASSWORD: str  = os.getenv("RMQ_PASSWORD", "guest")
    RMQ_EVENTS_EXCHANGE_NAME: str  = os.getenv("RMQ_EVENTS_EXCHANGE_NAME", "pacs_client")
    RMQ_COMMANDS_EXCHANGE_NAME: str  = os.getenv("RMQ_COMMANDS_EXCHANGE_NAME", "pacs_client")

    # Celery
    CELERY_BROKER_URL: str = os.getenv("CELERY_BROKER_URL", "")
    CELERY_RESULT_BACKEND: str = os.getenv("CELERY_RESULT_BACKEND", "pc://")

    # TCP сервер
    TCP_SERVER_HOST: str  = os.getenv("TCP_SERVER_HOST", "localhost")
    TCP_SERVER_PORT:int = int(os.getenv("TCP_SERVER_PORT", 9000))
    TCP_SERVER_CERT: str  = os.getenv("TCP_SERVER_CERT", "certs/cert.pem")
    TCP_SERVER_KEY: str  = os.getenv("TCP_SERVER_KEY", "certs/key.pem")
    TCP_SERVER_CERT_CN: str  = os.getenv("TCP_SERVER_CERT_CN", "SKD")

    # Postgres
    DATABASE_NAME: str = os.getenv("DATABASE_NAME", "postgres")
    DATABASE_USER: str = os.getenv("DATABASE_USER", "postgres")
    DATABASE_PASSWORD: str = os.getenv("DATABASE_PASSWORD", "postgres")
    DATABASE_HOST: str = os.getenv("DATABASE_USER", "localhost")
    DATABASE_PORT: int = int(os.getenv("DATABASE_PORT", 5432))

    # Константы конфигурации Реверс 8000
    REVERS_TEMPLATE_ID: int = int(os.getenv("REVERS_TEMPLATE_ID", 16))
    REVERS_ACTION_ISSUE: int = int(os.getenv("REVERS_ACTION_ISSUE", 1))
    REVERS_ACTION_WITHDRAW : int= int(os.getenv("REVERS_ACTION_WITHDRAW", 0))
    REVERS_DATA_ID: int = int(os.getenv("REVERS_DATA_ID", 293))
    REVERS_VERSION: int = int(os.getenv("REVERS_VERSION", 1))

    # Команды Реверс 8000
    FILTER_EVENTS_CMD: str  = json.dumps({"Command": "filterevents", "Id": 1, "Version": 1, "Filter": 1})
    PING_CMD: str  = json.dumps({"Command": "ping", "Id": 1, "Version": 1})
    USERLIST_CMD: str  = json.dumps({"Command": "userlist", "Id": 1, "Version": 1})
    APLIST_CMD: str  = json.dumps({"Command": "aplist", "Id": 1, "Version": 1})


    class Config:
        env_file = ".env"

settings = Settings()
