import asyncio
import json
import os
import signal
import sys

from dotenv import load_dotenv
from core.db import DB
from core.tcpclient import TcpClient
from core.rabbitmq import RabbitMQProducer
from utils.logger import get_logger

from utils.functions import (
    create_buffer,
    insert_event_to_db,
    chunk_data_async,
    load_system_ap,
    load_system_card_owner
)

# Загружаем переменные из .env
load_dotenv()
logger = get_logger("tcp_client")

# Читаем настройки из .env
# RabbitMQ
RMQ_HOST = os.getenv("RMQ_HOST", "rabbitmq")
RMQ_PORT = int(os.getenv("RMQ_PORT", 5672))
RMQ_VIRTUAL_HOST = os.getenv("RMQ_VIRTUAL_HOST", "/")
RMQ_USER = os.getenv("RMQ_USER", "guest")
RMQ_PASSWORD = os.getenv("RMQ_PASSWORD", "guest")
RMQ_QUEUE_NAME = os.getenv("RMQ_QUEUE_NAME", "pacs_client")

# TCP сервер
TCP_SERVER_HOST = os.getenv("TCP_SERVER_HOST", "localhost")
TCP_SERVER_PORT = int(os.getenv("TCP_SERVER_PORT", 9000))
TCP_SERVER_CERT = os.getenv("TCP_SERVER_CERT", "certs/cert.pem")
TCP_SERVER_KEY = os.getenv("TCP_SERVER_KEY", "certs/key.pem")

# Postgres
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_NAME = os.getenv("POSTGRES_DB", "pacs")
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")

# Команды
FILTER_EVENTS_CMD = json.dumps({"Command": "filterevents", "Id": 1, "Version": 1, "Filter": 1})
PING_CMD = json.dumps({"Command": "ping", "Id": 1, "Version": 1})
USERLIST_CMD = json.dumps({"Command": "userlist", "Id": 1, "Version": 1})
APLIST_CMD = json.dumps({"Command": "aplist", "Id": 1, "Version": 1})


# глобальное событие остановки
shutdown_event = asyncio.Event()

producer_instance: RabbitMQProducer | None = None


def handle_exit(signum, frame):
    """Обработчик сигналов завершения (graceful shutdown)."""
    logger.info("Получен сигнал отключения...")
    shutdown_event.set()
    if producer_instance:
        producer_instance.close()  # жёстко рвём соединение

async def receive_data(client: TcpClient, db: DB, producer: RabbitMQProducer):
    """
    Основной цикл приёма данных от PACS.

    :param client: экземпляр TcpClient
    :param db: подключение к Postgres
    :param producer: продюсер сообщений RabbitMQ
    """
    """Основной цикл приёма данных от PACS"""
    await client.send(create_buffer(FILTER_EVENTS_CMD))
    await client.send(create_buffer(APLIST_CMD))
    await client.send(create_buffer(USERLIST_CMD))

    while not shutdown_event.is_set():
        try:
            raw_data = await chunk_data_async(client)
            if not raw_data or len(raw_data) < 4:
                logger.warning("Получены пустые или недействительные данные")
                continue # просто проверили таймаут → снова в цикл

            payload = raw_data[4:]
            try:
                received = json.loads(payload.decode('utf-8'))
            except json.JSONDecodeError as e:
                logger.error(f"Получен недопустимый JSON: {e}, данные: {payload[:200]}...")
                continue

            command = received.get("Command")
            data = received.get("Data")

            match command:
                case "ping":
                    await client.send(create_buffer(PING_CMD))
                    logger.debug(f"RECEIVED: {received}")

                case "events":
                    logger.debug(f"RECEIVED: {received}")
                    if isinstance(data, list):
                        event_ids = await insert_event_to_db(db, data)
                        for eid in event_ids:
                            await producer.publish(RMQ_QUEUE_NAME, eid)
                    else:
                        logger.warning(f"Ожидался список в 'events.Data', получен {type(data)}: {data}")

                case "userlist":
                    if isinstance(data, list):
                        await load_system_card_owner(db, data)
                    else:
                        logger.warning(f"Ожидался список в 'userlist.Data', получен {type(data)}: {data}")

                case "aplist":
                    if isinstance(data, list):
                        await load_system_ap(db, data)
                    else:
                        logger.warning(f"Ожидался список в 'aplist.Data', получен {type(data)}: {data}")

                case _:
                    logger.warning(f"Неизвестная или отсутствующая команда: {received}")

        except Exception as e:
            logger.error(f"Ошибка в receive_data: {e}")
            await asyncio.sleep(1)  # чтобы не зациклиться


async def main():
    """
    Точка входа в приложение.

    Инициализация БД, RabbitMQ и TCP клиента.
    Запуск цикла обработки данных.
    """

    db = DB(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, database=DB_NAME)
    await db.connect()

    # global producer_instance
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(shutdown()))
    loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.create_task(shutdown()))

    async def shutdown():
        logger.info("Получен сигнал отключения...")
        shutdown_event.set()
        
    try:
        async with RabbitMQProducer(
                host=RMQ_HOST,
                port=RMQ_PORT,
                virtual_host=RMQ_VIRTUAL_HOST,
                username=RMQ_USER,
                password=RMQ_PASSWORD
        ) as producer, TcpClient(
            host=TCP_SERVER_HOST,
            port=TCP_SERVER_PORT,
            server_cert=TCP_SERVER_CERT,
            server_key=TCP_SERVER_KEY,
            logger=logger,
        ) as client:
            logger.info("Клиент PACS TCP запущен")
            await receive_data(client, db, producer)
    except Exception as e:
        logger.error(f"TCP соединение не удалось: {e}")
        sys.exit(1)
    finally:
        logger.info("Закрытие соединений...")
        await db.close()
        logger.info("DB закрыта")

if __name__ == "__main__":

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
         handle_exit(None, None)