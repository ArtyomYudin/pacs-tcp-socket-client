import asyncio
import json
import os
import signal

from dotenv import load_dotenv
from core.settings import settings
from core.db import DB
from rabbitmq.handlers import rmq_handler
from core.tcpclient import TcpClient
from rabbitmq.consumer import RabbitMQConsumer
from rabbitmq.producer import RabbitMQProducer
from utils.logger import get_logger

from utils.functions import (
    create_buffer,
    insert_event_to_db,
    chunk_data_async,
    load_system_ap,
    load_system_card_owner
)

logger = get_logger(settings.DEBUG_MODE)

# глобальное событие остановки
# shutdown_event = asyncio.Event()

# producer_instance: RabbitMQProducer | None = None
#
#
# def handle_exit(signum, frame):
#     """Обработчик сигналов завершения (graceful shutdown)."""
#     logger.info("Получен сигнал отключения...")
#     shutdown_event.set()
#     if producer_instance:
#         producer_instance.close()  # жёстко рвём соединение

async def receive_data(client: TcpClient, db: DB, producer: RabbitMQProducer, shutdown_event):
    """
    Основной цикл приёма данных от PACS.

    :param shutdown_event:
    :param client: экземпляр TcpClient
    :param db: подключение к Postgres
    :param producer: продюсер сообщений RabbitMQ
    """
    """Основной цикл приёма данных от PACS"""
    await client.send(create_buffer(settings.FILTER_EVENTS_CMD))
    await client.send(create_buffer(settings.APLIST_CMD))
    await client.send(create_buffer(settings.USERLIST_CMD))

    while not shutdown_event.is_set():
        try:
            try:
                raw_data = await asyncio.wait_for(
                    chunk_data_async(client),
                    timeout=5
                )
            except asyncio.TimeoutError:
                continue
            # raw_data = await chunk_data_async(client)
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
                    await client.send(create_buffer(settings.PING_CMD))
                    logger.debug(f"RECEIVED: {received}")

                case "events":
                    logger.debug(f"RECEIVED: {received}")
                    if isinstance(data, list):
                        event_ids = await insert_event_to_db(db, data, logger)
                        for eid in event_ids:
                            await producer.publish(settings.RMQ_EVENTS_EXCHANGE_NAME, {"new_pacs_event_id": eid})
                    else:
                        logger.warning(f"Ожидался список в 'events.Data', получен {type(data)}: {data}")

                case "userlist":
                    if isinstance(data, list):
                        await load_system_card_owner(db, data, logger)
                    else:
                        logger.warning(f"Ожидался список в 'userlist.Data', получен {type(data)}: {data}")

                case "aplist":
                    if isinstance(data, list):
                        await load_system_ap(db, data, logger)
                    else:
                        logger.warning(f"Ожидался список в 'aplist.Data', получен {type(data)}: {data}")

                case "addcard":
                    logger.debug(f"Ответ от команды addcard: {received}")
                case "editcard":
                    logger.debug(f"Ловим ошибки на загрузке карты: {received}")
                case "loadcard":
                    logger.debug(f"Ответ от команды loadcard: {received}")
                case "delcard":
                    err = received.get("ErrCode")
                    logger.debug(f"Ответ от команды delcard: {received}")

                case _:
                    logger.warning(f"Неизвестная или отсутствующая команда: {received}")

        except Exception as e:
            if shutdown_event.is_set():
                logger.info("receive_data остановлен")
            else:
                logger.error(f"Ошибка в receive_data: {e}")
            await asyncio.sleep(1) # чтобы не зациклиться


async def main():
    """
    Точка входа в приложение.

    Инициализация БД, RabbitMQ и TCP клиента.
    Запуск цикла обработки данных.
    """
    shutdown_event = asyncio.Event()

    loop = asyncio.get_running_loop()


    def _shutdown():
        logger.info("Получен сигнал завершения, останавливаемся...")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown)

    db = DB(
        user=settings.DATABASE_USER,
        password=settings.DATABASE_PASSWORD,
        host=settings.DATABASE_HOST,
        database=settings.DATABASE_NAME,
        logger=logger)
    await db.connect()

    # # global producer_instance
    # loop = asyncio.get_event_loop()
    # loop.add_signal_handler(signal.SIGINT, lambda *args: asyncio.create_task(shutdown()))
    # loop.add_signal_handler(signal.SIGTERM, lambda *args: asyncio.create_task(shutdown()))

    # async def shutdown():
    #     logger.info("Получен сигнал отключения...")
    #     shutdown_event.set()
        
    try:
        async with (
            RabbitMQConsumer(
                host=settings.RMQ_HOST,
                port=settings.RMQ_PORT,
                virtual_host=settings.RMQ_VIRTUAL_HOST,
                username=settings.RMQ_USER,
                password=settings.RMQ_PASSWORD,
                logger=logger
            ) as consumer,
            RabbitMQProducer(
                    host=settings.RMQ_HOST,
                    port=settings.RMQ_PORT,
                    virtual_host=settings.RMQ_VIRTUAL_HOST,
                    username=settings.RMQ_USER,
                    password=settings.RMQ_PASSWORD,
                    logger=logger
            ) as producer, TcpClient(
                host=settings.TCP_SERVER_HOST,
                port=settings.TCP_SERVER_PORT,
                server_cert=settings.TCP_SERVER_CERT,
                server_key=settings.TCP_SERVER_KEY,
                server_cert_cn=settings.TCP_SERVER_CERT_CN,
                logger=logger,
            ) as client):
                logger.info("Клиент PACS TCP запущен")
                await consumer.connect()

                # Определяем обработчик внутри области видимости, чтобы захватить 'client'
                async def _rmq_handler_wrapped(message):
                    return await rmq_handler(message, client)

                # Регистрируем обработчики очередей
                # await consumer.consume("events", events_handler)
                await consumer.consume(settings.RMQ_COMMANDS_EXCHANGE_NAME, "pacs_client", _rmq_handler_wrapped)

                await receive_data(client, db, producer, shutdown_event)
    except Exception as e:
        logger.error(f"TCP соединение не удалось: {e}")
        # sys.exit(1)
    finally:
        logger.info("Закрытие соединений...")
        await db.close()
        logger.info("DB закрыта")

if __name__ == "__main__":
    asyncio.run(main())
    # try:
    #     asyncio.run(main())
    # except KeyboardInterrupt:
    #      handle_exit(None, None)