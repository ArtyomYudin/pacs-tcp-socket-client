import asyncio
import aio_pika
import json
from aio_pika import RobustConnection, RobustChannel, Message, DeliveryMode
from aio_pika.exceptions import AMQPConnectionError
from typing import Dict

from utils.logger import get_logger


class RabbitMQProducer:
    """
    Асинхронный продюсер RabbitMQ для публикации сообщений.

    Использует aio-pika (поддержка reconnect).
    """

    def __init__(self, host: str, port: int, virtual_host: str, username: str, password: str, logger=None):
        self.host = host
        self.port = port
        self.virtual_host = virtual_host
        self.username = username
        self.password = password
        self.logger = logger or get_logger("rabbitmq")

        self.connection: RobustConnection | None = None
        self.channel: RobustChannel | None = None

    async def __aenter__(self):
        """Вход в асинхронный контекстный менеджер."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Выход из асинхронного контекстного менеджера."""
        await self.close()

    async def connect(self, retries: int = 5, delay: int = 5):
        """
        Подключение к RabbitMQ с повторными попытками.

        :param retries: количество попыток
        :param delay: задержка между попытками
        """
        for attempt in range(1, retries + 1):
            try:
                self.connection = await aio_pika.connect_robust(
                    host=self.host,
                    port=self.port,
                    login=self.username,
                    password=self.password,
                    virtualhost=self.virtual_host,
                    timeout=10
                )
                self.channel = await self.connection.channel()
                self.logger.info(f"Подключение к RabbitMQ {self.host}:{self.port}/{self.virtual_host}")
                return
            except AMQPConnectionError as e:
                self.logger.warning(f"Попытка подключения {attempt}/{retries} не удалась: {e}")
                await asyncio.sleep(delay)

        raise ConnectionError(f"Не удалось подключиться к RabbitMQ по адресу {self.host}:{self.port}")

    async def publish(self, queue_name: str, message: Dict[str, any], max_retries: int = 3):
        """
        Асинхронная отправка сообщения в очередь с ограниченным числом попыток.

        :param queue_name: имя очереди
        :param message: тело сообщения (должно быть сериализуемо в JSON)
        :param max_retries: максимальное количество попыток отправки (по умолчанию 3)
        """
        for attempt in range(1, max_retries + 1):
            if not self.channel or self.channel.is_closed:
                self.logger.warning(
                    f"[Попытка {attempt}/{max_retries}] Нет активного подключения RabbitMQ. Повторное подключение...")
                try:
                    await self.connect()
                except Exception as connect_error:
                    self.logger.error(f"Не удалось восстановить соединение: {connect_error}")
                    if attempt == max_retries:
                        raise ConnectionError(
                            f"Не удалось подключиться к RabbitMQ после {max_retries} попыток") from connect_error
                    await asyncio.sleep(1)  # небольшая пауза перед повтором
                    continue

            try:
                # Сериализуем сообщение в JSON и кодируем в байты
                body = json.dumps(message, ensure_ascii=False).encode('utf-8')

                queue = await self.channel.declare_queue(queue_name, durable=True)
                await self.channel.default_exchange.publish(
                    Message(
                        body=body,
                        delivery_mode=DeliveryMode.PERSISTENT
                    ),
                    routing_key=queue.name
                )
                self.logger.info(f"Опубликовано сообщение в очередь '{queue_name}' (попытка {attempt})")
                return  # Успешно — выходим из функции

            except Exception as e:
                self.logger.error(f"[Попытка {attempt}/{max_retries}] Ошибка публикации в очередь '{queue_name}': {e}")

                # Если это последняя попытка — пробрасываем исключение
                if attempt == max_retries:
                    raise RuntimeError(
                        f"Не удалось опубликовать сообщение в очередь '{queue_name}' после {max_retries} попыток") from e

                # Перед следующей попыткой — ждём немного
                await asyncio.sleep(2 ** attempt)  # экспоненциальная задержка: 2, 4, 8 сек...

            # Теоретически сюда не должно дойти, но на всякий случай
        raise RuntimeError("Не удалось опубликовать сообщение — все попытки исчерпаны")

    async def close(self):
        """Закрытие соединения."""
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
            self.logger.info("Соединение RabbitMQ закрыто")