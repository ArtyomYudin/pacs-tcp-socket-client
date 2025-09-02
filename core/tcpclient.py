import asyncio
import ssl
from utils.logger import get_logger

class TcpClient:
    """
    Асинхронный TCP клиент с поддержкой TLS.

    Позволяет подключаться к удалённому серверу,
    отправлять и получать данные по защищённому соединению.
    """
    def __init__(self, host: str, port: int, server_cert: str, server_key: str, logger=None, use_ssl: bool = True):
        """
        Инициализация клиента.

        :param host: IP или домен сервера
        :param port: TCP порт
        :param server_cert: путь к сертификату сервера
        :param server_key: путь к приватному ключу
        :param logger: объект логгера (если None → создаётся дефолтный)
        :param use_ssl: использовать ли SSL/TLS
        """
        self.host = host
        self.port = port
        self.server_cert = server_cert
        self.server_key = server_key
        self.use_ssl = use_ssl

        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.logger = logger or get_logger("tcp_client")

    async def __aenter__(self):
        """Поддержка `async with TcpClient()`"""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Закрытие соединения при выходе из `async with`"""
        await self.close()

    async def connect(self, retries: int = 5, delay: int = 5, timeout: float = 10.0):
        """
        Установить соединение с сервером.

        :param retries: количество попыток подключения
        :param delay: задержка между попытками
        :param timeout: таймаут подключения (сек)
        :raises ConnectionError: если подключиться не удалось
        """
        # context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        # context.options &= ~ssl.OP_NO_TLSv1_3 & ~ssl.OP_NO_TLSv1_2 & ~ssl.OP_NO_TLSv1_1
        # context.verify_mode = ssl.CERT_REQUIRED
        # context.minimum_version = ssl.TLSVersion.TLSv1
        # context.set_ciphers('DEFAULT@SECLEVEL=0')
        # context.load_verify_locations(self.server_cert)
        # context.load_cert_chain(certfile=self.server_cert, keyfile=self.server_key)
        # context.check_hostname = False

        ssl_context = None
        if self.use_ssl:
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            # ssl_context.check_hostname = True
            ssl_context.verify_mode = ssl.CERT_REQUIRED
            ssl_context.minimum_version = ssl.TLSVersion.TLSv1
            ssl_context.set_ciphers("DEFAULT@SECLEVEL=0")
            ssl_context.load_verify_locations(self.server_cert)
            ssl_context.load_cert_chain(certfile=self.server_cert, keyfile=self.server_key)

        for attempt in range(retries):
            try:
                self.reader, self.writer = await asyncio.wait_for(
                    asyncio.open_connection(self.host, self.port, ssl=ssl_context, server_hostname='SKD'),
                    timeout=timeout
                )
                self.logger.info(f"Подключено к {self.host}:{self.port}")
                return
            except Exception as e:
                self.logger.warning(f"Попытка подключения {attempt + 1}/{retries} не удалась: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(delay)

            raise ConnectionError(f"Не удалось подключиться к {self.host}:{self.port} после {retries} попыток")


    async def send(self, data: bytes):
        """
        Отправить данные серверу.

        :param data: байты для отправки
        :raises ConnectionError: если соединение не установлено
        """
        if not self.writer:
            raise ConnectionError("Не подключено")
        self.writer.write(data)
        await self.writer.drain()
        self.logger.info(f"Отправлено {len(data)} байт")

    async def receive_exactly(self, n):
        """
        Получить ровно N байт от сервера.

        :param n: количество байт
        :return: байтовая строка
        :raises ConnectionError: если соединение разорвано
        """

        if not self.reader:
            raise ConnectionError("Не подключено")

        data = b''
        while len(data) < n:
            chunk = await self.reader.read(n - len(data))
            if not chunk:
                raise ConnectionError("Соединение закрыто")
            data += chunk
        self.logger.debug(f"Получено {len(data)} байт")
        return data

    async def close(self):
        """
        Закрыть соединение.
        """
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
            self.logger.info("TCP соединение закрыто")
            self.writer = None
            self.reader = None
