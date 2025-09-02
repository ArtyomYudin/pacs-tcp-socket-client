from asyncpg import create_pool, Pool

from utils.logger import get_logger
from celery.bin.result import result


class DB:
    """
    Асинхронный класс для работы с PostgreSQL через пул соединений.

    Использует библиотеку asyncpg и поддерживает автоматическое
    подключение при выполнении запросов.
    """
    def __init__(self, user: str, password: str, host: str, database: str, logger = None):
        """
        Инициализация параметров подключения.

        :param user: имя пользователя PostgreSQL
        :param password: пароль
        :param host: хост (например, "localhost" или "postgres")
        :param database: имя базы данных
        :param logger: внешний логгер. Если не передан, создаётся локальный.
        """
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        self.pool: Pool | None = None
        self.logger = logger or get_logger("DB")

    async def connect(self):
        """Создание пула соединений, если он ещё не создан."""
        if self.pool is None:
            try:
                self.pool = await create_pool(
                    user=self.user,
                    password=self.password,
                    database=self.database,
                    host=self.host
                )
                self.logger.info("Подключение к PostgreSQL установлено")
            except Exception as e:
                self.logger.error(f"Ошибка подключения к БД: {e}")
                raise

    async def execute(self, query, *args):
        """
        Выполняет SQL-запрос (INSERT, UPDATE, DELETE).

        :param query: SQL-запрос
        :param  args: параметры запроса
        :return: str: статус выполнения (например "INSERT 0 1")
        """
        if self.pool is None:
            await self.connect()
        try:
            async with self.pool.acquire() as conn:
                return await conn.execute(query, *args)
        except Exception as e:
            self.logger.error(f"Ошибка выполнения execute: {e}, query={query}, args={args}")
            raise

    async def fetch_row(self, query, *args):
        """
        Возвращает одну строку из БД.

        :param query: SQL-запрос
        :param args: параметры запроса
        :return: asyncpg.Record | None: одна строка или None
        """
        if self.pool is None:
            await self.connect()
        try:
            async with self.pool.acquire() as conn:
                return await conn.fetchrow(query, *args)
        except Exception as e:
            self.logger.error(f"Ошибка выполнения fetch_row: {e}, query={query}, args={args}")
            raise

    async def fetch_all(self, query: str, *args):
        """
        Возвращает все строки из БД.

        :param query: SQL-запрос
        :param  args: параметры запроса
        :return: list[asyncpg.Record]: список строк
        """
        if self.pool is None:
            await self.connect()
        try:
            async with self.pool.acquire() as conn:
                return await conn.fetch(query, *args)
        except Exception as e:
            self.logger.error(f"Ошибка выполнения fetch_all: {e}, query={query}, args={args}")
            raise

    async def close(self):
        """Закрывает пул соединений."""
        if self.pool:
            await self.pool.close()
            self.logger.info("Подключение к PostgreSQL закрыто")
