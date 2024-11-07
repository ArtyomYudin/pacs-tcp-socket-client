import asyncpg
from celery.bin.result import result


class DB:
    def __init__(self, user, password, host, database):
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        self.pool = None

    async def connect(self):
            self.pool = await asyncpg.create_pool(user=self.user, password=self.password,
                                                    database=self.database, host=self.host)

    async def execute(self, query, *args):
        if self.pool is None:
            self.pool = await asyncpg.create_pool(user=self.user, password=self.password,
                                                    database=self.database, host=self.host)
        async with self.pool.acquire() as conn:
            return await conn.execute(query, *args)

    async def fetch_row(self, query, *args):
        if self.pool is None:
            await self.connect()
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(query, *args)

    async def close(self):
        if self.pool is not None:
            await self.pool.close()
