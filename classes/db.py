import asyncpg

class DB:
    def __init__(self, user, password, host, database):
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        self.connection = None

        #asyncio.run(self.connect())

    async def connect(self):
            self.connection = await asyncpg.connect(user=self.user, password=self.password,
                                                    database=self.database, host=self.host)

    async def execute(self, query, *args):
        if self.connection is None:
            self.connection = await asyncpg.connect(user=self.user, password=self.password,
                                                    database=self.database, host=self.host)
        result = await self.connection.execute(query, *args)
        return result

    async def fetch_row(self, query, *args):
        if self.connection is None:
            await self.connect()
        result = await self.connection.fetchrow(query, *args)
        return result

    async def close(self):
        if self.connection is not None:
            await self.connection.close()
