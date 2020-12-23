import asyncio
import copy
import logging
from typing import List

import sqlalchemy as sa
from asyncpg.connection import Connection
from asyncpg.pool import Pool


logger = logging.getLogger(__name__)


class Batcher:

    def __init__(self, pool: Pool, settings: dict, table: sa.Table) -> None:
        self._valid_batch: List[dict] = []
        self._pool = pool
        self.settings = settings
        self.table = table

        self.processed_items = 0
        self.inserted_records = 0

    async def work(self) -> None:
        while True:
            await self.proccess_batch()
            await asyncio.sleep(self.settings['BATCHER_SLEEP_TIME'])

    async def proccess_batch(self) -> None:
        if self._valid_batch:
            valid_patients_list = copy.deepcopy(self._valid_batch)
            del self._valid_batch[:]

            query = (
                self.table.insert()
                .values(valid_patients_list)
            )
            async with self._pool.acquire() as conn:
                res = await conn.execute(query)

            res_split = res.split()
            real_insert_count = res_split[2]
            self.inserted_records += int(real_insert_count)
            logger.debug(
                "%s records in this batch, total: %s",
                real_insert_count, self.inserted_records,
            )

    async def process(self, conn: Connection, item: str) -> None:
        self.processed_items += 1

    def get_stats(self) -> dict:
        return {
            "processed_items": self.processed_items,
            "inserted_records": self.inserted_records,
        }
