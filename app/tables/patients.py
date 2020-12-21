import asyncio
import json
import logging
from typing import List

import sqlalchemy as sa
from asyncpg.connection import Connection
from asyncpg.pool import Pool
from sqlalchemy.dialects import postgresql

from .db import metadata


logger = logging.getLogger(__name__)


patients_table = sa.Table(
    'patients', metadata,
    sa.Column('id', postgresql.INTEGER, primary_key=True),
    sa.Column('source_id', postgresql.TEXT, nullable=False),
    sa.Column('birth_date', postgresql.TIMESTAMP),
    sa.Column('gender', postgresql.TEXT),
    sa.Column('race_code', postgresql.TEXT),
    sa.Column('race_code_system', postgresql.TEXT),
    sa.Column('ethnicity_code', postgresql.TEXT),
    sa.Column('ethnicity_code_system', postgresql.TEXT),
    sa.Column('country', postgresql.TEXT),
)


class Batching:

    def __init__(self, pool: Pool, settings: dict) -> None:
        self._valid_patients_list: List[dict] = []
        self._pool = pool
        self.settings = settings

    async def work(self) -> None:
        while True:
            await asyncio.sleep(self.settings['BATCHER_SLEEP_TIME'])
            await self.save_batch()

    async def save_batch(self) -> None:
        if self._valid_patients_list:
            async with self._pool.acquire() as conn:
                query = (
                    patients_table.insert()
                    .values(self._valid_patients_list)
                )
                del self._valid_patients_list[:]
                await conn.execute(query)

    async def process(self, conn: Connection, item: str) -> None:
        try:
            patient = json.loads(item)
        except json.JSONDecodeError:
            logger.debug("invalid JSON")

        # country = patient.get('address', [])[0].get('country')
        # birth_date = patient.get('birthDate')
        # logger.info(birth_date)

        valid_patient = {
            'source_id': patient.get('id', 0),
            # 'birth_date': birth_date,
            'gender': patient.get('gender'),
            # 'country': country,
        }

        self._valid_patients_list.append(valid_patient)
