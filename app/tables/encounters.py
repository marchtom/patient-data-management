import datetime
import json
import logging
from typing import Any, List, Optional, Tuple

import sqlalchemy as sa
from aiocache import cached
from asyncpg import Record
from asyncpg.connection import Connection
from asyncpg.pool import Pool
from sqlalchemy import func, text
from sqlalchemy.dialects import postgresql

from app.settings import settings

from .basic_batcher import Batcher
from .db import metadata

from . import patients


logger = logging.getLogger(__name__)


encounters_table = sa.Table(
    'encounters', metadata,
    sa.Column('id', postgresql.INTEGER, primary_key=True),
    sa.Column('source_id', postgresql.TEXT, nullable=False),
    sa.Column(
        'patient_id',
        postgresql.INTEGER,
        sa.ForeignKey('patients.id'),
        nullable=False,
    ),
    sa.Column('start_date', postgresql.TIMESTAMP(timezone=True), nullable=False),
    sa.Column('end_date', postgresql.TIMESTAMP(timezone=True), nullable=False),
    sa.Column('type_code', postgresql.TEXT),
    sa.Column('type_code_system', postgresql.TEXT),
)


async def get_encounter_id(conn: Connection, source_id: str) -> str:
    query = (
        encounters_table.select()
        .where(encounters_table.c.source_id == source_id)
        .with_only_columns([encounters_table.c.id])
    )
    return await conn.fetchval(query)


async def popular_start_encounters_days(pool: Pool) -> dict:
    result_raw = await popular_encounters_days(pool, encounters_table.c.start_date)

    result_list = [dict(row) for row in result_raw]
    result_dict = {
        row.get("weekday", "").strip(): row.get("count") for row in result_list
    }

    return result_dict


async def popular_end_encounters_days(pool: Pool) -> dict:
    result_raw = await popular_encounters_days(pool, encounters_table.c.end_date)

    result_list = [dict(row) for row in result_raw]
    result_dict = {
        row.get("weekday", "").strip(): row.get("count") for row in result_list
    }

    return result_dict


async def popular_encounters_days(pool: Pool, column: sa.Column) -> List[Record]:
    query = (
        encounters_table.select()
        .with_only_columns([
            func.to_char(column, 'Day').label('weekday'),
            func.count(encounters_table.c.id).label('count'),
        ])
        .group_by(text('weekday'))
        .order_by(text('count DESC'))
    )

    async with pool.acquire() as conn:
        return await conn.fetch(query)


class EncountersBatching(Batcher):

    def __init__(self, pool: Pool, settings: dict) -> None:
        super().__init__(pool, settings, encounters_table)

    @staticmethod
    def _find_code(type_: List[Any]) -> Tuple[Optional[str], Optional[str]]:
        if not type_:
            return None, None

        try:
            coding = type_[0].get("coding")
            code = coding[0].get("code")
            system = coding[0].get("system")
            return code, system
        except (AttributeError, IndexError):
            return None, None

        return None, None

    # TODO: Move cache to redis or any database. In memory cache is not shared between queue workers.
    @cached(ttl=settings['CACHE_TTL'])  # type: ignore
    async def get_patient_id(self, patient_id_reference: str) -> str:
        async with self._pool.acquire() as conn:
            return await patients.get_patient_id(conn, patient_id_reference)

    async def process(self, conn: Connection, item: str) -> None:
        await super().process(conn, item)

        # skip items that are not valid JSON
        try:
            encounter = json.loads(item)
        except json.JSONDecodeError:
            logger.info("invalid JSON")
            return

        # source_id is required
        if (source_id := encounter.get('id')) is None:
            return

        # patient_id is required
        if (subject := encounter.get("subject")) is None:
            return
        else:
            if (patient_id_reference := subject.get("reference")) is None:
                return
            else:
                patient_id_reference = patient_id_reference.replace("Patient/", "")

        if not (patient_id := await self.get_patient_id(patient_id_reference)):
            return

        # start_date and end_date are required
        if (period := encounter.get("period")) is None:
            return
        else:
            start_date_str = period.get("start")
            end_date_str = period.get("end")
            if start_date_str and end_date_str:
                try:
                    start_date = datetime.datetime.fromisoformat(start_date_str)
                    end_date = datetime.datetime.fromisoformat(end_date_str)
                except (TypeError, ValueError):
                    return
            else:
                return

        # type_code and type_code_system are optional
        type_code, type_code_system = EncountersBatching._find_code(encounter.get("type"))

        valid_encounter = {
            'source_id': str(source_id),
            'patient_id': patient_id,
            'start_date': start_date,
            'end_date': end_date,
            'type_code': type_code,
            'type_code_system': type_code_system,
        }

        self._valid_batch.append(valid_encounter)
