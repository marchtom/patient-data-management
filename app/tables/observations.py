import json
import logging
import datetime
from typing import Optional, Tuple

import sqlalchemy as sa
from aiocache import cached
from asyncpg.connection import Connection
from asyncpg.pool import Pool
from sqlalchemy.dialects import postgresql

from app.settings import settings

from .basic_batcher import Batcher
from .db import metadata

from . import encounters, patients


logger = logging.getLogger(__name__)


observations_table = sa.Table(
    'observations', metadata,
    sa.Column('id', postgresql.INTEGER, primary_key=True),
    sa.Column('source_id', postgresql.TEXT, nullable=False),
    sa.Column(
        'patient_id',
        postgresql.INTEGER,
        sa.ForeignKey('patients.id'),
        nullable=False,
    ),
    sa.Column(
        'encounter_id',
        postgresql.INTEGER,
        sa.ForeignKey('encounters.id'),
    ),
    sa.Column('observation_date', postgresql.DATE, nullable=False),
    sa.Column('type_code', postgresql.TEXT, nullable=False),
    sa.Column('type_code_system', postgresql.TEXT, nullable=False),
    sa.Column('value', postgresql.NUMERIC, nullable=False),
    sa.Column('unit_code', postgresql.TEXT, nullable=False),
    sa.Column('unit_code_system', postgresql.TEXT, nullable=False),
)


class ObservationsBatching(Batcher):

    def __init__(self, pool: Pool, settings: dict) -> None:
        super().__init__(pool, settings, observations_table)

    @staticmethod
    def _find_code(code_: Optional[dict]) -> Tuple[Optional[str], Optional[str]]:
        if not code_:
            return None, None

        try:
            coding = code_.get("coding")
            code = coding[0].get("code")  # type: ignore
            system = coding[0].get("system")  # type: ignore
            return code, system
        except (AttributeError, IndexError):
            return None, None

        return None, None

    # TODO: Move cache to redis or any database. In memory cache is not shared between queue workers.
    @cached(ttl=settings['CACHE_TTL'])  # type: ignore
    async def get_patient_id(self, patient_id_reference: str) -> str:
        async with self._pool.acquire() as conn:
            return await patients.get_patient_id(conn, patient_id_reference)

    @cached(ttl=settings['CACHE_TTL'])  # type: ignore
    async def get_encounter_id(self, encounter_id_reference: str) -> str:
        async with self._pool.acquire() as conn:
            return await encounters.get_encounter_id(conn, encounter_id_reference)

    async def process(self, conn: Connection, item: str) -> None:
        await super().process(conn, item)

        # skip items that are not valid JSON
        try:
            observation = json.loads(item)
        except json.JSONDecodeError:
            logger.info("invalid JSON")
            return

        # source_id is required
        if (source_id := observation.get('id')) is None:
            return

        # patient_id is required
        if (subject := observation.get("subject")) is None:
            return
        else:
            if (patient_id_reference := subject.get("reference")) is None:
                return
            else:
                patient_id_reference = patient_id_reference.replace("Patient/", "")

        if not (patient_id := await self.get_patient_id(patient_id_reference)):
            return

        # encounter_id is optional
        encounter_id = None
        if (subject := observation.get("context")) is not None:
            if (encounter_id_reference := subject.get("reference")) is not None:
                encounter_id_reference = encounter_id_reference.replace("Encounter/", "")
                encounter_id = await self.get_encounter_id(encounter_id_reference)

        # observation_date is required
        if (observation_date_raw := observation.get("effectiveDateTime")) is None:
            return

        try:
            observation_date = datetime.datetime.fromisoformat(observation_date_raw)
        except ValueError:
            return

        data = []

        # type_code and type_code_system are required
        type_code, type_code_system = ObservationsBatching._find_code(observation.get("code"))
        if type_code is not None and type_code_system is not None:
            if (value_quantity := observation.get("valueQuantity")) is None:
                return
            value = value_quantity.get("value")
            unit_code = value_quantity.get("unit")
            unit_code_system = value_quantity.get("system")
            data.append({
                'type_code': type_code,
                'type_code_system': type_code_system,
                'value': value,
                'unit_code': unit_code,
                'unit_code_system': unit_code_system,
            })
        else:
            if not (components := observation.get("component")):
                return
            else:
                for comp in components:
                    type_code, type_code_system = ObservationsBatching._find_code(comp.get("code"))
                    if (value_quantity := comp.get("valueQuantity")) is None:
                        continue
                    value = value_quantity.get("value")
                    unit_code = value_quantity.get("unit")
                    unit_code_system = value_quantity.get("system")
                    data.append({
                        'type_code': type_code,
                        'type_code_system': type_code_system,
                        'value': value,
                        'unit_code': unit_code,
                        'unit_code_system': unit_code_system,
                    })

        for row in data:
            valid_encounter = {
                'source_id': str(source_id),
                'patient_id': patient_id,
                'encounter_id': encounter_id,
                'observation_date': observation_date,
                'type_code': row.get("type_code"),
                'type_code_system': row.get("type_code_system"),
                'value': row.get("value"),
                'unit_code': row.get("unit_code"),
                'unit_code_system': row.get("unit_code_system"),
            }

            self._valid_batch.append(valid_encounter)
