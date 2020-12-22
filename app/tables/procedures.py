import json
import logging
import datetime
from typing import Any, List, Optional, Tuple

import sqlalchemy as sa
from aiocache import cached
from asyncpg.connection import Connection
from asyncpg.pool import Pool
from sqlalchemy.dialects import postgresql

from .basic_batcher import Batcher
from .db import metadata

from . import encounters, patients


logger = logging.getLogger(__name__)


procedures_table = sa.Table(
    'procedures', metadata,
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
    sa.Column('procedure_date', postgresql.DATE, nullable=False),
    sa.Column('type_code', postgresql.TEXT, nullable=False),
    sa.Column('type_code_system', postgresql.TEXT, nullable=False),
)


class ProceduresBatching(Batcher):

    def __init__(self, pool: Pool, settings: dict) -> None:
        super().__init__(pool, settings, procedures_table)

    @staticmethod
    def _find_code(code_: List[Any]) -> Tuple[Optional[str], Optional[str]]:
        if not code_:
            return None, None

        try:
            coding = code_.get("coding")
            code = coding[0].get("code")
            system = coding[0].get("system")
            return code, system
        except (AttributeError, IndexError):
            return None, None

        return None, None

    # TODO: Move cache to redis or any database. In memory cache is not shared between queue workers.
    @cached(ttl=30)  # type: ignore
    async def get_patient_id(self, patient_id_reference: str) -> str:
        async with self._pool.acquire() as conn:
            return await patients.get_patient_id(conn, patient_id_reference)

    @cached(ttl=30)  # type: ignore
    async def get_encounter_id(self, encounter_id_reference: str) -> str:
        async with self._pool.acquire() as conn:
            return await encounters.get_encounter_id(conn, encounter_id_reference)

    async def process(self, conn: Connection, item: str) -> None:

        # skip items that are not valid JSON
        try:
            procedure = json.loads(item)
        except json.JSONDecodeError:
            logger.info("invalid JSON")
            return

        # source_id is required
        if (source_id := procedure.get('id')) is None:
            return

        # patient_id is required
        if (subject := procedure.get("subject")) is None:
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
        if (subject := procedure.get("context")) is not None:
            if (encounter_id_reference := subject.get("reference")) is not None:
                encounter_id_reference = encounter_id_reference.replace("Encounter/", "")
                encounter_id = await self.get_encounter_id(encounter_id_reference)

        # procedure_date is required
        if (procedure_date_raw := procedure.get("performedDateTime")) is None:
            if (performed_period := procedure.get("performedPeriod")) is None:
                return
            else:
                procedure_date_raw = performed_period.get("start")

        try:
            procedure_date = datetime.datetime.fromisoformat(procedure_date_raw)
        except ValueError:
            return

        # type_code and type_code_system are required
        type_code, type_code_system = ProceduresBatching._find_code(procedure.get("code"))
        if type_code is None or type_code_system is None:
            return

        valid_encounter = {
            'source_id': str(source_id),
            'patient_id': patient_id,
            'encounter_id': encounter_id,
            'procedure_date': procedure_date,
            'type_code': type_code,
            'type_code_system': type_code_system,
        }

        self._valid_batch.append(valid_encounter)
