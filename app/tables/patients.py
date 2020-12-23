import json
import logging
from datetime import datetime
from typing import Any, Final, List, Optional, Tuple

import sqlalchemy as sa
from asyncpg.connection import Connection
from asyncpg.pool import Pool
from sqlalchemy.dialects import postgresql

from .basic_batcher import Batcher
from .db import metadata


logger = logging.getLogger(__name__)


patients_table = sa.Table(
    'patients', metadata,
    sa.Column('id', postgresql.INTEGER, primary_key=True),
    sa.Column('source_id', postgresql.TEXT, nullable=False),
    sa.Column('birth_date', postgresql.DATE),
    sa.Column('gender', postgresql.TEXT),
    sa.Column('race_code', postgresql.TEXT),
    sa.Column('race_code_system', postgresql.TEXT),
    sa.Column('ethnicity_code', postgresql.TEXT),
    sa.Column('ethnicity_code_system', postgresql.TEXT),
    sa.Column('country', postgresql.TEXT),
)


RACE_CODE_URL: Final = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race"
ETHNICITY_CODE_URL: Final = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity"


async def get_patient_id(conn: Connection, source_id: str) -> str:
    query = (
        patients_table.select()
        .where(patients_table.c.source_id == source_id)
        .with_only_columns([patients_table.c.id])
    )
    return await conn.fetchval(query)


class PatientsBatching(Batcher):

    def __init__(self, pool: Pool, settings: dict) -> None:
        super().__init__(pool, settings, patients_table)

    @staticmethod
    def _find_code(extension: List[Any], url: str) -> Tuple[Optional[str], Optional[str]]:
        if not extension:
            return None, None

        for item in extension:
            if not hasattr(item, "get"):
                continue
            if item.get('url') == url:
                # we can expect missing keys or empty lists at this point
                try:
                    valueCodeableConcept = item.get('valueCodeableConcept')
                    coding = valueCodeableConcept.get('coding')
                    code = coding[0].get('code')
                    system = coding[0].get('system')
                    return code, system
                except (AttributeError, IndexError):
                    return None, None

        return None, None

    async def process(self, conn: Connection, item: str) -> None:
        await super().process(conn, item)

        # skip items that are not valid JSON
        try:
            patient = json.loads(item)
        except json.JSONDecodeError:
            logger.info("invalid JSON")
            return

        # source_id is required
        if (source_id := patient.get('id')) is None:
            return

        # birth_date is optional, but might be invalid
        try:
            birth_date: Optional[datetime] = datetime.strptime(patient.get('birthDate'), '%Y-%m-%d')
        except (ValueError, TypeError):
            birth_date = None

        # address is optional
        if (addresses := patient.get('address')):
            country = addresses[0].get('country')
        else:
            country = None

        # race_code is optional
        race_code, race_code_system = PatientsBatching._find_code(
            patient.get('extension'), RACE_CODE_URL,
        )

        # ethnicity_code is optional
        ethnicity_code, ethnicity_code_system = PatientsBatching._find_code(
            patient.get('extension'), ETHNICITY_CODE_URL,
        )

        valid_patient = {
            'source_id': str(source_id),
            'birth_date': birth_date,
            'gender': patient.get('gender'),
            'country': country,
            'race_code': race_code,
            'race_code_system': race_code_system,
            'ethnicity_code': ethnicity_code,
            'ethnicity_code_system': ethnicity_code_system,
        }

        self._valid_batch.append(valid_patient)
