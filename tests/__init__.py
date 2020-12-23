import argparse
import asyncio
from typing import List

import ndjson
import psycopg2
import psycopg2.extras
from aioresponses import aioresponses
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from app import init_app
from app.settings import settings


SLEEP_PERIOD = 1


def create_database(settings: dict) -> None:
    database_name = settings['POSTGRES_DATABASE_NAME']

    # connect to default `postgres` database
    with psycopg2.connect(
        database="postgres",
        host=settings['POSTGRES_DATABASE_HOST'],
        user=settings['POSTGRES_DATABASE_USERNAME'],
        password=settings['POSTGRES_DATABASE_PASSWORD'],
    ) as conn:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        # create `test` database if it's not present
        cur1 = conn.cursor()
        cur1.execute("SELECT FROM pg_database WHERE datname = %s", (database_name,))

        if cur1.fetchone() is None:
            cur2 = conn.cursor()
            cur2.execute(f'CREATE DATABASE {database_name}')
        else:
            print('database %s already exists, recreating', database_name)


def init_database_schema(settings: dict) -> None:
    with psycopg2.connect(
        database=settings['POSTGRES_DATABASE_NAME'],
        host=settings['POSTGRES_DATABASE_HOST'],
        user=settings['POSTGRES_DATABASE_USERNAME'],
        password=settings['POSTGRES_DATABASE_PASSWORD'],
    ) as conn:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    with open("sql_scripts/purge_tables.sql", "r") as f:
        cur1 = conn.cursor()
        cur1.execute(f.read())
    with open("sql_scripts/schema.sql", "r") as f:
        cur2 = conn.cursor()
        cur2.execute(f.read())


async def run_app(
    loop: asyncio.AbstractEventLoop,
    payload: List[dict],
    entity: str,
    mock_url: str,
) -> None:
    asyncio.set_event_loop(loop)
    with aioresponses() as mocked:
        body = ndjson.dumps(payload)
        mocked.get(mock_url, status=200, body=body)
        test_app = init_app(loop=loop, settings=settings, command_line_args=argparse.Namespace(verbose=False))

        if entity == "patients":
            task = loop.create_task(test_app.resolve_patients())
        elif entity == "encounters":
            task = loop.create_task(test_app.resolve_encounters())
        elif entity == "procedures":
            task = loop.create_task(test_app.resolve_procedures())
        elif entity == "observations":
            task = loop.create_task(test_app.resolve_observations())
        else:
            raise ValueError("unknown entity")

        await asyncio.sleep(SLEEP_PERIOD)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def run_patients_test(loop: asyncio.AbstractEventLoop, payload: List[dict]) -> None:
    await run_app(
        loop, payload, "patients", mock_url=settings['PATIENTS_PATH']
    )


async def run_encounters_test(loop: asyncio.AbstractEventLoop, payload: List[dict]) -> None:
    # add patients seeds
    with psycopg2.connect(
        database=settings['POSTGRES_DATABASE_NAME'],
        host=settings['POSTGRES_DATABASE_HOST'],
        user=settings['POSTGRES_DATABASE_USERNAME'],
        password=settings['POSTGRES_DATABASE_PASSWORD'],
    ) as conn:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with open("tests/seed/patients.sql", "r") as f:
            cur1 = conn.cursor()
            cur1.execute(f.read())

    await run_app(
        loop, payload, "encounters", mock_url=settings['ENCOUNTERS_PATH']
    )


async def run_procedures_test(loop: asyncio.AbstractEventLoop, payload: List[dict]) -> None:
    # add patients and encounters seeds
    with psycopg2.connect(
        database=settings['POSTGRES_DATABASE_NAME'],
        host=settings['POSTGRES_DATABASE_HOST'],
        user=settings['POSTGRES_DATABASE_USERNAME'],
        password=settings['POSTGRES_DATABASE_PASSWORD'],
    ) as conn:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with open("tests/seed/patients.sql", "r") as f:
            cur1 = conn.cursor()
            cur1.execute(f.read())
        with open("tests/seed/encounters.sql", "r") as f:
            cur2 = conn.cursor()
            cur2.execute(f.read())

    await run_app(
        loop, payload, "procedures", mock_url=settings['PROCEDURES_PATH']
    )


async def run_observations_test(loop: asyncio.AbstractEventLoop, payload: List[dict]) -> None:
    # add patients and encounters seeds
    with psycopg2.connect(
        database=settings['POSTGRES_DATABASE_NAME'],
        host=settings['POSTGRES_DATABASE_HOST'],
        user=settings['POSTGRES_DATABASE_USERNAME'],
        password=settings['POSTGRES_DATABASE_PASSWORD'],
    ) as conn:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with open("tests/seed/patients.sql", "r") as f:
            cur1 = conn.cursor()
            cur1.execute(f.read())
        with open("tests/seed/encounters.sql", "r") as f:
            cur2 = conn.cursor()
            cur2.execute(f.read())

    await run_app(
        loop, payload, "observations", mock_url=settings['OBSERVATIONS_PATH']
    )


def get_data(table: str) -> dict:
    with psycopg2.connect(
        database=settings['POSTGRES_DATABASE_NAME'],
        host=settings['POSTGRES_DATABASE_HOST'],
        user=settings['POSTGRES_DATABASE_USERNAME'],
        password=settings['POSTGRES_DATABASE_PASSWORD'],
    ) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                sql.SQL("SELECT * FROM {};").format(sql.Identifier(table))
            )
            return cur.fetchall()
