import asyncio
from typing import List

import ndjson
import psycopg2
import psycopg2.extras
from aioresponses import aioresponses
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from app import init_app
from app.settings import settings


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


async def run_app(loop: asyncio.AbstractEventLoop, payload: List[dict]) -> None:
    asyncio.set_event_loop(loop)
    with aioresponses() as mocked:
        body = ndjson.dumps(payload)
        mocked.get(settings['PATIENTS_PATH'], status=200, body=body)
        test_app = init_app(loop=loop, settings=settings)
        task = loop.create_task(test_app.main())
        await asyncio.sleep(1)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def get_data() -> dict:
    with psycopg2.connect(
        database=settings['POSTGRES_DATABASE_NAME'],
        host=settings['POSTGRES_DATABASE_HOST'],
        user=settings['POSTGRES_DATABASE_USERNAME'],
        password=settings['POSTGRES_DATABASE_PASSWORD'],
    ) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM patients;")
            return cur.fetchall()
