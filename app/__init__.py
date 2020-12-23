import argparse
import asyncio
import logging
import time
from typing import Optional

import aiohttp
import asyncpgsa
import psycopg2
from asyncpg.pool import Pool
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from .tables import encounters, observations, patients, procedures
from .tables.basic_batcher import Batcher
from .settings import settings


logger = logging.getLogger(__name__)


class App:

    def __init__(
        self, loop: asyncio.AbstractEventLoop, settings: dict,
        command_line_args: argparse.Namespace,
    ):
        self._settings = settings
        self._loop = loop
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=settings['MAX_QUEUE_SIZE'])
        self.stats: dict = {}

        self.command_line_args = command_line_args

        self._config_logging()

    def _config_logging(self) -> None:
        logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()

        if self.command_line_args.verbose:
            ch.setLevel(logging.DEBUG)
        else:
            ch.setLevel(logging.INFO)

        formatter = logging.Formatter('[%(asctime)s - %(name)s - %(levelname)s] %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    async def _worker(self, name: str, queue: asyncio.Queue, batcher: Batcher, pool: Pool) -> None:
        logger.debug(f"Worker {name} START")

        async with pool.acquire() as conn:
            while True:
                item = await queue.get()
                await batcher.process(conn, item)
                queue.task_done()

    async def _prepare_data(self, queue: asyncio.Queue, url: str) -> None:
        async with aiohttp.ClientSession(loop=self._loop) as session:
            async with session.get(url) as response:
                while True:
                    chunk = await response.content.readline()
                    if not chunk:
                        logger.debug("EOF reached")
                        break
                    await queue.put(chunk)

    async def _resolve_data(self, batcher: Batcher, url: str, pool: Pool) -> None:

        batcher_task = self._loop.create_task(batcher.work())

        tasks = []
        for i in range(self._settings['QUEUE_WORKERS_AMOUNT']):
            task = self._loop.create_task(
                self._worker(f'queue-{i}', self._queue, batcher, pool)
            )
            tasks.append(task)

        await self._prepare_data(self._queue, url)

        await self._queue.join()

        for task in tasks:
            task.cancel()

        await batcher.proccess_batch()
        batcher_task.cancel()

        await asyncio.gather(*tasks, batcher_task, return_exceptions=True)

    async def create_pool(self) -> Pool:
        return await asyncpgsa.create_pool(
            host=self._settings['POSTGRES_DATABASE_HOST'],
            database=self._settings['POSTGRES_DATABASE_NAME'],
            user=self._settings['POSTGRES_DATABASE_USERNAME'],
            password=self._settings['POSTGRES_DATABASE_PASSWORD'],
            min_size=self._settings['POSTGRES_MIN_CONNECTION_POOL_SIZE'],
            max_size=self._settings['POSTGRES_MAX_CONNECTION_POOL_SIZE'],
            loop=self._loop,
        )

    async def resolve_patients(self, pool: Optional[Pool] = None) -> None:
        logger.info("Resolving Patients")
        started_at = time.monotonic()
        if pool is None:
            pool = await self.create_pool()

        batcher: patients.PatientsBatching = patients.PatientsBatching(pool, self._settings)
        await self._resolve_data(batcher, self._settings['PATIENTS_PATH'], pool)
        self.stats['patients'] = batcher.get_stats()
        logger.info(f"Patients resolving time: {(time.monotonic() - started_at):.4f} s")

    async def resolve_encounters(self, pool: Optional[Pool] = None) -> None:
        logger.info("Resolving Encounters")
        started_at = time.monotonic()
        if pool is None:
            pool = await self.create_pool()

        batcher: encounters.EncountersBatching = encounters.EncountersBatching(pool, self._settings)
        await self._resolve_data(batcher, self._settings['ENCOUNTERS_PATH'], pool)
        self.stats['encounters'] = batcher.get_stats()
        logger.info(f"Encounters resolving time: {(time.monotonic() - started_at):.4f} s")

    async def resolve_procedures(self, pool: Optional[Pool] = None) -> None:
        logger.info("Resolving Procedures")
        started_at = time.monotonic()
        if pool is None:
            pool = await self.create_pool()

        batcher: procedures.ProceduresBatching = procedures.ProceduresBatching(pool, self._settings)
        await self._resolve_data(batcher, self._settings['PROCEDURES_PATH'], pool)
        self.stats['procedures'] = batcher.get_stats()
        logger.info(f"Procedures resolving time: {(time.monotonic() - started_at):.4f} s")

    async def resolve_observations(self, pool: Optional[Pool] = None) -> None:
        logger.info("Resolving Observations")
        started_at = time.monotonic()
        if pool is None:
            pool = await self.create_pool()

        batcher: observations.ObservationsBatching = observations.ObservationsBatching(pool, self._settings)
        await self._resolve_data(batcher, self._settings['OBSERVATIONS_PATH'], pool)
        self.stats['observations'] = batcher.get_stats()
        logger.info(f"Observations resolving time: {(time.monotonic() - started_at):.4f} s")

    async def post_run_stats(self, pool: Pool) -> None:
        self.stats['patients_genders'] = await patients.patients_by_gender(pool)
        self.stats['most_popular_procedures'] = await procedures.most_popular_procedures(pool)
        self.stats['popular_start_encounters_days'] = await encounters.popular_start_encounters_days(pool)
        self.stats['popular_end_encounters_days'] = await encounters.popular_end_encounters_days(pool)

        self.print_final_report()

    def print_final_report(self) -> None:
        print("- Final Report -")

        print("Batchers statistics:")
        print(f"\tPatients item processed:       {self.stats.get('patients', {}).get('processed_items', 0):8}")
        print(f"\tPatients records inserted:     {self.stats.get('patients', {}).get('inserted_records', 0):8}")
        print(f"\tEncounters item processed:     {self.stats.get('encounters', {}).get('processed_items', 0):8}")
        print(f"\tEncounters records inserted:   {self.stats.get('encounters', {}).get('inserted_records', 0):8}")
        print(f"\tProcedures item processed:     {self.stats.get('procedures', {}).get('processed_items', 0):8}")
        print(f"\tProcedures records inserted:   {self.stats.get('procedures', {}).get('inserted_records', 0):8}")
        print(f"\tObservations item processed:   {self.stats.get('observations', {}).get('processed_items', 0):8}")
        print(f"\tObservations records inserted: {self.stats.get('observations', {}).get('inserted_records', 0):8}")

        print("Additional statistics:")

        print("\tPatients by gender:")
        for item in self.stats['patients_genders'].items():
            print(f"\t{item[0]:>20} {item[1]:8}")

        print("\t10 most popular procedures:")
        for item in self.stats['most_popular_procedures'].items():
            print(f"\t{item[0]:>20} {item[1]:8}")

        print("\tMost popular start encounter days of week:")
        for item in self.stats['popular_start_encounters_days'].items():
            print(f"\t{item[0]:>20} {item[1]:8}")

        print("\tMost popular end encounter days of week:")
        for item in self.stats['popular_end_encounters_days'].items():
            print(f"\t{item[0]:>20} {item[1]:8}")

    async def main_single_entity(self, pool: Pool, entity: str) -> None:
        if entity == "patients":
            await self.resolve_patients(pool)
        elif entity == "encounters":
            await self.resolve_encounters(pool)
        elif entity == "procedures":
            await self.resolve_procedures(pool)
        elif entity == "observations":
            await self.resolve_observations(pool)

    async def main(self) -> None:
        pool = await self.create_pool()

        if (entity := self.command_line_args.entity):
            await self.main_single_entity(pool, entity)
        else:
            await self.resolve_patients(pool)
            await self.resolve_encounters(pool)
            await self.resolve_procedures(pool)
            await self.resolve_observations(pool)

        await self.post_run_stats(pool)


def clear_data() -> None:
    with psycopg2.connect(
        database=settings['POSTGRES_DATABASE_NAME'],
        host=settings['POSTGRES_DATABASE_HOST'],
        user=settings['POSTGRES_DATABASE_USERNAME'],
        password=settings['POSTGRES_DATABASE_PASSWORD'],
    ) as conn:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur1 = conn.cursor()
        with open("sql_scripts/clear_data.sql", "r") as file:
            cur1.execute(file.read())


def init_app(
    loop: asyncio.AbstractEventLoop,
    settings: dict,
    command_line_args: argparse.Namespace,
) -> App:
    return App(
        loop=loop,
        settings=settings,
        command_line_args=command_line_args,
    )


def start() -> None:
    parser = argparse.ArgumentParser(description='patient data processing')
    parser.add_argument(
        '-c', '--clean', action='store_true',
        help="Clears database before running app, previously stored all data will be lost"
    )
    parser.add_argument('-v', '--verbose', action='store_true', help="Increased verbosity, DEBUG level logs are shown")
    parser.add_argument(
        '-e', '--entity', choices=["patients", "encounters", "procedures", "observations"],
        help="Run app for single entity.",
    )
    args = parser.parse_args()

    if args.clean:
        clear_data()

    started_at = time.monotonic()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    app = init_app(loop=loop, settings=settings, command_line_args=args)
    loop.run_until_complete(app.main())

    logger.info(f"TOTAL TIME: {(time.monotonic() - started_at):.4f} s")
