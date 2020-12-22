import asyncio
import logging
import time
from typing import Optional

import aiohttp
import asyncpgsa
from asyncpg.pool import Pool

from .tables import encounters, observations, patients, procedures
from .tables.basic_batcher import Batcher
from .settings import settings


logger = logging.getLogger(__name__)


class App:

    def __init__(self, loop: asyncio.AbstractEventLoop, settings: dict):
        self._settings = settings
        self._loop = loop
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=settings['MAX_QUEUE_SIZE'])

        self._config_logging()

    def _config_logging(self) -> None:
        logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
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
        if pool is None:
            pool = await self.create_pool()

        batcher: patients.PatientsBatching = patients.PatientsBatching(pool, self._settings)
        await self._resolve_data(batcher, self._settings['PATIENTS_PATH'], pool)

    async def resolve_encounters(self, pool: Optional[Pool] = None) -> None:
        if pool is None:
            pool = await self.create_pool()

        batcher: encounters.EncountersBatching = encounters.EncountersBatching(pool, self._settings)
        await self._resolve_data(batcher, self._settings['ENCOUNTERS_PATH'], pool)

    async def resolve_procedures(self, pool: Optional[Pool] = None) -> None:
        if pool is None:
            pool = await self.create_pool()

        batcher: procedures.ProceduresBatching = procedures.ProceduresBatching(pool, self._settings)
        await self._resolve_data(batcher, self._settings['PROCEDURES_PATH'], pool)

    async def resolve_observations(self, pool: Optional[Pool] = None) -> None:
        if pool is None:
            pool = await self.create_pool()

        batcher: observations.ObservationsBatching = observations.ObservationsBatching(pool, self._settings)
        await self._resolve_data(batcher, self._settings['OBSERVATIONS_PATH'], pool)

    async def main(self) -> None:
        pool = await self.create_pool()

        started_at = time.monotonic()
        await self.resolve_patients(pool)
        logger.info(f"Patients resolving time: {(time.monotonic() - started_at):.4f} s")

        started_at = time.monotonic()
        await self.resolve_encounters(pool)
        logger.info(f"Encounters resolving time: {(time.monotonic() - started_at):.4f} s")

        started_at = time.monotonic()
        await self.resolve_procedures(pool)
        logger.info(f"Procedures resolving time: {(time.monotonic() - started_at):.4f} s")

        started_at = time.monotonic()
        await self.resolve_observations(pool)
        logger.info(f"Observations resolving time: {(time.monotonic() - started_at):.4f} s")


def init_app(loop: asyncio.AbstractEventLoop, settings: dict) -> App:
    return App(
        loop=loop,
        settings=settings,
    )


def start() -> None:
    started_at = time.monotonic()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    app = init_app(loop=loop, settings=settings)
    loop.run_until_complete(app.main())

    logger.info(f"TOTAL TIME: {(time.monotonic() - started_at):.4f} s")
