import asyncio
import logging
import time

import aiohttp
import asyncpgsa

from .tables import patients
from .settings import settings


logger = logging.getLogger(__name__)


class App:

    def __init__(self, loop: asyncio.AbstractEventLoop, settings: dict):
        self._settings = settings
        self._loop = loop
        self._queue: asyncio.Queue = asyncio.Queue(
            maxsize=settings['MAX_QUEUE_SIZE'],
            loop=self._loop,
        )

        self._config_logging()

    def _config_logging(self) -> None:
        logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter('[%(asctime)s - %(name)s - %(levelname)s] %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    async def _patients_worker(self, name: str, queue: asyncio.Queue) -> None:
        logger.info(f"Worker {name} START")

        async with self._pool.acquire() as conn:
            while True:
                item = await queue.get()
                await self._batcher.process(conn, item)
                queue.task_done()

    async def _prepare_patients_data(self, queue: asyncio.Queue) -> None:
        async with aiohttp.ClientSession(loop=self._loop) as session:
            async with session.get(self._settings['PATIENTS_PATH']) as response:
                while True:
                    chunk = await response.content.readline()
                    if not chunk:
                        logger.info("EOF reached")
                        break
                    await queue.put(chunk)

    async def main(self) -> None:
        self._pool = await asyncpgsa.create_pool(
            host=self._settings['POSTGRES_DATABASE_HOST'],
            database=self._settings['POSTGRES_DATABASE_NAME'],
            user=self._settings['POSTGRES_DATABASE_USERNAME'],
            password=self._settings['POSTGRES_DATABASE_PASSWORD'],
            min_size=self._settings['POSTGRES_MIN_CONNECTION_POOL_SIZE'],
            max_size=self._settings['POSTGRES_MAX_CONNECTION_POOL_SIZE'],
            loop=self._loop,
        )

        self._batcher = patients.Batching(self._pool, self._settings)
        asyncio.create_task(self._batcher.work())

        tasks = []
        for i in range(self._settings['QUEUE_WORKERS_AMOUNT']):
            task = asyncio.create_task(
                self._patients_worker(f'queue-worker-{i}', self._queue)
            )
            tasks.append(task)

        await self._prepare_patients_data(self._queue)

        await self._queue.join()

        for task in tasks:
            task.cancel()

        await self._batcher.proccess_batch()

        await asyncio.gather(*tasks, return_exceptions=True)


def start() -> None:
    started_at = time.monotonic()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    app = App(loop=loop, settings=settings)
    loop.run_until_complete(app.main())

    logger.info(f"TOTAL TIME: {time.monotonic() - started_at} s")
