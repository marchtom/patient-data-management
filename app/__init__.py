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

        async with self.pool.acquire() as conn:
            while True:
                item = await queue.get()
                await self.batcher.process(conn, item)
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
        self.pool = await asyncpgsa.create_pool(
            host=settings['POSTGRES_DATABASE_HOST'],
            database=settings['POSTGRES_DATABASE_NAME'],
            user=settings['POSTGRES_DATABASE_USERNAME'],
            password=settings['POSTGRES_DATABASE_PASSWORD'],
            max_size=settings['POSTGRES_CONNECTION_POOL_SIZE'],
            loop=self._loop,
        )

        self.batcher = patients.Batching(self.pool, self._settings)
        batcher_task = asyncio.create_task(self.batcher.work())

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

        batcher_task.cancel()
        await self.batcher.save_batch()

        await asyncio.gather(*tasks, return_exceptions=True)


def start() -> None:
    started_at = time.monotonic()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    app = App(loop=loop, settings=settings)
    loop.run_until_complete(app.main())

    logger.info(f"TOTAL TIME: {time.monotonic() - started_at} s")
