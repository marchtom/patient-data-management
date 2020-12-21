import asyncio
import logging
from typing import Final

import aiohttp


logger = logging.getLogger(__name__)


PATIENTS_PATH: Final = "https://raw.githubusercontent.com/smart-on-fhir/flat-fhir-files/master/r3/Patient.ndjson"


def config_logging() -> None:
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('[%(asctime)s - %(name)s - %(levelname)s] \n%(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)


async def prepare_patients_data() -> None:
    async with aiohttp.ClientSession() as session:
        async with session.get(PATIENTS_PATH) as response:
            while True:
                chunk = await response.content.readline()
                if not chunk:
                    logger.info("EOF reached")
                    break


async def main() -> None:
    config_logging()
    await prepare_patients_data()


def start() -> None:
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
