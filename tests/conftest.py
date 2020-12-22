from asyncio import AbstractEventLoop
from typing import Callable

import pytest
from _pytest.main import Session

from app.settings import settings

from . import create_database, init_database_schema


TEST_DATABASE_NAME = "test"


def pytest_sessionstart(session: Session) -> None:
    settings['POSTGRES_DATABASE_NAME'] = TEST_DATABASE_NAME
    create_database(settings=settings)


@pytest.fixture  # type: ignore
def loop(event_loop: AbstractEventLoop) -> AbstractEventLoop:
    """
    Without this default pytest-aiohttp one is used.
    """
    yield event_loop
    event_loop.close()


@pytest.fixture  # type: ignore
def database(loop: AbstractEventLoop, aiohttp_client: Callable) -> None:
    init_database_schema(settings=settings)
