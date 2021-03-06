from setuptools import find_packages, setup

setup(
    name='etl-tool',
    packages=find_packages('', exclude=['tests']),
    entry_points="""
        [console_scripts]
        etl-tool = app:start
    """,
    install_requires=[
        'aiocache',
        'aiohttp',
        'asyncpg',
        'asyncpgsa',
        'invoke',
        'ujson',
        'msgpack',
        'python-dotenv',
        'SQLAlchemy',
    ],
    extras_require={
        'dev': [
            'aioresponses',
            'flake8',
            'mypy',
            'ndjson'
            'psycopg2-binary',
            'pytest-aiohttp',
            'pytest-asyncio',
            'pytest',
        ],
    },
)
