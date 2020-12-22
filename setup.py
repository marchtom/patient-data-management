from setuptools import find_packages, setup

setup(
    name='patient-data',
    packages=find_packages('', exclude=['tests']),
    entry_points="""
        [console_scripts]
        patient-data = app:start
    """,
    install_requires=[
        'aiohttp',
        'asyncpg',
        'asyncpgsa',
        'python-dotenv',
        'SQLAlchemy',
    ],
    extras_require={
        'dev': [
            'aioresponses',
            'flake8',
            'mypy',
            'psycopg2-binary',
            'pytest-aiohttp',
            'pytest-asyncio',
            'pytest',
        ],
    },
)
