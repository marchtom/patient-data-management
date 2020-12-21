from setuptools import find_packages, setup

setup(
    name='patient-data',
    packages=find_packages(''),
    entry_points="""
        [console_scripts]
        patient-data = app:start
    """,
    install_requires=[
        'aiohttp',
        'asyncpg',
        'python-dotenv',
        'SQLAlchemy',
    ],
    extras_require={
        'dev': [
            'flake8',
            'mypy',
        ],
    },
)
