import psycopg2
from invoke import Collection, task
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from app.settings import settings

# Main namespace
ns = Collection()

# Database namespace
db = Collection('db')


@task(name='lint')
def lint(c):
    '''Runs mypy and flake8'''
    c.run("mypy app", pty=True)
    c.run("flake8 app", pty=True)


@task(name='test')
def test(c):
    '''Runs tests'''
    c.run("pytest tests/", pty=True)


@task(
    name='schema',
    help={"path": "Path to .sql file with schema"},
)
def db_schema(c, path="sql_scripts/schema.sql"):
    '''Applies database schema from file'''

    with psycopg2.connect(
        database=settings['POSTGRES_DATABASE_NAME'],
        host=settings['POSTGRES_DATABASE_HOST'],
        user=settings['POSTGRES_DATABASE_USERNAME'],
        password=settings['POSTGRES_DATABASE_PASSWORD'],
    ) as conn:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur1 = conn.cursor()
        with open(path, "r") as file:
            cur1.execute(file.read())


@task(name='drop')
def db_drop(c):
    '''Drops every table in application's database'''

    with psycopg2.connect(
        database=settings['POSTGRES_DATABASE_NAME'],
        host=settings['POSTGRES_DATABASE_HOST'],
        user=settings['POSTGRES_DATABASE_USERNAME'],
        password=settings['POSTGRES_DATABASE_PASSWORD'],
    ) as conn:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur1 = conn.cursor()
        with open("sql_scripts/purge_tables.sql", "r") as file:
            cur1.execute(file.read())


db.add_task(db_schema)
db.add_task(db_drop)
ns.add_collection(db)

ns.add_task(lint)
ns.add_task(test)
