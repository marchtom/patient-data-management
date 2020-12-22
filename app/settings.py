import os

from dotenv import load_dotenv


load_dotenv()

settings = dict(
    PATIENTS_PATH="https://raw.githubusercontent.com/smart-on-fhir/flat-fhir-files/master/r3/Patient.ndjson",

    POSTGRES_DATABASE_USERNAME=os.getenv("POSTGRES_DATABASE_USERNAME", "postgres"),
    POSTGRES_DATABASE_PASSWORD=os.getenv("POSTGRES_DATABASE_PASSWORD", "postgres"),
    POSTGRES_DATABASE_NAME=os.getenv("POSTGRES_DATABASE_NAME", "db1"),
    POSTGRES_DATABASE_PORT=int(os.getenv("POSTGRES_DATABASE_PORT", 5432)),
    POSTGRES_DATABASE_HOST=os.getenv("POSTGRES_DATABASE_HOST", "localhost"),

    POSTGRES_MIN_CONNECTION_POOL_SIZE=int(os.getenv("POSTGRES_MIN_CONNECTION_POOL_SIZE", 1)),
    POSTGRES_MAX_CONNECTION_POOL_SIZE=int(os.getenv("POSTGRES_MAX_CONNECTION_POOL_SIZE", 20)),

    MAX_QUEUE_SIZE=int(os.getenv("MAX_QUEUE_SIZE", 10)),
    QUEUE_WORKERS_AMOUNT=int(os.getenv("QUEUE_WORKERS_AMOUNT", 5)),

    BATCHER_SLEEP_TIME=int(os.getenv("BATCHER_SLEEP_TIME", 0.1)),
)
