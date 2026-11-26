"""PostgreSQL connection settings and bounded startup retries."""

import logging
import os

import psycopg
from tenacity import (
    Retrying,
    before_sleep_log,
    retry_if_exception_type,
    stop_after_attempt,
    wait_fixed,
)

logger = logging.getLogger(__name__)


def get_connection() -> psycopg.Connection:
    """Return a psycopg connection, retrying while PostgreSQL is unavailable."""
    required = ("POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER")
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise ValueError(f"Missing database settings: {', '.join(missing)}")

    settings = {
        "host": os.environ["POSTGRES_HOST"],
        "dbname": os.environ["POSTGRES_DB"],
        "user": os.environ["POSTGRES_USER"],
        "port": os.environ.get("POSTGRES_PORT") or "5432",
    }
    if password := os.environ.get("POSTGRES_PASSWORD"):
        settings["password"] = password

    retry = Retrying(
        retry=retry_if_exception_type(psycopg.OperationalError),
        stop=stop_after_attempt(12),
        wait=wait_fixed(2),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    return retry(psycopg.connect, **settings, connect_timeout=3)
