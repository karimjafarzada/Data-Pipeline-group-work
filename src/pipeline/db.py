"""PostgreSQL connection settings and bounded startup retries."""

import logging
import os
import re

import psycopg
from tenacity import (
    Retrying,
    before_sleep_log,
    retry_if_exception,
    stop_after_attempt,
    wait_fixed,
)

logger = logging.getLogger(__name__)


def _retry_startup(error: BaseException) -> bool:
    if not isinstance(error, psycopg.OperationalError):
        return False
    if error.sqlstate in {"28P01", "28000"}:
        return False
    # libpq can omit SQLSTATE during the initial connection handshake.
    detail = str(error)
    return "authentication failed" not in detail and not re.search(
        r'role "[^"\n]+" does not exist', detail
    )


def get_connection(*, attempts: int = 12) -> psycopg.Connection:
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
        retry=retry_if_exception(_retry_startup),
        stop=stop_after_attempt(attempts),
        wait=wait_fixed(2),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    return retry(psycopg.connect, **settings, connect_timeout=3)
