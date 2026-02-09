"""Database connection helpers."""

import os
from typing import Optional

import psycopg2
from psycopg2.extensions import connection as PgConnection


# Environment variables used for database configuration.
_ENV_DEFAULTS = {
    "DB_HOST": "localhost",
    "DB_PORT": "5432",
    "DB_NAME": "reform_register",
    "DB_USER": "postgres",
    "DB_PASSWORD": "postgres",
}


def get_db_connection(
    *,
    host: Optional[str] = None,
    port: Optional[str] = None,
    dbname: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
) -> PgConnection:
    """Create and return a PostgreSQL connection using psycopg2.

    Falls back to environment variables when explicit arguments are not provided.
    """

    conn = psycopg2.connect(
        host=host or os.getenv("DB_HOST", _ENV_DEFAULTS["DB_HOST"]),
        port=port or os.getenv("DB_PORT", _ENV_DEFAULTS["DB_PORT"]),
        dbname=dbname or os.getenv("DB_NAME", _ENV_DEFAULTS["DB_NAME"]),
        user=user or os.getenv("DB_USER", _ENV_DEFAULTS["DB_USER"]),
        password=password or os.getenv("DB_PASSWORD", _ENV_DEFAULTS["DB_PASSWORD"]),
    )
    return conn
