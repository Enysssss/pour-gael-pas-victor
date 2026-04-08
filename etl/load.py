"""
Load stage — bulk-inserts a cleaned DataFrame into PostgreSQL via psycopg2.

Uses execute_values for efficient batch inserts.
Wraps every operation in a transaction with automatic rollback on error.
"""

import logging
import os
from typing import Optional

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from .exceptions import DatabaseLoadError

logger = logging.getLogger("etl")

# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

# Default DSN — overridden by DATABASE_URL env var if set
_DEFAULT_DSN = "postgresql+psycopg2://postgres:postgres@mainline.proxy.rlwy.net:51566/healthai"


def _build_dsn() -> str:
    """
    Return a psycopg2-compatible DSN string.
    Converts SQLAlchemy-style 'postgresql+psycopg2://...' to plain 'postgresql://...'.
    """
    raw = os.getenv("DATABASE_URL", _DEFAULT_DSN)
    # Strip the SQLAlchemy dialect prefix if present
    return raw.replace("postgresql+psycopg2://", "postgresql://")


def _get_connection() -> psycopg2.extensions.connection:
    """Open and return a new psycopg2 connection."""
    dsn = _build_dsn()
    try:
        conn = psycopg2.connect(dsn)
        conn.autocommit = False  # explicit transaction management
        return conn
    except psycopg2.OperationalError as exc:
        raise DatabaseLoadError(table_name="N/A", detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _get_table_columns(conn, table_name: str) -> list[str]:
    """Query information_schema to get the actual columns of the target table (public schema only)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,),
        )
        return [row[0] for row in cur.fetchall()]


def load(df: pd.DataFrame, table_name: str) -> int:
    """
    Insert all rows from *df* into *table_name* using a single transaction.

    Automatically aligns DataFrame columns with the actual table schema —
    extra columns in the DataFrame are silently dropped, missing columns
    are left to DB defaults.

    Parameters
    ----------
    df:
        Cleaned DataFrame produced by the clean stage.
    table_name:
        Target PostgreSQL table name.

    Returns
    -------
    int
        Number of rows successfully inserted.

    Raises
    ------
    DatabaseLoadError
        On any database error (connection failure, constraint violation, etc.).
    """
    if df.empty:
        logger.warning("load | table=%s DataFrame is empty — nothing to insert", table_name)
        return 0

    conn: psycopg2.extensions.connection | None = None
    try:
        conn = _get_connection()

        # ── Align columns with actual DB schema ──────────────────────────────
        db_columns = _get_table_columns(conn, table_name)
        if not db_columns:
            raise DatabaseLoadError(
                table_name=table_name,
                detail=f"Table '{table_name}' not found or has no columns.",
            )

        # Keep only columns present in BOTH the DataFrame and the DB table
        df_cols = set(df.columns)
        columns = [c for c in db_columns if c in df_cols]

        dropped = df_cols - set(db_columns)
        if dropped:
            logger.warning(
                "load | table=%s columns_not_in_db=%s (ignored)", table_name, sorted(dropped)
            )

        if not columns:
            raise DatabaseLoadError(
                table_name=table_name,
                detail="No matching columns between DataFrame and DB table.",
            )

        df = df[columns]

    except DatabaseLoadError:
        if conn:
            conn.close()
        raise
    except Exception as exc:
        if conn:
            conn.close()
        raise DatabaseLoadError(table_name=table_name, detail=str(exc)) from exc

    rows_to_insert = len(df)
    # Build tuples, converting NaN → None for NULL handling
    records = [
        tuple(None if pd.isna(v) else v for v in row)
        for row in df.itertuples(index=False, name=None)
    ]

    col_list = ", ".join(f'"{c}"' for c in columns)
    insert_sql = f'INSERT INTO "{table_name}" ({col_list}) VALUES %s ON CONFLICT DO NOTHING'

    logger.info("load | table=%s rows=%d columns=%s", table_name, rows_to_insert, columns)

    try:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, records, page_size=500)
            inserted = cur.rowcount if cur.rowcount >= 0 else rows_to_insert
        conn.commit()
        logger.info("load | table=%s inserted=%d", table_name, inserted)
        return inserted

    except Exception as exc:
        if conn:
            conn.rollback()
            logger.error("load | table=%s ROLLBACK — %s", table_name, exc)
        raise DatabaseLoadError(table_name=table_name, detail=str(exc)) from exc

    finally:
        if conn:
            conn.close()
