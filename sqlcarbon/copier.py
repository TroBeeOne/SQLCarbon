"""Chunked data copy engine for SQLcarbon."""
from __future__ import annotations

import logging
from decimal import Decimal

import pyodbc

from .schema_reader import SchemaInfo, parse_table_ref

logger = logging.getLogger(__name__)


class PartialCopyError(RuntimeError):
    """Raised when a batch insert fails after some rows were already committed."""

    def __init__(self, message: str, rows_committed: int) -> None:
        super().__init__(message)
        self.rows_committed = rows_committed


def copy_data(
    src_conn: pyodbc.Connection,
    dst_conn: pyodbc.Connection,
    src_table_ref: str,
    dst_table_ref: str,
    schema_info: SchemaInfo,
    batch_size: int,
    nolock: bool,
    job_name: str,
) -> int:
    """
    Stream rows from source to destination in chunks.

    Returns the total number of rows successfully inserted.
    Raises PartialCopyError if a batch fails after rows have already been committed.
    Raises RuntimeError for failures before any rows are committed.
    """
    src_schema, src_table = parse_table_ref(src_table_ref)
    dst_schema, dst_table = parse_table_ref(dst_table_ref)

    # Only non-computed columns can be SELECTed and INSERTed explicitly
    cols = schema_info.copyable_columns
    col_names = [f"[{col.name}]" for col in cols]

    nolock_hint = " WITH (NOLOCK)" if nolock else ""
    select_sql = (
        f"SELECT {', '.join(col_names)} "
        f"FROM [{src_schema}].[{src_table}]{nolock_hint};"
    )
    params_placeholder = ", ".join(["?"] * len(col_names))
    insert_sql = (
        f"INSERT INTO [{dst_schema}].[{dst_table}] WITH (TABLOCK) "
        f"({', '.join(col_names)}) VALUES ({params_placeholder});"
    )

    src_cursor = src_conn.cursor()
    dst_cursor = dst_conn.cursor()
    dst_cursor.fast_executemany = True

    identity_col = schema_info.identity_column
    if identity_col:
        logger.info(
            "[%s] Setting IDENTITY_INSERT ON for [%s].[%s].",
            job_name, dst_schema, dst_table,
        )
        dst_cursor.execute(f"SET IDENTITY_INSERT [{dst_schema}].[{dst_table}] ON;")

    src_cursor.execute(select_sql)
    total_rows = 0

    try:
        while True:
            rows = src_cursor.fetchmany(batch_size)
            if not rows:
                break

            # pyodbc requires plain Python types; convert Decimal to str
            processed = [
                tuple(str(v) if isinstance(v, Decimal) else v for v in row)
                for row in rows
            ]

            try:
                dst_cursor.executemany(insert_sql, processed)
                dst_conn.commit()
                total_rows += len(rows)
                logger.info("[%s]   ... %s rows inserted.", job_name, f"{total_rows:,}")
            except Exception as batch_err:
                try:
                    dst_conn.rollback()
                except Exception:
                    pass
                msg = (
                    f"Batch insert failed after {total_rows:,} rows were committed. "
                    f"Destination table [{dst_schema}].[{dst_table}] contains incomplete data. "
                    f"Underlying error: {batch_err}"
                )
                if total_rows > 0:
                    raise PartialCopyError(msg, total_rows) from batch_err
                raise RuntimeError(msg) from batch_err

    finally:
        if identity_col:
            try:
                dst_cursor.execute(
                    f"SET IDENTITY_INSERT [{dst_schema}].[{dst_table}] OFF;"
                )
                dst_conn.commit()
                logger.info("[%s] IDENTITY_INSERT OFF.", job_name)
            except Exception as exc:
                logger.warning(
                    "[%s] Could not SET IDENTITY_INSERT OFF: %s", job_name, exc
                )
        try:
            src_cursor.close()
        except Exception:
            pass
        try:
            dst_cursor.close()
        except Exception:
            pass

    return total_rows
