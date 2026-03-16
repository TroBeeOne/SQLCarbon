"""Parquet file writer for SQLcarbon."""
from __future__ import annotations

import logging
import os
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pyodbc

from .schema_reader import ColumnInfo, SchemaInfo, parse_table_ref

logger = logging.getLogger(__name__)


def _sql_to_arrow_type(col: ColumnInfo) -> pa.DataType:
    """Map a SQL Server column type to the closest PyArrow type."""
    dt = col.data_type.lower()

    if dt == "bigint":
        return pa.int64()
    if dt in ("int", "integer"):
        return pa.int32()
    if dt == "smallint":
        return pa.int16()
    if dt == "tinyint":
        return pa.int8()
    if dt == "bit":
        return pa.bool_()
    if dt == "float":
        return pa.float64()
    if dt == "real":
        return pa.float32()
    if dt in ("decimal", "numeric") and col.numeric_precision and col.numeric_scale is not None:
        return pa.decimal128(col.numeric_precision, col.numeric_scale)
    if dt == "money":
        return pa.decimal128(19, 4)
    if dt == "smallmoney":
        return pa.decimal128(10, 4)
    if dt == "date":
        return pa.date32()
    if dt in ("datetime", "datetime2", "smalldatetime"):
        return pa.timestamp("us")
    if dt == "datetimeoffset":
        return pa.timestamp("us", tz="UTC")
    if dt == "time":
        return pa.time64("us")
    if dt in ("binary", "varbinary", "image", "timestamp", "rowversion"):
        return pa.binary()
    if dt == "uniqueidentifier":
        return pa.string()
    # varchar, nvarchar, char, nchar, text, ntext, xml, sql_variant, geography, geometry, etc.
    return pa.string()


def _build_arrow_schema(schema_info: SchemaInfo) -> pa.Schema:
    fields = [
        pa.field(col.name, _sql_to_arrow_type(col), nullable=col.is_nullable)
        for col in schema_info.copyable_columns
    ]
    return pa.schema(fields)


def _coerce_value(val, arrow_type: pa.DataType):
    """Coerce a pyodbc value to something PyArrow can accept."""
    if val is None:
        return None
    if isinstance(val, Decimal):
        return val  # pa.decimal128 accepts Python Decimal natively
    if pa.types.is_string(arrow_type) and not isinstance(val, str):
        return str(val)
    if pa.types.is_binary(arrow_type) and isinstance(val, (bytes, bytearray)):
        return bytes(val)
    return val


def write_parquet(
    src_conn: pyodbc.Connection,
    src_table_ref: str,
    schema_info: SchemaInfo,
    destination_file: str,
    batch_size: int,
    nolock: bool,
    job_name: str,
) -> int:
    """
    Stream rows from source and write to a Parquet file.

    Returns the total number of rows written.
    Overwrites the file if it already exists.
    """
    src_schema, src_table = parse_table_ref(src_table_ref)
    cols = schema_info.copyable_columns
    col_names = [f"[{col.name}]" for col in cols]

    nolock_hint = " WITH (NOLOCK)" if nolock else ""
    select_sql = (
        f"SELECT {', '.join(col_names)} "
        f"FROM [{src_schema}].[{src_table}]{nolock_hint};"
    )

    # Create parent directories if needed
    parent = os.path.dirname(os.path.abspath(destination_file))
    if parent:
        os.makedirs(parent, exist_ok=True)

    if os.path.exists(destination_file):
        logger.warning(
            "[%s] Parquet file '%s' already exists and will be overwritten.",
            job_name, destination_file,
        )

    arrow_schema = _build_arrow_schema(schema_info)
    arrow_types = [arrow_schema.field(col.name).type for col in cols]

    src_cursor = src_conn.cursor()
    src_cursor.execute(select_sql)

    total_rows = 0
    writer: pq.ParquetWriter | None = None

    try:
        while True:
            rows = src_cursor.fetchmany(batch_size)
            if not rows:
                break

            # Transpose row-oriented data to column-oriented arrays
            col_data: list[list] = [[] for _ in cols]
            for row in rows:
                for i, val in enumerate(row):
                    col_data[i].append(_coerce_value(val, arrow_types[i]))

            arrays: list[pa.Array] = []
            for i, col in enumerate(cols):
                try:
                    arrays.append(pa.array(col_data[i], type=arrow_types[i]))
                except Exception:
                    # Fallback: stringify the column rather than abort the job
                    logger.warning(
                        "[%s] Column '%s' could not be cast to %s; falling back to string.",
                        job_name, col.name, arrow_types[i],
                    )
                    arrays.append(
                        pa.array(
                            [str(v) if v is not None else None for v in col_data[i]],
                            type=pa.string(),
                        )
                    )

            batch = pa.Table.from_arrays(arrays, schema=arrow_schema)

            if writer is None:
                writer = pq.ParquetWriter(destination_file, arrow_schema)
            writer.write_table(batch)

            total_rows += len(rows)
            logger.info("[%s]   ... %s rows written.", job_name, f"{total_rows:,}")

    finally:
        if writer:
            writer.close()
        try:
            src_cursor.close()
        except Exception:
            pass

    return total_rows
