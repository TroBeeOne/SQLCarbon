"""SQL Server version detection and compatibility warnings."""
from __future__ import annotations

import logging

import pyodbc

logger = logging.getLogger(__name__)

# data_type -> (min_major_version, introduced_in_name)
_TYPE_INTRODUCED: dict[str, tuple[int, str]] = {
    "xml":              (9,  "SQL Server 2005"),
    "varchar(max)":     (9,  "SQL Server 2005"),
    "nvarchar(max)":    (9,  "SQL Server 2005"),
    "varbinary(max)":   (9,  "SQL Server 2005"),
    "date":             (10, "SQL Server 2008"),
    "time":             (10, "SQL Server 2008"),
    "datetime2":        (10, "SQL Server 2008"),
    "datetimeoffset":   (10, "SQL Server 2008"),
    "hierarchyid":      (10, "SQL Server 2008"),
    "geometry":         (10, "SQL Server 2008"),
    "geography":        (10, "SQL Server 2008"),
}

_VERSION_NAMES: dict[int, str] = {
    8:  "SQL Server 2000",
    9:  "SQL Server 2005",
    10: "SQL Server 2008/R2",
    11: "SQL Server 2012",
    12: "SQL Server 2014",
    13: "SQL Server 2016",
    14: "SQL Server 2017",
    15: "SQL Server 2019",
    16: "SQL Server 2022",
}


def _get_major_version(cursor: pyodbc.Cursor) -> int | None:
    try:
        cursor.execute("SELECT SERVERPROPERTY('ProductVersion')")
        row = cursor.fetchone()
        if row and row[0]:
            return int(str(row[0]).split(".")[0])
    except Exception as exc:
        logger.warning("Could not query SQL Server version: %s", exc)
    return None


def _version_name(major: int) -> str:
    return _VERSION_NAMES.get(major, f"SQL Server (major version {major})")


def check_version_compatibility(
    src_cursor: pyodbc.Cursor,
    dst_cursor: pyodbc.Cursor | None,
    column_types: list[str],
    job_name: str,
) -> None:
    """Emit warnings if column types may be unsupported on the destination server.

    Pass dst_cursor=None for parquet destinations (source version is still logged).
    """
    src_ver = _get_major_version(src_cursor)
    if src_ver is None:
        logger.warning("[%s] Version compatibility check skipped (could not detect source version).", job_name)
        return

    if dst_cursor is None:
        logger.info("[%s] Source: %s | Destination: Parquet file", job_name, _version_name(src_ver))
        return

    dst_ver = _get_major_version(dst_cursor)
    if dst_ver is None:
        logger.warning("[%s] Version compatibility check skipped (could not detect destination version).", job_name)
        return

    logger.info(
        "[%s] Source: %s | Destination: %s",
        job_name, _version_name(src_ver), _version_name(dst_ver),
    )

    for col_type in column_types:
        if col_type in _TYPE_INTRODUCED:
            min_ver, introduced_in = _TYPE_INTRODUCED[col_type]
            if dst_ver < min_ver:
                logger.warning(
                    "[%s] WARNING: column type '%s' was introduced in %s but destination "
                    "is %s. This job will likely fail.",
                    job_name, col_type, introduced_in, _version_name(dst_ver),
                )
