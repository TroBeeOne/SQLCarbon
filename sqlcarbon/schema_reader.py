"""Schema introspection for SQL Server tables."""
from __future__ import annotations

from dataclasses import dataclass, field

import pyodbc


def parse_table_ref(table_ref: str) -> tuple[str, str]:
    """Parse 'schema.table' or 'table' into (schema, table), stripping brackets."""
    if "." in table_ref:
        schema, table = table_ref.split(".", 1)
        return schema.strip("[]"), table.strip("[]")
    return "dbo", table_ref.strip("[]")


def table_exists(cursor: pyodbc.Cursor, schema_name: str, table_name: str) -> bool:
    """Return True if the table exists in the given schema."""
    cursor.execute(
        """
        SELECT 1
        FROM sys.objects o
        JOIN sys.schemas s ON s.schema_id = o.schema_id
        WHERE s.name = ? AND o.name = ? AND o.type = 'U'
        """,
        schema_name,
        table_name,
    )
    return cursor.fetchone() is not None


@dataclass
class ColumnInfo:
    name: str
    data_type: str
    is_nullable: bool
    char_length: int | None        # char count (nvarchar already divided by 2)
    numeric_precision: int | None
    numeric_scale: int | None
    datetime_precision: int | None
    is_identity: bool = False
    identity_seed: int | None = None
    identity_increment: int | None = None
    is_computed: bool = False
    computed_definition: str | None = None
    computed_is_persisted: bool = False


@dataclass
class IndexColumnInfo:
    name: str
    is_descending: bool
    is_included: bool


@dataclass
class IndexInfo:
    name: str
    is_unique: bool
    is_primary_key: bool
    type_desc: str                  # CLUSTERED or NONCLUSTERED
    columns: list[IndexColumnInfo] = field(default_factory=list)


@dataclass
class CheckConstraintInfo:
    name: str
    definition: str


@dataclass
class DefaultConstraintInfo:
    name: str
    column_name: str
    definition: str


@dataclass
class ExtendedPropertyInfo:
    name: str
    value: str
    column_name: str | None = None  # None = table-level


@dataclass
class SchemaInfo:
    schema_name: str
    table_name: str
    columns: list[ColumnInfo] = field(default_factory=list)
    indexes: list[IndexInfo] = field(default_factory=list)
    check_constraints: list[CheckConstraintInfo] = field(default_factory=list)
    default_constraints: list[DefaultConstraintInfo] = field(default_factory=list)
    extended_properties: list[ExtendedPropertyInfo] = field(default_factory=list)

    @property
    def identity_column(self) -> ColumnInfo | None:
        for col in self.columns:
            if col.is_identity:
                return col
        return None

    @property
    def copyable_columns(self) -> list[ColumnInfo]:
        """Columns that can be included in SELECT/INSERT (excludes computed columns)."""
        return [c for c in self.columns if not c.is_computed]


def read_schema(
    cursor: pyodbc.Cursor,
    table_ref: str,
    include_indexes: bool = False,
    include_constraints: bool = False,
    include_extended_properties: bool = False,
) -> SchemaInfo:
    """Read full schema metadata for a table."""
    schema_name, table_name = parse_table_ref(table_ref)
    info = SchemaInfo(schema_name=schema_name, table_name=table_name)

    # ── Columns ──────────────────────────────────────────────────────────────
    cursor.execute(
        """
        SELECT
            c.name,
            tp.name                                              AS data_type,
            c.is_nullable,
            CASE WHEN tp.name IN
                      ('varchar','nvarchar','char','nchar','binary','varbinary')
                 THEN c.max_length
                 ELSE NULL
            END                                                  AS raw_max_length,
            CASE WHEN tp.name IN ('decimal','numeric')
                 THEN c.precision
                 ELSE NULL
            END                                                  AS numeric_precision,
            CASE WHEN tp.name IN ('decimal','numeric')
                 THEN c.scale
                 ELSE NULL
            END                                                  AS numeric_scale,
            CASE WHEN tp.name IN ('datetime2','datetimeoffset','time')
                 THEN c.scale
                 ELSE NULL
            END                                                  AS datetime_precision,
            c.is_identity,
            ic.seed_value,
            ic.increment_value,
            c.is_computed
        FROM sys.columns c
        JOIN sys.types tp
            ON tp.user_type_id = c.user_type_id
        JOIN sys.objects o
            ON o.object_id = c.object_id
        JOIN sys.schemas s
            ON s.schema_id = o.schema_id
        LEFT JOIN sys.identity_columns ic
            ON ic.object_id = c.object_id
           AND ic.column_id = c.column_id
        WHERE s.name = ? AND o.name = ?
        ORDER BY c.column_id
        """,
        schema_name,
        table_name,
    )
    rows = cursor.fetchall()
    if not rows:
        raise ValueError(
            f"Table '[{schema_name}].[{table_name}]' not found or has no columns. "
            f"Verify the table name and that the connection has SELECT permissions."
        )

    # Collect computed column definitions in one pass
    cursor.execute(
        """
        SELECT c.name, cc.definition, cc.is_persisted
        FROM sys.computed_columns cc
        JOIN sys.columns c
            ON c.object_id = cc.object_id AND c.column_id = cc.column_id
        JOIN sys.objects o ON o.object_id = cc.object_id
        JOIN sys.schemas s ON s.schema_id = o.schema_id
        WHERE s.name = ? AND o.name = ?
        """,
        schema_name,
        table_name,
    )
    computed_map: dict[str, tuple[str, bool]] = {
        row[0]: (row[1], bool(row[2])) for row in cursor.fetchall()
    }

    for row in rows:
        (
            name, data_type, is_nullable, raw_max_length,
            num_prec, num_scale, dt_prec, is_identity,
            seed, increment, is_computed,
        ) = row

        # Convert byte-length → char-length for Unicode types
        char_length = raw_max_length
        if data_type in ("nvarchar", "nchar") and char_length is not None and char_length != -1:
            char_length = char_length // 2

        computed_def = None
        computed_persisted = False
        if is_computed and name in computed_map:
            computed_def, computed_persisted = computed_map[name]

        info.columns.append(
            ColumnInfo(
                name=name,
                data_type=data_type,
                is_nullable=bool(is_nullable),
                char_length=char_length,
                numeric_precision=num_prec,
                numeric_scale=num_scale,
                datetime_precision=dt_prec,
                is_identity=bool(is_identity),
                identity_seed=int(seed) if seed not in (None, "") else None,
                identity_increment=int(increment) if increment not in (None, "") else None,
                is_computed=bool(is_computed),
                computed_definition=computed_def,
                computed_is_persisted=computed_persisted,
            )
        )

    # ── Indexes ───────────────────────────────────────────────────────────────
    if include_indexes:
        cursor.execute(
            """
            SELECT
                i.name,
                i.is_unique,
                i.is_primary_key,
                i.type_desc,
                c.name          AS col_name,
                ic.is_descending_key,
                ic.is_included_column,
                ic.key_ordinal
            FROM sys.indexes i
            JOIN sys.index_columns ic
                ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            JOIN sys.columns c
                ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            JOIN sys.objects o ON o.object_id = i.object_id
            JOIN sys.schemas s ON s.schema_id = o.schema_id
            WHERE s.name = ? AND o.name = ? AND i.type > 0
            ORDER BY i.index_id, ic.is_included_column, ic.key_ordinal
            """,
            schema_name,
            table_name,
        )
        index_map: dict[str, IndexInfo] = {}
        for row in cursor.fetchall():
            idx_name, is_unique, is_pk, type_desc, col_name, is_desc, is_included, _ = row
            if idx_name not in index_map:
                index_map[idx_name] = IndexInfo(
                    name=idx_name,
                    is_unique=bool(is_unique),
                    is_primary_key=bool(is_pk),
                    type_desc=type_desc,
                )
            index_map[idx_name].columns.append(
                IndexColumnInfo(
                    name=col_name,
                    is_descending=bool(is_desc),
                    is_included=bool(is_included),
                )
            )
        info.indexes = list(index_map.values())

    # ── Constraints ───────────────────────────────────────────────────────────
    if include_constraints:
        cursor.execute(
            """
            SELECT cc.name, cc.definition
            FROM sys.check_constraints cc
            JOIN sys.objects o ON o.object_id = cc.parent_object_id
            JOIN sys.schemas s ON s.schema_id = o.schema_id
            WHERE s.name = ? AND o.name = ?
            """,
            schema_name,
            table_name,
        )
        for row in cursor.fetchall():
            info.check_constraints.append(
                CheckConstraintInfo(name=row[0], definition=row[1])
            )

        cursor.execute(
            """
            SELECT dc.name, c.name AS column_name, dc.definition
            FROM sys.default_constraints dc
            JOIN sys.columns c
                ON c.object_id = dc.parent_object_id
               AND c.column_id = dc.parent_column_id
            JOIN sys.objects o ON o.object_id = dc.parent_object_id
            JOIN sys.schemas s ON s.schema_id = o.schema_id
            WHERE s.name = ? AND o.name = ?
            """,
            schema_name,
            table_name,
        )
        for row in cursor.fetchall():
            info.default_constraints.append(
                DefaultConstraintInfo(name=row[0], column_name=row[1], definition=row[2])
            )

    # ── Extended Properties ───────────────────────────────────────────────────
    if include_extended_properties:
        # Table-level
        cursor.execute(
            """
            SELECT p.name, CAST(p.value AS nvarchar(max))
            FROM sys.extended_properties p
            JOIN sys.objects o ON o.object_id = p.major_id
            JOIN sys.schemas s ON s.schema_id = o.schema_id
            WHERE p.class = 1 AND p.minor_id = 0
              AND s.name = ? AND o.name = ?
            """,
            schema_name,
            table_name,
        )
        for row in cursor.fetchall():
            info.extended_properties.append(
                ExtendedPropertyInfo(name=row[0], value=str(row[1]))
            )

        # Column-level
        cursor.execute(
            """
            SELECT p.name, CAST(p.value AS nvarchar(max)), c.name AS column_name
            FROM sys.extended_properties p
            JOIN sys.objects o ON o.object_id = p.major_id
            JOIN sys.schemas s ON s.schema_id = o.schema_id
            JOIN sys.columns c
                ON c.object_id = p.major_id AND c.column_id = p.minor_id
            WHERE p.class = 1 AND p.minor_id > 0
              AND s.name = ? AND o.name = ?
            """,
            schema_name,
            table_name,
        )
        for row in cursor.fetchall():
            info.extended_properties.append(
                ExtendedPropertyInfo(name=row[0], value=str(row[1]), column_name=row[2])
            )

    return info
