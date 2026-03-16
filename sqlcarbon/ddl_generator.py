"""DDL generation from SchemaInfo objects."""
from __future__ import annotations

from .schema_reader import ColumnInfo, SchemaInfo, parse_table_ref


def _column_type_str(col: ColumnInfo) -> str:
    dt = col.data_type
    if dt in ("varchar", "nvarchar", "char", "nchar", "binary", "varbinary"):
        length = "max" if col.char_length == -1 else str(col.char_length)
        return f"{dt}({length})"
    if dt in ("decimal", "numeric"):
        return f"{dt}({col.numeric_precision}, {col.numeric_scale})"
    if dt in ("datetime2", "datetimeoffset", "time"):
        return f"{dt}({col.datetime_precision})"
    return dt


def generate_create_table(schema_info: SchemaInfo, dest_table_ref: str) -> str:
    """Return a CREATE TABLE statement for the destination table."""
    dest_schema, dest_table = parse_table_ref(dest_table_ref)

    col_defs: list[str] = []
    for col in schema_info.columns:
        if col.is_computed:
            defn = f"    [{col.name}] AS {col.computed_definition}"
            if col.computed_is_persisted:
                defn += " PERSISTED"
        else:
            type_str = _column_type_str(col)
            defn = f"    [{col.name}] {type_str}"
            if col.is_identity:
                seed = col.identity_seed if col.identity_seed is not None else 1
                inc = col.identity_increment if col.identity_increment is not None else 1
                defn += f" IDENTITY({seed},{inc})"
            defn += " NOT NULL" if not col.is_nullable else " NULL"
        col_defs.append(defn)

    cols_str = ",\n".join(col_defs)
    return f"CREATE TABLE [{dest_schema}].[{dest_table}] (\n{cols_str}\n);"


def generate_create_indexes(schema_info: SchemaInfo, dest_table_ref: str) -> list[str]:
    """Return CREATE INDEX / ALTER TABLE ADD CONSTRAINT PRIMARY KEY statements."""
    dest_schema, dest_table = parse_table_ref(dest_table_ref)
    statements: list[str] = []

    for idx in schema_info.indexes:
        key_cols = [c for c in idx.columns if not c.is_included]
        inc_cols = [c for c in idx.columns if c.is_included]

        key_parts = [
            f"[{c.name}] {'DESC' if c.is_descending else 'ASC'}" for c in key_cols
        ]

        if idx.is_primary_key:
            sql = (
                f"ALTER TABLE [{dest_schema}].[{dest_table}]\n"
                f"    ADD CONSTRAINT [{idx.name}] PRIMARY KEY {idx.type_desc}\n"
                f"    ({', '.join(key_parts)});"
            )
        else:
            unique_str = "UNIQUE " if idx.is_unique else ""
            sql = (
                f"CREATE {unique_str}{idx.type_desc} INDEX [{idx.name}]\n"
                f"    ON [{dest_schema}].[{dest_table}] ({', '.join(key_parts)})"
            )
            if inc_cols:
                inc_parts = [f"[{c.name}]" for c in inc_cols]
                sql += f"\n    INCLUDE ({', '.join(inc_parts)})"
            sql += ";"

        statements.append(sql)

    return statements


def generate_add_constraints(schema_info: SchemaInfo, dest_table_ref: str) -> list[str]:
    """Return ALTER TABLE ADD CONSTRAINT statements for check and default constraints."""
    dest_schema, dest_table = parse_table_ref(dest_table_ref)
    statements: list[str] = []

    for cc in schema_info.check_constraints:
        statements.append(
            f"ALTER TABLE [{dest_schema}].[{dest_table}]\n"
            f"    ADD CONSTRAINT [{cc.name}] CHECK {cc.definition};"
        )

    for dc in schema_info.default_constraints:
        statements.append(
            f"ALTER TABLE [{dest_schema}].[{dest_table}]\n"
            f"    ADD CONSTRAINT [{dc.name}] DEFAULT {dc.definition}"
            f" FOR [{dc.column_name}];"
        )

    return statements


def generate_extended_properties(
    schema_info: SchemaInfo, dest_table_ref: str
) -> list[str]:
    """Return sp_addextendedproperty EXEC statements."""
    dest_schema, dest_table = parse_table_ref(dest_table_ref)
    statements: list[str] = []

    for ep in schema_info.extended_properties:
        escaped = str(ep.value).replace("'", "''")
        if ep.column_name:
            sql = (
                f"EXEC sys.sp_addextendedproperty\n"
                f"    @name = N'{ep.name}', @value = N'{escaped}',\n"
                f"    @level0type = N'Schema', @level0name = N'{dest_schema}',\n"
                f"    @level1type = N'Table',  @level1name = N'{dest_table}',\n"
                f"    @level2type = N'Column', @level2name = N'{ep.column_name}';"
            )
        else:
            sql = (
                f"EXEC sys.sp_addextendedproperty\n"
                f"    @name = N'{ep.name}', @value = N'{escaped}',\n"
                f"    @level0type = N'Schema', @level0name = N'{dest_schema}',\n"
                f"    @level1type = N'Table',  @level1name = N'{dest_table}';"
            )
        statements.append(sql)

    return statements
