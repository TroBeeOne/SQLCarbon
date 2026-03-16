"""Job orchestration for SQLcarbon."""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

from .config_loader import Defaults, JobConfig, MigrationPlan
from .connection import get_connection
from .copier import PartialCopyError, copy_data
from .ddl_generator import (
    generate_add_constraints,
    generate_create_indexes,
    generate_create_table,
    generate_extended_properties,
)
from .parquet_writer import write_parquet
from .schema_reader import parse_table_ref, read_schema, table_exists
from .version_checker import check_version_compatibility

logger = logging.getLogger(__name__)


@dataclass
class JobResult:
    job_name: str
    success: bool
    rows_copied: int = 0
    partial: bool = False
    error: str | None = None
    duration_seconds: float = 0.0


@dataclass
class RunSummary:
    total_jobs: int = 0
    succeeded: int = 0
    failed: int = 0
    results: list[JobResult] = field(default_factory=list)


def _resolve(job_val, default_val):
    """Return job-level value if set, otherwise fall back to the global default."""
    return job_val if job_val is not None else default_val


def run_plan(plan: MigrationPlan) -> RunSummary:
    """Execute all jobs in the migration plan sequentially."""
    summary = RunSummary(total_jobs=len(plan.jobs))

    for job in plan.jobs:
        result = _run_single_job(job, plan)
        summary.results.append(result)

        if result.success:
            summary.succeeded += 1
        else:
            summary.failed += 1
            stop = _resolve(job.options.stop_on_failure, plan.defaults.stop_on_failure)
            if stop:
                logger.error(
                    "stop_on_failure=true for job '%s'. Halting remaining jobs.",
                    job.name,
                )
                break

    _log_summary(summary)
    return summary


def _run_single_job(job: JobConfig, plan: MigrationPlan) -> JobResult:
    defaults: Defaults = plan.defaults
    logger.info("=" * 60)
    logger.info("Starting Job: [%s]", job.name)
    logger.info("=" * 60)
    start = time.monotonic()

    # Resolve effective options (job-level overrides globals)
    copy_mode          = _resolve(job.options.copy_mode,                    defaults.copy_mode)
    batch_size         = _resolve(job.options.batch_size,                   defaults.batch_size)
    create_indexes     = _resolve(job.options.create_indexes,               defaults.create_indexes)
    create_constraints = _resolve(job.options.create_constraints,           defaults.create_constraints)
    include_ext_props  = _resolve(job.options.include_extended_properties,  defaults.include_extended_properties)
    nolock             = defaults.nolock  # global-only setting

    is_parquet = job.destination_file is not None

    src_cfg = plan.connections[job.source_connection]
    dst_cfg = plan.connections[job.destination_connection] if not is_parquet else None

    src_conn = dst_conn = None
    rows_copied = 0

    try:
        logger.info(
            "[%s] Source:      %s / %s", job.name, src_cfg.server, src_cfg.database
        )
        if is_parquet:
            logger.info("[%s] Destination: Parquet → %s", job.name, job.destination_file)
        else:
            logger.info(
                "[%s] Destination: %s / %s", job.name, dst_cfg.server, dst_cfg.database
            )
        logger.info("[%s] Copy mode:   %s", job.name, copy_mode)

        src_conn = get_connection(src_cfg, autocommit=True)
        src_cursor = src_conn.cursor()

        # ── Parquet destination path ───────────────────────────────────────
        if is_parquet:
            if copy_mode == "schema_only":
                raise ValueError(
                    "copy_mode 'schema_only' is not valid for a parquet destination. "
                    "Use 'full' or 'data_only' (both write all rows to the file)."
                )
            logger.info("[%s] Reading source schema...", job.name)
            schema_info = read_schema(src_cursor, job.source_table)
            check_version_compatibility(
                src_cursor, None,
                [col.data_type for col in schema_info.columns],
                job.name,
            )
            logger.info(
                "[%s] Starting parquet export (batch_size=%s, nolock=%s)...",
                job.name, f"{batch_size:,}", nolock,
            )
            rows_copied = write_parquet(
                src_conn=src_conn,
                src_table_ref=job.source_table,
                schema_info=schema_info,
                destination_file=job.destination_file,
                batch_size=batch_size,
                nolock=nolock,
                job_name=job.name,
            )

            duration = time.monotonic() - start
            logger.info(
                "[%s] SUCCESS | rows=%s | duration=%.2fs",
                job.name, f"{rows_copied:,}", duration,
            )
            return JobResult(
                job_name=job.name,
                success=True,
                rows_copied=rows_copied,
                duration_seconds=duration,
            )

        # ── SQL destination path (existing behavior) ───────────────────────
        dst_conn = get_connection(dst_cfg, autocommit=False)
        dst_cursor = dst_conn.cursor()

        schema_info = None

        # ── Phase 1: Schema (create table + optional DDL) ──────────────────
        if copy_mode in ("full", "schema_only"):
            logger.info("[%s] Reading source schema...", job.name)
            schema_info = read_schema(
                src_cursor,
                job.source_table,
                include_indexes=create_indexes,
                include_constraints=create_constraints,
                include_extended_properties=include_ext_props,
            )

            # Warn about computed columns
            computed = [c.name for c in schema_info.columns if c.is_computed]
            if computed:
                logger.warning(
                    "[%s] Table has computed column(s): %s. "
                    "These will be recreated as computed columns on the destination "
                    "and excluded from the data INSERT.",
                    job.name, ", ".join(computed),
                )

            check_version_compatibility(
                src_cursor, dst_cursor,
                [col.data_type for col in schema_info.columns],
                job.name,
            )

            # Hard-fail if destination table already exists
            dst_schema, dst_table = parse_table_ref(job.destination_table)
            if table_exists(dst_cursor, dst_schema, dst_table):
                raise RuntimeError(
                    f"Destination table [{dst_schema}].[{dst_table}] already exists. "
                    f"SQLcarbon does not modify or overwrite existing tables. "
                    f"Drop or rename the destination table before retrying."
                )

            create_sql = generate_create_table(schema_info, job.destination_table)
            logger.info("[%s] Creating table [%s].[%s]...", job.name, dst_schema, dst_table)
            dst_cursor.execute(create_sql)
            dst_conn.commit()
            logger.info("[%s] Table created.", job.name)

            if create_indexes and schema_info.indexes:
                for idx_sql in generate_create_indexes(schema_info, job.destination_table):
                    preview = idx_sql.split("\n")[0][:72]
                    logger.info("[%s] Index: %s ...", job.name, preview)
                    dst_cursor.execute(idx_sql)
                    dst_conn.commit()

            if create_constraints and (
                schema_info.check_constraints or schema_info.default_constraints
            ):
                for cst_sql in generate_add_constraints(schema_info, job.destination_table):
                    preview = cst_sql.split("\n")[0][:72]
                    logger.info("[%s] Constraint: %s ...", job.name, preview)
                    dst_cursor.execute(cst_sql)
                    dst_conn.commit()

            if include_ext_props and schema_info.extended_properties:
                for ep_sql in generate_extended_properties(schema_info, job.destination_table):
                    dst_cursor.execute(ep_sql)
                dst_conn.commit()
                logger.info(
                    "[%s] Applied %d extended propert(y/ies).",
                    job.name, len(schema_info.extended_properties),
                )

        # ── Phase 2: Data copy ─────────────────────────────────────────────
        if copy_mode in ("full", "data_only"):
            if schema_info is None:
                # data_only: read schema for column info only
                logger.info("[%s] Reading source schema (column info for data copy)...", job.name)
                schema_info = read_schema(src_cursor, job.source_table)
                check_version_compatibility(
                    src_cursor, dst_cursor,
                    [col.data_type for col in schema_info.columns],
                    job.name,
                )

                # For data_only, give a clear error if destination table is missing
                dst_schema, dst_table = parse_table_ref(job.destination_table)
                if not table_exists(dst_cursor, dst_schema, dst_table):
                    raise RuntimeError(
                        f"data_only mode: destination table [{dst_schema}].[{dst_table}] "
                        f"does not exist. Create the table before running a data_only job."
                    )

            logger.info(
                "[%s] Starting data copy (batch_size=%s, nolock=%s)...",
                job.name, f"{batch_size:,}", nolock,
            )
            rows_copied = copy_data(
                src_conn=src_conn,
                dst_conn=dst_conn,
                src_table_ref=job.source_table,
                dst_table_ref=job.destination_table,
                schema_info=schema_info,
                batch_size=batch_size,
                nolock=nolock,
                job_name=job.name,
            )

        duration = time.monotonic() - start
        logger.info(
            "[%s] SUCCESS | rows=%s | duration=%.2fs",
            job.name, f"{rows_copied:,}", duration,
        )
        return JobResult(
            job_name=job.name,
            success=True,
            rows_copied=rows_copied,
            duration_seconds=duration,
        )

    except PartialCopyError as exc:
        duration = time.monotonic() - start
        logger.error(
            "[%s] PARTIAL FAILURE — %s rows were committed before the error occurred. "
            "Destination table contains INCOMPLETE data. Manual cleanup may be required.",
            job.name, f"{exc.rows_committed:,}",
        )
        logger.error("[%s] Error: %s", job.name, exc)
        return JobResult(
            job_name=job.name,
            success=False,
            rows_copied=exc.rows_committed,
            partial=True,
            error=str(exc),
            duration_seconds=duration,
        )

    except Exception as exc:
        duration = time.monotonic() - start
        logger.error("[%s] FAILED — %s", job.name, exc, exc_info=True)
        return JobResult(
            job_name=job.name,
            success=False,
            error=str(exc),
            duration_seconds=duration,
        )

    finally:
        if src_conn:
            try:
                src_conn.close()
            except Exception:
                pass
        if dst_conn:
            try:
                dst_conn.close()
            except Exception:
                pass
        logger.info("Finished Job: [%s]\n", job.name)


def _log_summary(summary: RunSummary) -> None:
    logger.info("=" * 60)
    logger.info("RUN SUMMARY")
    logger.info("=" * 60)
    logger.info("Total jobs : %d", summary.total_jobs)
    logger.info("Succeeded  : %d", summary.succeeded)
    logger.info("Failed     : %d", summary.failed)
    for r in summary.results:
        if r.success:
            status = "OK"
        elif r.partial:
            status = "PARTIAL"
        else:
            status = "FAILED"
        logger.info(
            "  [%s] %s — %s rows in %.2fs",
            status, r.job_name, f"{r.rows_copied:,}", r.duration_seconds,
        )
        if r.error:
            logger.info("         Error: %s", r.error)
    logger.info("=" * 60)
