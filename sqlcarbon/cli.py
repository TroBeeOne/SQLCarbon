"""Command-line interface for SQLcarbon."""
from __future__ import annotations

import logging
import sys
from datetime import datetime

import click

from .config_loader import MigrationPlan
from .orchestrator import run_plan


def _setup_logging() -> None:
    log_file = f"sqlcarbon_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


@click.group()
@click.version_option(package_name="sqlcarbon")
def cli() -> None:
    """SQLcarbon — reliable SQL Server table-to-table copy tool."""


@cli.command()
@click.argument("config_file", type=click.Path(exists=True, dir_okay=False))
def run(config_file: str) -> None:
    """Run all jobs defined in CONFIG_FILE."""
    _setup_logging()
    logger = logging.getLogger(__name__)

    try:
        plan = MigrationPlan.from_yaml(config_file)
    except Exception as exc:
        click.echo(f"ERROR: Failed to load config '{config_file}': {exc}", err=True)
        sys.exit(1)

    logger.info(
        "Loaded plan: %d connection(s), %d job(s) from '%s'",
        len(plan.connections), len(plan.jobs), config_file,
    )
    summary = run_plan(plan)
    sys.exit(0 if summary.failed == 0 else 1)


@cli.command()
@click.argument("config_file", type=click.Path(exists=True, dir_okay=False))
def validate(config_file: str) -> None:
    """Validate CONFIG_FILE without running any jobs."""
    try:
        plan = MigrationPlan.from_yaml(config_file)
        click.echo(
            f"OK: Config is valid — "
            f"{len(plan.connections)} connection(s), {len(plan.jobs)} job(s)."
        )
    except Exception as exc:
        click.echo(f"ERROR: {exc}", err=True)
        sys.exit(1)


@cli.command()
def init() -> None:
    """Print a sample plan.yaml to stdout."""
    click.echo(_SAMPLE_YAML)


_SAMPLE_YAML = """\
# SQLcarbon sample plan — copy/paste and edit as needed.

connections:
  source_db:
    server: "sql01.example.com"
    database: "SourceDB"
    auth:
      mode: "trusted"
    # driver: "ODBC Driver 17 for SQL Server"   # default; change to 18 if needed

  dest_db:
    server: "sql02.example.com,1445"
    database: "DestDB"
    auth:
      mode: "sql"
      username: "sa"
      password: "changeme"
    driver: "ODBC Driver 18 for SQL Server"

defaults:
  batch_size: 100000
  stop_on_failure: false
  create_indexes: false
  create_constraints: false
  include_extended_properties: false
  copy_mode: "full"          # full | schema_only | data_only
  nolock: true

jobs:
  - name: CopyCustomers
    source_connection: source_db
    destination_connection: dest_db
    source_table: dbo.Customers
    destination_table: dbo.Customers_Archive
    options:
      create_indexes: true
      stop_on_failure: false

  - name: CopyOrders
    source_connection: source_db
    destination_connection: dest_db
    source_table: dbo.Orders
    destination_table: dbo.Orders_Archive
    options:
      copy_mode: "schema_only"
      stop_on_failure: true
"""
