"""
SQLcarbon
=========
Reliable, deterministic SQL Server table-to-table copy tool.

Quickstart (library usage)::

    from sqlcarbon import MigrationPlan, run_plan

    plan = MigrationPlan.from_yaml("plan.yaml")
    summary = run_plan(plan)

    # or from a dict
    plan = MigrationPlan.from_dict({
        "connections": { ... },
        "jobs": [ ... ],
    })
"""
from .config_loader import (
    AuthConfig,
    ConnectionConfig,
    Defaults,
    JobConfig,
    JobOptions,
    MigrationPlan,
)
from .orchestrator import JobResult, RunSummary, run_plan

__version__ = "0.2.0"

__all__ = [
    "MigrationPlan",
    "ConnectionConfig",
    "AuthConfig",
    "JobConfig",
    "JobOptions",
    "Defaults",
    "run_plan",
    "RunSummary",
    "JobResult",
]
