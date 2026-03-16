"""Configuration models and loaders for SQLcarbon."""
from __future__ import annotations

from typing import Literal

import yaml
from pydantic import BaseModel, Field, model_validator


class AuthConfig(BaseModel):
    mode: Literal["trusted", "sql"] = "trusted"
    username: str | None = None
    password: str | None = None

    @model_validator(mode="after")
    def _check_sql_credentials(self) -> AuthConfig:
        if self.mode == "sql" and (not self.username or not self.password):
            raise ValueError("SQL auth mode requires both username and password")
        return self


class ConnectionConfig(BaseModel):
    server: str
    database: str
    auth: AuthConfig = Field(default_factory=AuthConfig)
    driver: str = "ODBC Driver 17 for SQL Server"
    trust_server_certificate: bool = False


class JobOptions(BaseModel):
    batch_size: int | None = None
    create_indexes: bool | None = None
    create_constraints: bool | None = None
    include_extended_properties: bool | None = None
    stop_on_failure: bool | None = None
    copy_mode: Literal["full", "schema_only", "data_only"] | None = None


class JobConfig(BaseModel):
    name: str
    source_connection: str
    source_table: str
    # SQL Server destination
    destination_connection: str | None = None
    destination_table: str | None = None
    # Parquet destination
    destination_file: str | None = None
    options: JobOptions = Field(default_factory=JobOptions)

    @model_validator(mode="after")
    def _validate_destination(self) -> JobConfig:
        has_sql = self.destination_connection is not None and self.destination_table is not None
        has_parquet = self.destination_file is not None
        if has_sql and has_parquet:
            raise ValueError(
                f"Job '{self.name}': specify either destination_connection/destination_table "
                f"(SQL) or destination_file (Parquet), not both"
            )
        if not has_sql and not has_parquet:
            raise ValueError(
                f"Job '{self.name}': must specify either destination_connection + "
                f"destination_table (SQL) or destination_file (Parquet)"
            )
        return self


class Defaults(BaseModel):
    batch_size: int = 100000
    stop_on_failure: bool = False
    create_indexes: bool = False
    create_constraints: bool = False
    include_extended_properties: bool = False
    copy_mode: Literal["full", "schema_only", "data_only"] = "full"
    nolock: bool = True


class MigrationPlan(BaseModel):
    connections: dict[str, ConnectionConfig]
    jobs: list[JobConfig]
    defaults: Defaults = Field(default_factory=Defaults)

    @model_validator(mode="after")
    def _validate_job_connections(self) -> MigrationPlan:
        for job in self.jobs:
            if job.source_connection not in self.connections:
                raise ValueError(
                    f"Job '{job.name}': source_connection '{job.source_connection}' "
                    f"is not defined in connections"
                )
            if (
                job.destination_connection is not None
                and job.destination_connection not in self.connections
            ):
                raise ValueError(
                    f"Job '{job.name}': destination_connection '{job.destination_connection}' "
                    f"is not defined in connections"
                )
        return self

    @classmethod
    def from_yaml(cls, path: str) -> MigrationPlan:
        """Load a MigrationPlan from a YAML file path."""
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return cls.model_validate(data)

    @classmethod
    def from_yaml_string(cls, text: str) -> MigrationPlan:
        """Load a MigrationPlan from a YAML string."""
        data = yaml.safe_load(text)
        return cls.model_validate(data)

    @classmethod
    def from_dict(cls, data: dict) -> MigrationPlan:
        """Load a MigrationPlan from a Python dict."""
        return cls.model_validate(data)
