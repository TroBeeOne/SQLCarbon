"""Connection building and management for SQLcarbon."""
from __future__ import annotations

import pyodbc

from .config_loader import ConnectionConfig


def build_connection_string(cfg: ConnectionConfig) -> str:
    """Build a pyodbc connection string from a ConnectionConfig."""
    parts = [
        f"DRIVER={{{cfg.driver}}}",
        f"SERVER={cfg.server}",
        f"DATABASE={cfg.database}",
    ]
    if cfg.auth.mode == "trusted":
        parts.append("Trusted_Connection=yes")
    else:
        parts.append(f"UID={cfg.auth.username}")
        parts.append(f"PWD={cfg.auth.password}")
    if cfg.trust_server_certificate:
        parts.append("Encrypt=yes")
        parts.append("TrustServerCertificate=yes")
    return ";".join(parts)


def get_connection(cfg: ConnectionConfig, autocommit: bool = False) -> pyodbc.Connection:
    """Open and return a pyodbc connection."""
    conn_str = build_connection_string(cfg)
    return pyodbc.connect(conn_str, autocommit=autocommit)
