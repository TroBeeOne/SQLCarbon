# SQLcarbon

**Reliable, deterministic SQL Server table-to-table copy tool.**

Copy tables between SQL Server instances with a single command or a few lines of Python — no SSIS, no BCP scripts, no fuss.

> Created by **TroBeeOne LLC**

---

## Features

- Copy tables across different SQL Server instances (same or different versions)
- Supports trusted (Windows) and SQL authentication
- Recreates schema: columns, identity columns (with correct seed/increment), computed columns
- Optionally copies indexes, check/default constraints, and extended properties
- Three copy modes: **full**, **schema_only**, **data_only**
- **Export directly to Parquet files** — use SQL Server as a source and write `.parquet` output
- Chunked streaming reads with `fast_executemany` inserts — handles tables of any size
- Continues to the next job when one fails (configurable `stop_on_failure`)
- Clear, structured log files written to your working directory
- Version compatibility warnings (e.g., using `datetime2` against a SQL Server 2005 target)
- Use as a **CLI tool** or a **Python library**

---

## Installation

```bash
pip install sqlcarbon
```

Requires **Python 3.10+** and the [Microsoft ODBC Driver for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server).

---

## Quick Start — CLI

### 1. Generate a sample config

```bash
sqlcarbon init > plan.yaml
```

### 2. Edit `plan.yaml`

```yaml
connections:
  my_source:
    server: "sql01.example.com"
    database: "Sales"
    auth:
      mode: "trusted"

  my_dest:
    server: "sql02.example.com"
    database: "Archive"
    auth:
      mode: "sql"
      username: "sa"
      password: "yourpassword"

defaults:
  batch_size: 100000
  stop_on_failure: false
  create_indexes: false
  create_constraints: false
  include_extended_properties: false
  copy_mode: "full"
  nolock: true

jobs:
  - name: CopyCustomers
    source_connection: my_source
    destination_connection: my_dest
    source_table: dbo.Customers
    destination_table: dbo.Customers_Archive
```

### 3. Validate your config (no database changes)

```bash
sqlcarbon validate plan.yaml
```

```
OK: Config is valid — 2 connection(s), 1 job(s).
```

### 4. Run it

```bash
sqlcarbon run plan.yaml
```

```
2026-03-08 10:00:01 INFO     ============================================================
2026-03-08 10:00:01 INFO     Starting Job: [CopyCustomers]
2026-03-08 10:00:01 INFO     ============================================================
2026-03-08 10:00:01 INFO     [CopyCustomers] Source:      sql01.example.com / Sales
2026-03-08 10:00:01 INFO     [CopyCustomers] Destination: sql02.example.com / Archive
2026-03-08 10:00:01 INFO     [CopyCustomers] Copy mode:   full
2026-03-08 10:00:02 INFO     [CopyCustomers] Source: SQL Server 2019 | Destination: SQL Server 2019
2026-03-08 10:00:02 INFO     [CopyCustomers] Creating table [dbo].[Customers_Archive]...
2026-03-08 10:00:02 INFO     [CopyCustomers] Table created.
2026-03-08 10:00:02 INFO     [CopyCustomers] Starting data copy (batch_size=100,000, nolock=True)...
2026-03-08 10:00:04 INFO     [CopyCustomers]   ... 100,000 rows inserted.
2026-03-08 10:00:05 INFO     [CopyCustomers]   ... 185,432 rows inserted.
2026-03-08 10:00:05 INFO     [CopyCustomers] SUCCESS | rows=185,432 | duration=3.84s
```

A log file is also written to your current directory: `sqlcarbon_20260308_100001.log`

---

## Quick Start — Python Library

```python
from sqlcarbon import MigrationPlan, run_plan

plan = MigrationPlan.from_yaml("plan.yaml")
summary = run_plan(plan)

print(f"Succeeded: {summary.succeeded} / {summary.total_jobs}")
for result in summary.results:
    print(f"  {result.job_name}: {result.rows_copied:,} rows in {result.duration_seconds:.2f}s")
```

### Load from a Python dict

```python
from sqlcarbon import MigrationPlan, run_plan

plan = MigrationPlan.from_dict({
    "connections": {
        "src": {
            "server": "sql01.example.com",
            "database": "Sales",
            "auth": {"mode": "trusted"},
        },
        "dst": {
            "server": "sql02.example.com",
            "database": "Archive",
            "auth": {"mode": "sql", "username": "sa", "password": "yourpassword"},
        },
    },
    "jobs": [
        {
            "name": "CopyCustomers",
            "source_connection": "src",
            "destination_connection": "dst",
            "source_table": "dbo.Customers",
            "destination_table": "dbo.Customers_Archive",
        }
    ],
})

summary = run_plan(plan)
```

### Load from a YAML string

```python
from sqlcarbon import MigrationPlan, run_plan

yaml_text = """
connections:
  src:
    server: "sql01.example.com"
    database: "Sales"
    auth:
      mode: "trusted"
  dst:
    server: "sql02.example.com"
    database: "Archive"
    auth:
      mode: "trusted"
jobs:
  - name: CopyOrders
    source_connection: src
    destination_connection: dst
    source_table: dbo.Orders
    destination_table: dbo.Orders_Archive
"""

plan = MigrationPlan.from_yaml_string(yaml_text)
summary = run_plan(plan)
```

---

## Configuration Reference

### `connections`

Each named connection supports:

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `server` | Yes | — | Server name, IP, or `server,port` / `server:port` |
| `database` | Yes | — | Target database name |
| `auth.mode` | No | `trusted` | `trusted` (Windows auth) or `sql` (SQL auth) |
| `auth.username` | If `sql` | — | SQL login username |
| `auth.password` | If `sql` | — | SQL login password |
| `driver` | No | `ODBC Driver 17 for SQL Server` | ODBC driver name |
| `trust_server_certificate` | No | `false` | Set `true` to bypass SSL certificate validation (equivalent to SSMS "Trust server certificate") |

**Custom port example:**
```yaml
connections:
  my_conn:
    server: "sql01.example.com,1445"
    database: "MyDB"
    auth:
      mode: "trusted"
```

**ODBC Driver 18 example** (needed for newer SQL Server / Azure SQL):
```yaml
connections:
  my_conn:
    server: "sql01.example.com"
    database: "MyDB"
    auth:
      mode: "trusted"
    driver: "ODBC Driver 18 for SQL Server"
    trust_server_certificate: true   # bypass cert validation (like SSMS checkbox)
```

---

### `defaults`

Global defaults applied to all jobs unless overridden at the job level.

| Field | Default | Description |
|-------|---------|-------------|
| `batch_size` | `100000` | Rows per read/insert chunk |
| `stop_on_failure` | `false` | Stop all remaining jobs if one fails |
| `create_indexes` | `false` | Recreate indexes on destination |
| `create_constraints` | `false` | Recreate check and default constraints |
| `include_extended_properties` | `false` | Copy extended properties |
| `copy_mode` | `full` | `full`, `schema_only`, or `data_only` |
| `nolock` | `true` | Use `WITH (NOLOCK)` on source reads |

---

### `jobs`

Each job represents one table copy operation. A job writes to either a **SQL Server table** or a **Parquet file** — specify one, not both.

**SQL Server destination:**

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Friendly name shown in logs |
| `source_connection` | Yes | Name of a connection defined under `connections` |
| `source_table` | Yes | Source table, e.g. `dbo.Customers` |
| `destination_connection` | Yes (SQL) | Name of a connection defined under `connections` |
| `destination_table` | Yes (SQL) | Destination table, e.g. `dbo.Customers_Archive` |
| `options` | No | Per-job overrides (see below) |

**Parquet destination:**

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Friendly name shown in logs |
| `source_connection` | Yes | Name of a connection defined under `connections` |
| `source_table` | Yes | Source table, e.g. `dbo.Customers` |
| `destination_file` | Yes (Parquet) | Path to the output `.parquet` file |
| `options` | No | `batch_size`, `nolock`, `stop_on_failure` apply; `copy_mode` must be `full` or `data_only` |

**Per-job `options`** (all optional — fall back to `defaults` if omitted):

```yaml
options:
  batch_size: 50000
  create_indexes: true
  create_constraints: true
  include_extended_properties: false
  stop_on_failure: true
  copy_mode: "schema_only"
```

---

## Copy Modes

| Mode | Creates Table | Copies Data | Use When |
|------|:---:|:---:|----------|
| `full` (default) | Yes | Yes | Normal table archiving / migration |
| `schema_only` | Yes | No | Pre-create table structure before a data load |
| `data_only` | No | Yes | Destination table already exists; just load rows |

> **Safety:** SQLcarbon will **never** drop or truncate an existing table. If a destination table already exists when running `full` or `schema_only`, the job hard-fails with a clear error message and no data is touched.
>
> For `data_only`, if the destination table does **not** exist, the job hard-fails with a clear error message.

---

## Parquet Export Example

Export a SQL Server table directly to a `.parquet` file — no destination connection needed:

```yaml
connections:
  prod:
    server: "sql01.example.com"
    database: "Sales"
    auth:
      mode: "trusted"

defaults:
  batch_size: 100000
  nolock: true

jobs:
  - name: ExportCustomersToParquet
    source_connection: prod
    source_table: dbo.Customers
    destination_file: "C:/exports/customers.parquet"

  - name: ExportOrdersToParquet
    source_connection: prod
    source_table: dbo.Orders
    destination_file: "C:/exports/orders.parquet"
    options:
      batch_size: 50000
```

> **Note:** If the destination file already exists it will be overwritten. Parent directories are created automatically if they do not exist.

You can mix SQL and Parquet destinations in the same plan:

```yaml
jobs:
  - name: ArchiveCustomers          # SQL → SQL
    source_connection: prod
    destination_connection: archive
    source_table: dbo.Customers
    destination_table: dbo.Customers_Archive

  - name: ExportCustomers           # SQL → Parquet
    source_connection: prod
    source_table: dbo.Customers
    destination_file: "C:/exports/customers.parquet"
```

---

## Multiple Jobs Example

```yaml
connections:
  prod:
    server: "sql-prod.example.com"
    database: "Operations"
    auth:
      mode: "trusted"

  archive:
    server: "sql-archive.example.com"
    database: "Archive2026"
    auth:
      mode: "trusted"

defaults:
  batch_size: 100000
  stop_on_failure: false
  create_indexes: true
  copy_mode: "full"
  nolock: true

jobs:
  - name: CopyCustomers
    source_connection: prod
    destination_connection: archive
    source_table: dbo.Customers
    destination_table: dbo.Customers

  - name: CopyOrders
    source_connection: prod
    destination_connection: archive
    source_table: dbo.Orders
    destination_table: dbo.Orders
    options:
      stop_on_failure: true     # stop everything if Orders fails

  - name: CopyOrderLines
    source_connection: prod
    destination_connection: archive
    source_table: dbo.OrderLines
    destination_table: dbo.OrderLines

  - name: SchemaOnlyProducts
    source_connection: prod
    destination_connection: archive
    source_table: dbo.Products
    destination_table: dbo.Products
    options:
      copy_mode: "schema_only"
      create_indexes: true
      create_constraints: true
```

---

## Behavior Notes

- **Identity columns** — SQLcarbon reads the exact seed and increment from the source and recreates them on the destination. `SET IDENTITY_INSERT ON/OFF` is handled automatically.
- **Computed columns** — Detected and recreated as computed columns on the destination. They are excluded from the data `INSERT` (SQL Server recalculates them automatically).
- **Partial failures** — If a batch insert fails mid-copy, SQLcarbon logs a clear `PARTIAL FAILURE` warning with the number of rows already committed. The partial data is left in place for inspection; SQLcarbon does not attempt cleanup.
- **Version compatibility** — If the source uses a data type not available on the destination (e.g., `datetime2` targeting SQL Server 2005), a warning is logged before the job runs. SQLcarbon does not attempt type transformations.

---

## CLI Reference

```
sqlcarbon --help
sqlcarbon run <config.yaml>       Run all jobs in the plan
sqlcarbon validate <config.yaml>  Validate config without touching any database
sqlcarbon init                    Print a sample plan.yaml to stdout
```

---

## License

MIT License — see `LICENSE` for details.

---

*SQLcarbon is an open-source project initially created by **TroBeeOne LLC**.*
