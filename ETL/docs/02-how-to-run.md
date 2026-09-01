# How to run — ETL (Phase 1)

Manual, step-by-step. Everything here can also be run inside the Airflow DAG (see [airflow/docs/01-dag-reference.md](../../airflow/docs/01-dag-reference.md)) — this doc is for running it directly, e.g. while developing.

## Prerequisites

- Python 3.10+ (this project was built on 3.14.2)
- A Google Cloud project on **BigQuery Sandbox** (free, no billing account, no credit card — just sign in at [console.cloud.google.com/bigquery](https://console.cloud.google.com/bigquery))
- A service account with **BigQuery Data Editor** + **BigQuery Job User** roles granted at the **project IAM level** (not just on the service account itself — under IAM & Admin → IAM → Grant Access, not the Service Accounts page)

## 1. Setup

```powershell
cd ETL
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

Copy `.env.example` to `.env` and fill in your values:

```
GCP_PROJECT_ID=your-bigquery-sandbox-project-id
BQ_DATASET_RAW=raw_ecommerce
BQ_DATASET_MARTS=marts_ecommerce
GOOGLE_APPLICATION_CREDENTIALS=d:\path\to\ETL\service-account.json
RANDOMAPI_BASE_URL=https://randomapi.dev/api
```

Download the service account's JSON key (IAM & Admin → Service Accounts → your account → Keys → Add Key → JSON) and save it as `service-account.json` inside `ETL/` (already gitignored).

Verify the connection:

```powershell
python -m scripts.test_connection
```

## 2. Create the BigQuery datasets

```powershell
python -m scripts.setup_datasets
```

Idempotent — safe to rerun. Creates `raw_ecommerce` and `marts_ecommerce` if they don't already exist.

## 3. Run the pipeline

### Extract + land (writes to `raw/{source}/{date}/*.json`)

```powershell
python cli.py extract relational-seed
python cli.py extract transactions
python cli.py extract inventory
python cli.py extract returns
```

Useful flags: `--batches N` (fetch N calls, paced under the API's rate limit), `--seed-start N` (reproducible + distinct data per batch), `--count N` (transactions/inventory/returns, 1–100 per call).

### Inspect before loading (optional — sanity check)

```powershell
python cli.py transform flatten raw/relational_seed/2026-08-27/relational_seed_090727_001.json
python cli.py transform clean raw/relational_seed/2026-08-27/relational_seed_090727_001.json
```

`transform clean` also reports whether the derived-field checks (order totals, inventory `available`, refund estimate) pass — see [01-architecture.md](01-architecture.md) for what's actually checked.

### Load into BigQuery

```powershell
python cli.py load bigquery-all relational_seed 2026-08-27
python cli.py load bigquery-all transactions 2026-08-27
python cli.py load bigquery-all inventory 2026-08-27
python cli.py load bigquery-all returns 2026-08-27
```

`load bigquery-all <source> <date>` combines every batch landed that day per table and does one `WRITE_TRUNCATE` load — safe to rerun, won't duplicate rows. (`load bigquery <file>` also exists for loading one specific file by path, mainly for manual debugging.)

### Build the marts

```powershell
python cli.py load marts
```

Builds `dim_category`, `dim_product`, `dim_customer`, `fact_orders`, `fact_order_items` in `marts_ecommerce` from the `relational_seed` tables in `raw_ecommerce`. Scoped to that source only — see [01-architecture.md](01-architecture.md) for why.

## CLI reference

| Command | What it does |
|---|---|
| `extract relational-seed [--customers N] [--orders-per-customer N] [--items-per-order N] [--batches N] [--seed-start N]` | Fetch the master spine (categories/products/customers/orders/order_items) |
| `extract transactions [--count N] [--batches N]` | Fetch payment/financial events |
| `extract inventory [--count N] [--batches N]` | Fetch stock snapshots |
| `extract returns [--count N] [--batches N]` | Fetch return/RMA records |
| `transform flatten <file>` | Preview the flattened Arrow tables for a landed file |
| `transform clean <file>` | Preview + run derived-field validation for a landed file |
| `load bigquery <file>` | Load one landed file into `raw_ecommerce` |
| `load bigquery-all <source> <date>` | Load every file landed for a source on a given date |
| `load marts` | Build the Phase 1 marts from `raw_ecommerce` |

Every command supports `--help` for the full option list (Typer auto-generates it).
