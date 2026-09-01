"""
DAG: E-commerce ETL Phase 1
───────────────────────────
Extracts 4 sources from randomapi.dev, lands raw JSON, loads into BigQuery
(raw_ecommerce), builds the Phase 1 marts (marts_ecommerce), then runs a
post-load smoke test against BigQuery itself.

Schedule: daily.

Tasks per source (relational_seed, transactions, inventory, returns):
  1. extract_<source> — fetch + land raw JSON for today (extract/land.py)
  2. load_<source>    — load every file landed today into raw_ecommerce
                         (load/pipeline.py: load_source_date)

Then:
  3. build_marts — dim_category/dim_product/dim_customer/fact_orders/
                    fact_order_items from raw_ecommerce (relational_seed
                    only — the other 3 sources need identity crosswalk,
                    deferred to the dbt/Phase 2 rewrite, see plan-project.md §3)
  4. verify_load — re-queries BigQuery (COUNT(*), not trusting load job
                    metadata) for every table this run should have touched.
                    This is the same check that caught the WRITE_APPEND
                    duplicate-row bug during manual testing — now automated
                    instead of relying on someone noticing.

Business-rule validation (order totals, inventory reconciliation, etc.)
already happens inside load_source_date() itself — it raises and aborts
the load if a derived-field check fails (see transform/clean.py).
verify_load is a different, complementary check: did the load actually
land the rows it claims to, in the actual table.
"""

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import task
from airflow.models.dag import DAG

# ETL/ is mounted into the container at this path (see docker-compose.yaml)
# and its dependencies are installed straight into the image (Dockerfile).
# Env var override + fallback so this also resolves if ever run outside the
# container with ETL/ checked out one level up.
ETL_DIR = Path(os.environ.get("AIRFLOW_ETL_DIR", "/opt/airflow/etl"))
if str(ETL_DIR) not in sys.path:
    sys.path.insert(0, str(ETL_DIR))

log = logging.getLogger(__name__)

SOURCES = ["relational_seed", "transactions", "inventory", "returns"]

# Tables this run should have touched, per source — used by verify_load.
# Only "always present" tables are listed; child tables (locations, items,
# events, incoming) are batch-dependent and not guaranteed every run.
EXPECTED_RAW_TABLES = {
    "relational_seed": ["categories", "products", "customers", "orders", "order_items"],
    "transactions": ["transactions"],
    "inventory": ["inventory"],
    "returns": ["returns"],
}
EXPECTED_MART_TABLES = ["dim_category", "dim_product", "dim_customer", "fact_orders", "fact_order_items"]

default_args = {
    "owner": "endow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
}


@task
def extract_and_land(source: str) -> int:
    """Fetch one source from randomapi.dev and land it to raw/{source}/{ds}/."""
    import asyncio

    from extract.inventory import fetch_inventory_batches
    from extract.land import land_raw
    from extract.relational_seed import fetch_relational_seed_batches
    from extract.returns import fetch_returns_batches
    from extract.transactions import fetch_transactions_batches

    fetch_by_source = {
        "relational_seed": fetch_relational_seed_batches,
        "transactions": fetch_transactions_batches,
        "inventory": fetch_inventory_batches,
        "returns": fetch_returns_batches,
    }

    results = asyncio.run(fetch_by_source[source]())
    for batch_index, payload in enumerate(results, start=1):
        file_path = land_raw(source, batch_index, payload)
        log.info("landed %s -> %s", source, file_path)

    return len(results)


@task
def load_source(source: str, batch_count: int) -> dict[str, int]:
    """Load every file landed today for `source` into BigQuery raw_ecommerce."""
    from airflow.operators.python import get_current_context
    from google.cloud import bigquery

    from config import GCP_PROJECT_ID
    from load.pipeline import load_source_date

    context = get_current_context()
    ds = context["ds"]

    log.info("loading %s batch(es) landed today for source=%s date=%s", batch_count, source, ds)
    client = bigquery.Client(project=GCP_PROJECT_ID)
    return load_source_date(client, source, ds)


@task
def build_marts_task(relational_seed_load: dict[str, int]) -> dict[str, int]:
    """Build the Phase 1 marts from the freshly loaded relational_seed tables.

    `relational_seed_load` isn't read here — it exists so TaskFlow wires an
    XCom dependency, forcing this task to wait for load_relational_seed.
    """
    from google.cloud import bigquery

    from config import GCP_PROJECT_ID
    from load.marts import build_marts

    client = bigquery.Client(project=GCP_PROJECT_ID)
    return build_marts(client)


@task
def verify_load(load_results: list[dict], marts_result: dict) -> None:
    """Post-load smoke test: query BigQuery directly (not job metadata) and
    confirm every table this run should have touched actually has rows.

    This is deliberately independent of the pre-load validation in
    transform/clean.py — that checks business-rule correctness on the data
    before it's loaded; this checks the load itself actually landed data,
    the same way a WRITE_APPEND-instead-of-TRUNCATE bug was caught by hand
    during manual testing (see track-project.md Step 9).

    `load_results`/`marts_result` aren't read — same XCom-dependency-wiring
    reason as build_marts_task above. The actual check re-queries BigQuery
    directly rather than trusting what upstream tasks reported.
    """
    from google.cloud import bigquery

    from config import BQ_DATASET_MARTS, BQ_DATASET_RAW, GCP_PROJECT_ID

    client = bigquery.Client(project=GCP_PROJECT_ID)
    problems = []

    for tables in EXPECTED_RAW_TABLES.values():
        for table in tables:
            full_name = f"{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.{table}"
            count = list(client.query(f"SELECT COUNT(*) AS n FROM `{full_name}`").result())[0].n
            if count == 0:
                problems.append(f"{full_name}: 0 rows")

    for table in EXPECTED_MART_TABLES:
        full_name = f"{GCP_PROJECT_ID}.{BQ_DATASET_MARTS}.{table}"
        count = list(client.query(f"SELECT COUNT(*) AS n FROM `{full_name}`").result())[0].n
        if count == 0:
            problems.append(f"{full_name}: 0 rows")

    if problems:
        raise ValueError("Post-load smoke test failed:\n" + "\n".join(problems))

    log.info(
        "Post-load smoke test passed (%d raw tables, %d mart tables).",
        sum(len(t) for t in EXPECTED_RAW_TABLES.values()),
        len(EXPECTED_MART_TABLES),
    )


with DAG(
    dag_id="ecommerce_etl_phase1",
    description="randomapi.dev -> land raw -> load BigQuery raw_ecommerce -> Phase 1 marts -> verify",
    schedule="@daily",
    start_date=datetime(2026, 8, 28),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ecommerce", "phase1", "etl"],
) as dag:
    load_results = {}

    for source in SOURCES:
        batch_count = extract_and_land.override(task_id=f"extract_{source}")(source)
        load_results[source] = load_source.override(task_id=f"load_{source}")(source, batch_count)

    marts_result = build_marts_task(load_results["relational_seed"])

    verify_load(
        load_results=[load_results[s] for s in SOURCES],
        marts_result=marts_result,
    )
