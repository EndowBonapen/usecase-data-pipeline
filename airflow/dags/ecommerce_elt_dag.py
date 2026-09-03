"""
DAG: E-commerce ELT Phase 2
────────────────────────────
Extracts the same 4 sources as Phase 1, but lands them raw and untransformed
into BigQuery (elt_raw_ecommerce), then hands off to dbt for staging ->
intermediate (crosswalk) -> marts (elt_marts_ecommerce).

Schedule: daily.

Tasks per source (relational_seed, transactions, inventory, returns):
  1. extract_<source>  — fetch + land raw JSON for today (ELT/extract/land.py)
  2. load_raw_<source> — load every file landed today into elt_raw_ecommerce
                          as-is (ELT/load/raw.py: load_raw_source_date) —
                          nested objects/arrays become native BigQuery
                          STRUCT/ARRAY, no flatten in Python here (contrast
                          Phase 1)

Then:
  3. dbt_build — `dbt build` (staging + intermediate + marts + all 34 tests)
                 as one subprocess call. This is the one task in this DAG
                 that shells out rather than importing Python directly — dbt
                 is an external CLI tool, not our own code (contrast the ETL
                 DAG, which imports its own pipeline functions directly).

No separate verify_load task here — dbt's own tests already gate the build:
`dbt build` fails if any model or test fails, so marts only get (re)built
when everything upstream passed.

Status: authored, not run — same as the ETL DAG (see airflow/docs/01-dag-reference.md).
"""

import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import task
from airflow.models.dag import DAG

# ELT/ is mounted into the container at this path (see docker-compose.yaml)
# and both ETL's and ELT's dependencies are installed into the same image
# (Dockerfile) — one Airflow instance runs both DAGs.
ELT_DIR = Path(os.environ.get("AIRFLOW_ELT_DIR", "/opt/airflow/elt"))
if str(ELT_DIR) not in sys.path:
    sys.path.insert(0, str(ELT_DIR))

DBT_PROJECT_DIR = ELT_DIR / "dbt"
# Separate from ELT/dbt/profiles.yml (gitignored, local-dev only, literal
# values) — this one is env_var()-driven and lives at airflow/dbt_profiles/,
# committed since it has no secrets baked in.
DBT_PROFILES_DIR = Path(os.environ.get("AIRFLOW_DBT_PROFILES_DIR", "/opt/airflow/dbt_profiles"))

log = logging.getLogger(__name__)

SOURCES = ["relational_seed", "transactions", "inventory", "returns"]

default_args = {
    "owner": "endow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
}


@task
def extract_and_land(source: str) -> int:
    """Fetch one source from randomapi.dev and land it to ELT/raw/{source}/{ds}/.

    Separate copy of the same logic as the ETL DAG's extract_and_land — ELT
    has its own extract/ package, duplicated on purpose (track-project.md
    Step 2a) so ETL and ELT stay two independent pipelines, not because the
    fetch logic is actually any different.
    """
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
def load_raw(source: str, batch_count: int) -> dict[str, int]:
    """Load every file landed today for `source` into elt_raw_ecommerce, as-is.

    `batch_count` isn't read — it exists so TaskFlow wires an XCom dependency
    on the matching extract task; load_raw_source_date re-globs the day's
    folder itself rather than trusting a count.
    """
    from airflow.operators.python import get_current_context
    from google.cloud import bigquery

    from config import GCP_PROJECT_ID
    from load.raw import load_raw_source_date

    context = get_current_context()
    ds = context["ds"]

    log.info("loading %s batch(es) landed today for source=%s date=%s", batch_count, source, ds)
    client = bigquery.Client(project=GCP_PROJECT_ID)
    return load_raw_source_date(client, source, ds)


@task
def dbt_build(load_results: list[dict]) -> None:
    """Run `dbt build` — dbt is an external CLI tool, so this shells out
    (contrast every other task above, which imports Python directly).

    `load_results` isn't read — same XCom-dependency-wiring reason as
    `batch_count` above: forces this task to wait for every load_raw_* task.
    """
    from config import BQ_DATASET_RAW, GCP_PROJECT_ID

    env = {
        **os.environ,
        "GCP_PROJECT_ID": GCP_PROJECT_ID,
        "BQ_DATASET_RAW": BQ_DATASET_RAW,
    }

    result = subprocess.run(
        [
            "dbt",
            "build",
            "--project-dir", str(DBT_PROJECT_DIR),
            "--profiles-dir", str(DBT_PROFILES_DIR),
        ],
        env=env,
        capture_output=True,
        text=True,
    )
    log.info(result.stdout)
    if result.returncode != 0:
        log.error(result.stderr)
        raise RuntimeError(f"dbt build failed (exit {result.returncode})")


with DAG(
    dag_id="ecommerce_elt_phase2",
    description="randomapi.dev -> land raw -> load BigQuery elt_raw_ecommerce as-is -> dbt build",
    schedule="@daily",
    start_date=datetime(2026, 9, 3),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ecommerce", "phase2", "elt", "dbt"],
) as dag:
    load_results = []

    for source in SOURCES:
        batch_count = extract_and_land.override(task_id=f"extract_{source}")(source)
        loaded = load_raw.override(task_id=f"load_raw_{source}")(source, batch_count)
        load_results.append(loaded)

    dbt_build(load_results)
