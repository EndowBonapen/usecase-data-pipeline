# Airflow DAG reference

**Status**: authored, not yet run — Docker Desktop isn't installed on the machine this was built on. Everything below describes what *should* happen based on the code as written; it hasn't been verified end-to-end. This runs via Docker because Airflow doesn't run natively on Windows (needs POSIX signals/fork) — Docker Desktop on Windows uses WSL2 as its engine either way, so this isn't avoiding that dependency, just standardizing on the official path.

## Running it

Requires Docker Desktop.

```powershell
cd airflow
docker-compose up
```

First run builds the image (installs both `ETL/requirements.txt` and `ELT/requirements.txt`, including `dbt-bigquery`, into `apache/airflow:2.10.4-python3.11` — one Airflow instance runs both DAGs, so both dependency sets have to coexist in the same image) and runs `airflow-init` (DB migration + creates an `admin`/`admin` user) before the webserver/scheduler start. Once up, the UI is at `localhost:8080`.

`ETL/.env`/`ETL/service-account.json` and `ELT/.env` are picked up automatically — both folders are mounted into the container (`/opt/airflow/etl`, `/opt/airflow/elt`), and `GOOGLE_APPLICATION_CREDENTIALS` is overridden in `docker-compose.yaml` to point at the in-container path (the value baked into each `.env` is a Windows host path, which wouldn't resolve inside the container). The `ecommerce_elt_phase2` DAG's `dbt_build` task also needs a dbt profile — that's `airflow/dbt_profiles/profiles.yml` (env_var()-driven, committed — separate from `ELT/dbt/profiles.yml`, which is gitignored and has literal values for local dev).

To stop: `docker-compose down`. Add `-v` to also drop the Postgres metadata volume (full reset, including DAG run history).

## DAG: `ecommerce_etl_phase1`

Schedule: daily. `max_active_runs=1` — a new run won't start while the previous one is still going.

```
extract_relational_seed → load_relational_seed ─┐
extract_transactions    → load_transactions     ─┤
extract_inventory       → load_inventory        ─┼→ verify_load
extract_returns         → load_returns          ─┘
                           load_relational_seed → build_marts → verify_load
```

## Task-by-task

| Task | What it does | Depends on | If it fails |
|---|---|---|---|
| `extract_<source>` | Calls the source's async fetch function directly (imported, not shelled out), lands each batch to `raw/{source}/{ds}/`. Returns the batch count. | — | Retries twice (5 min apart, per `default_args`). If both retries fail (e.g. API down, rate-limited past the retry budget), the task is marked failed; its `load_<source>` task never runs (default trigger rule is `all_success`). |
| `load_<source>` | Takes the extract task's batch count as an XCom input (forces ordering; the value itself isn't used — the load re-globs the day's folder rather than trusting a count). Calls `load_source_date()` directly: reads every file landed for `(source, ds)`, concatenates same-named tables across batches, runs business-rule validation, `WRITE_TRUNCATE`-loads each table. | `extract_<source>` | If validation fails (a business rule doesn't hold — see [ETL/docs/01-architecture.md](../../ETL/docs/01-architecture.md)), raises before touching BigQuery — nothing gets loaded that run. If BigQuery itself errors (quota, auth), retries twice. Downstream (`build_marts`, `verify_load`) won't run if this fails. |
| `build_marts` | Rebuilds `dim_category`, `dim_product`, `dim_customer`, `fact_orders`, `fact_order_items` from whatever's currently in `raw_ecommerce` (`CREATE OR REPLACE TABLE ... AS SELECT`). | `load_relational_seed` | If a `CREATE OR REPLACE` statement errors (e.g. a column referenced doesn't exist — this is how the earlier trailing-comma SQL bug would have surfaced), the task fails; marts are left in whatever state the *last successful* run produced (each statement is atomic, but a mid-run failure means later mart tables in the sequence won't have updated). `verify_load` catches this either way. |
| `verify_load` | Independent post-load check — queries BigQuery `COUNT(*)` directly for every raw + mart table this run should have touched. Doesn't trust any upstream task's return value, only what's actually in BigQuery right now. | all `load_*` + `build_marts` | Raises with the specific table(s) that came back empty. This is the task to check first when a run is marked failed but you're not sure which table is actually the problem — its error message lists exactly which ones. |

## Diagnosing a failed run

1. **Which task failed** — Airflow UI → the DAG → Graph view, red task box.
2. **`extract_*` failed** — check the task log for the HTTP error. If it's a 429/rate-limit that outlasted the retry budget, the run will need a manual retry (UI → task → Clear) once the API's window resets (60 requests/60s, see `extract/http_client.py`).
3. **`load_*` failed with a validation message** — the log lists which derived-field check failed and the expected vs. actual values (from `transform/clean.py`'s `validate()`). This means the *source data itself* looks inconsistent for that run — worth checking whether the API's mock data generation changed shape.
4. **`build_marts` failed** — almost certainly a SQL error in `load/marts.py`; the log has the exact BigQuery error message and line.
5. **`verify_load` failed** — lists the specific empty table(s). If a raw table is empty but its `load_*` task shows as succeeded, that's a sign the load silently wrote 0 rows (e.g. an empty source file) rather than actually failing — worth checking the corresponding `raw/{source}/{ds}/` files by hand.

## Known gaps (not yet handled)

- No alerting beyond the Airflow UI itself — a failed run doesn't page/email/Slack anyone yet. `on_failure_callback` would be the place to add that.
- No Airflow **Pool** limiting concurrent API calls — the 4 `extract_*` tasks run in parallel and could, in principle, collectively exceed the 60 req/60s rate limit faster than any single source's own retry-on-429 logic accounts for. Hasn't caused a problem in manual testing (each extract is a handful of requests), but would matter at higher `--batches` values.
- Not yet tested end-to-end (see status note at the top).

## DAG: `ecommerce_elt_phase2`

Same schedule and `max_active_runs=1` as Phase 1. Structurally simpler — no equivalent to `build_marts`/`verify_load` as separate tasks, because `dbt build` already does staging → intermediate → marts → all 34 tests as one gated unit.

```
extract_relational_seed → load_raw_relational_seed ─┐
extract_transactions    → load_raw_transactions     ─┤
extract_inventory       → load_raw_inventory        ─┼→ dbt_build
extract_returns         → load_raw_returns          ─┘
```

### Task-by-task

| Task | What it does | Depends on | If it fails |
|---|---|---|---|
| `extract_<source>` | Same as Phase 1's `extract_<source>`, but calls ELT's own copy of the fetch functions and lands to `ELT/raw/{source}/{ds}/` (a separate raw zone from `ETL/raw/`). | — | Same retry behavior as Phase 1. |
| `load_raw_<source>` | Calls `load_raw_source_date()` directly: reads every file landed for `(source, ds)`, loads each table into `elt_raw_ecommerce` **as-is** — no flatten/cast in Python. Nested objects/arrays become native BigQuery STRUCT/ARRAY columns for dbt to `UNNEST()`. | `extract_<source>` | If BigQuery load itself errors (schema autodetect issue, quota, auth), retries twice. There's no business-rule validation at this stage (contrast Phase 1) — that's dbt's job, downstream. |
| `dbt_build` | Shells out to `dbt build --project-dir .../ELT/dbt --profiles-dir .../dbt_profiles` — runs every staging/intermediate/marts model plus all 34 tests in dependency order. | every `load_raw_*` | If any model fails to build (SQL error) or any test fails (a `unique`/`relationships`/`accepted_values`/custom singular test), `dbt build`'s own exit code is non-zero, which raises in the task. The log (captured via `subprocess.run`) has dbt's own output — same as running `dbt build` locally, including which specific model or test failed. Models built *before* the failure keep their new state (dbt doesn't roll back earlier models in the same run); ones after it don't get rebuilt. |

### Diagnosing a failed run

1. **`extract_*` failed** — same as Phase 1: check for a 429/rate-limit that outlasted the retry budget.
2. **`load_raw_*` failed** — almost certainly a BigQuery load error (check the task log for the actual API error), not a data-shape problem — there's no flatten step here to get wrong.
3. **`dbt_build` failed** — the task log **is** the `dbt build` output. Scroll to the first `ERROR` or `FAIL`: a model error names the `.sql` file and the BigQuery error message; a test failure names the specific test (e.g. `relationships_fact_returns_product_id__product_id__ref_dim_product_`) — cross-reference against `ELT/dbt/models/marts/_marts.yml` or `ELT/dbt/tests/` to see what it's asserting.
