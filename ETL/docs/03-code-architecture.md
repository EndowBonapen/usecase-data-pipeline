# ETL code architecture

How this folder is put together and why. For *what* the pipeline builds and *why it exists*, see [01-architecture.md](01-architecture.md) — this doc is about the code itself.

## Folder structure

```
ETL/
├── cli.py            # Typer app — the only entrypoint you run directly
├── config.py          # shared .env-backed config
├── extract/            # one module per source, all async (httpx)
│   ├── http_client.py  # shared retry-on-429 GET, used by every source
│   ├── land.py          # writes a fetched batch to raw/{source}/{date}/*.json
│   ├── relational_seed.py
│   ├── transactions.py
│   ├── inventory.py
│   └── returns.py
├── transform/
│   ├── flatten.py      # nested JSON -> pyarrow.Table per entity
│   └── clean.py        # money casting (Decimal, not float) + derived-field validation
├── load/
│   ├── bigquery.py     # load_table() — one pyarrow.Table -> one BigQuery table, via Parquet
│   ├── marts.py        # build_marts() — SQL CREATE OR REPLACE, relational_seed only
│   └── pipeline.py      # ties flatten+validate+load together; load_source_date() for a whole day
├── scripts/             # one-off / infra, not part of the recurring pipeline
│   ├── setup_datasets.py
│   └── test_connection.py
├── raw/                  # landed JSON, gitignored, partitioned by source/date
└── docs/                 # this folder
```

## Why extract/transform/load are separate packages

Each one maps to a distinct pipeline stage (see [01-architecture.md](01-architecture.md)'s bronze/silver/gold breakdown) and, not coincidentally, to a distinct Airflow task type. `extract_*` tasks only touch `extract/`; `load_*` tasks import from `transform/` and `load/`. Keeping them as separate importable packages (rather than one big script) is what let the Airflow DAG import the actual pipeline functions directly instead of shelling out to the CLI.

## Why async (httpx) instead of requests

There are 4 independent sources to pull from. `httpx.AsyncClient` + `asyncio.gather` (in the batch fetch functions) lets multiple calls run concurrently instead of sequentially — meaningful once `--batches` is used for volume. `http_client.py`'s retry-on-429 lives at this layer, not in Airflow — it's a per-request concern, not a per-task one (Airflow's own `retries` in `default_args` is a separate, coarser safety net for failures retry-on-429 doesn't cover: network drops, BigQuery quota, etc.).

## Why PyArrow instead of pandas

Explicit, typed columnar tables with a direct path to Parquet (`load/bigquery.py` writes Parquet to an in-memory buffer, not JSON) — BigQuery reads the schema Parquet already declares rather than autodetecting from JSON. Avoids pandas as a dependency entirely; not needed for this data volume.

## Why money is cast via `Decimal`, not `float`

The API returns money as integer minor units (cents) specifically to avoid floating-point rounding error. Converting `_minor` → `_amount` by dividing a `float` by 100 would reintroduce exactly the error the source format exists to prevent — division of two `float`s doesn't always land on an exact decimal. `clean.py` does `Decimal(value) / 100` per row instead, which is exact, then wraps the result as `pyarrow.decimal128`.

## Why `load bigquery-all <source> <date>` exists alongside `load bigquery <file>`

`load bigquery <file>` needs an exact filename — fine for manual use, but Airflow only knows the execution date, not the timestamped filename an extract task happened to generate (and a day can have more than one batch file). `load_source_date()` globs the day's folder, concatenates same-named tables across every batch found, and does one `WRITE_TRUNCATE` load per table — so loading twice, or loading a day with 3 batches vs. 1, both converge to the same correct end state instead of duplicating rows.

## Why `WRITE_TRUNCATE`, not `WRITE_APPEND`

There's no incremental/upsert logic yet (that's an explicit later exercise — see [01-architecture.md](01-architecture.md)). Without it, `WRITE_APPEND` silently duplicates rows on every rerun — which is exactly what happened during manual testing before this default was fixed. Full-refresh-per-run is the correct default until incremental loading is actually built, not just a temporary workaround.

## Why `scripts/` is separate from `extract/transform/load`

Different lifecycle: `setup_datasets.py` and `test_connection.py` run once or rarely (infra bootstrap, diagnostics), not on every pipeline execution. Mixing them into the recurring pipeline packages would make it unclear which code Airflow actually calls on a schedule.
