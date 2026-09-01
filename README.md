# usecase-data-pipeline

Trying out ETL and ELT for real ☕

## E-commerce data platform

A multi-source pipeline built from [randomapi.dev](https://randomapi.dev/apis) (relational-seed, transactions, inventory, returns), landed into BigQuery and modeled as a star schema.

**Phase 1 (ETL)** is done: async extract → land raw JSON → flatten/clean/cast in Python → load into BigQuery → build the Phase 1 marts (dim/fact). Orchestrated by an Airflow DAG via docker-compose.

**Phase 2 (ELT)**, where dbt takes over the transform step inside the warehouse, hasn't started yet.

Docs:
- [ETL/docs/01-architecture.md](ETL/docs/01-architecture.md) — data sources, why multi-source is harder than it looks, dimensional model
- [ETL/docs/02-how-to-run.md](ETL/docs/02-how-to-run.md) — setup and how to run it
- [ETL/docs/03-code-architecture.md](ETL/docs/03-code-architecture.md) — why the code is structured the way it is
- [airflow/docs/01-dag-reference.md](airflow/docs/01-dag-reference.md) — how to run the DAG, what each task does, what happens if one fails
