# usecase-data-pipeline

Latihan data engineering end-to-end: dua paradigma, satu data source.

- **ETL/** — Extract (Open-Meteo API) → Transform (pandas) → Load (Azure SQL Database)
- **ELT/** — Extract (Open-Meteo API) → Load raw (BigQuery) → Transform (dbt)

Kedua pipeline diorkestrasi oleh Apache Airflow (via WSL2 — Airflow tidak jalan native di Windows).

Data: cuaca harian 5 kota (Jakarta, Surabaya, Bandung, Medan, Makassar) dari [Open-Meteo](https://open-meteo.com/) (gratis, tanpa API key).

Lihat progres & panduan step-by-step di masing-masing folder.

## Use case 2 — E-commerce Data Platform (planning)

Multi-source pipeline (orders, transactions, inventory, returns) dari [randomapi.dev](https://randomapi.dev/apis), dengan star schema multi-fact. Masih tahap desain — lihat [plan-project.md](plan-project.md).
