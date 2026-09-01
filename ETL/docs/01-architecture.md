# Architecture

## What this is

An e-commerce data platform built from [randomapi.dev](https://randomapi.dev/apis) — a free, no-auth mock data API. The scenario: a Data Engineer at an e-commerce company needs to unify several operational systems (order management, payments, warehouse, returns) into one analytics platform. This is deliberately more complex than "hit one API, dump to a table" — multiple sources that need to be reconciled is the part that actually resembles real DE work.

## Data sources

| Source | Role | Notes |
|---|---|---|
| `/api/relational-seed` (schema=ecommerce) | Master spine — categories, products, customers, orders, order_items | The *only* source with API-guaranteed foreign keys (see below) |
| `/api/transactions` | Payment/financial events | IDs independent of everything else |
| `/api/inventory` | Stock snapshot per SKU/warehouse | `product` object independently generated, not linked to `/api/products` |
| `/api/returns` | Return/RMA lifecycle | `orderId` is order-*shaped*, not confirmed to reference a real order from a separate call |

## Why this isn't one shared database

This is a mock generator, not a backing database — each request generates fresh data. Only `/api/relational-seed` guarantees consistent foreign keys, and only *within a single call* (`categories → products`, `customers → orders → order_items`). The other three sources are self-consistent internally (e.g. an order's line items add up correctly) but don't share an entity pool with each other or with relational-seed.

Rather than pretend otherwise, this is treated as a **Master Data Management** problem — the same kind of identity-resolution challenge real companies hit when integrating systems from different vendors that don't share primary keys. Phase 2 (dbt) builds an explicit identity crosswalk layer for this; Phase 1 sidesteps it by scoping marts to `relational_seed` only, where the FKs are real.

## Two phases, two paradigms

**Phase 1 — ETL** (built): transform happens in Python *before* the data reaches BigQuery.
```
randomapi.dev → extract (async, httpx) → land raw JSON → flatten/clean (PyArrow)
             → load to BigQuery raw_ecommerce → SQL → marts_ecommerce (dim/fact)
```

**Phase 2 — ELT** (planned): transform happens *inside* the warehouse via dbt.
```
randomapi.dev → extract → load raw JSON as-is → BigQuery raw dataset
             → dbt: staging → intermediate (crosswalk) → marts
```

Same source, two approaches, deliberately — the point is to demonstrate both and be able to compare them (execution time, maintainability, ease of reprocessing), not just pick one.

## Pipeline stages (bronze → silver → gold)

| Stage | What | Where |
|---|---|---|
| Bronze | Raw JSON, untouched, immutable, partitioned by ingestion date | `raw/{source}/{date}/*.json` |
| Silver | Flattened, type-cast, validated | `BigQuery.raw_ecommerce` (Phase 1) — the name is a slight misnomer, see below |
| Gold | Dimensional model, query-ready | `BigQuery.marts_ecommerce` |

**Naming note**: in Phase 1, `raw_ecommerce` actually holds *cleaned* data (transform already happened in Python before loading) — not raw JSON. This is a known inconsistency, kept as-is deliberately rather than reworked mid-build. Phase 2 will use its own freshly named datasets (`elt_raw_ecommerce`, `elt_marts_ecommerce`) where "raw" means raw JSON for real.

Landing raw JSON first (bronze) matters more here than usual: the source is a *random generator*, not a stable dataset. If the raw snapshot isn't kept, there's no way to reprocess it later with the same values — a bug in the transform logic would mean permanently different data on retry, not just a delay.

## Dimensional model

Phase 1 builds a simplified star schema, scoped to `relational_seed` only:

```
dim_category ← dim_product ← fact_order_items → dim_customer
                                     ↓
                                fact_orders
```

Known simplifications (intentionally deferred to Phase 2, where dbt tests can verify a more complete model):
- `dim_product → dim_category` is a **snowflake** edge (FK to another dimension), not a flat star — a pure star would have `category_name` denormalized directly into `dim_product`.
- No `dim_date` yet — `fact_orders.placed_at` is a raw timestamp, not a `date_key` foreign key.
- Only 2 of the 5 planned fact tables exist (`fact_orders`, `fact_order_items`). `fact_transactions`, `fact_returns`, `fact_inventory_snapshot` need the identity crosswalk mentioned above.

## Data quality

Two independent layers, checking different things:

1. **Business-rule validation** (`transform/clean.py`, runs before every load) — re-derives values from the data itself and compares: `order_items.line_total` vs `quantity × unit_price`, `orders.total` vs the sum of its line items, `inventory.available` vs `onHand - reserved`, `returns.refund_estimated` vs `subtotal - restockingFee`. A mismatch aborts the load — bad data never reaches BigQuery.
2. **Post-load smoke test** (Airflow's `verify_load` task) — queries BigQuery directly after loading and confirms every table that should have rows actually does. This is a different failure mode: it catches load-mechanics bugs (like a `WRITE_APPEND`-instead-of-`WRITE_TRUNCATE` duplicate-row bug hit during manual testing) that business-rule validation wouldn't, since the source data was correct — the *loading* was the problem.

## What KPIs this enables

Sales (revenue, AOV), Customer (active/new/repeat, order frequency), Product (top/bottom sellers, revenue by category), Operations (fulfillment/cancellation rate), Inventory (stock value, reorder rate), Payment (success/failure rate, refund volume) — full list of source-to-KPI mapping intentions predates the current build and lives in the (gitignored, working) `plan-project.md`.
