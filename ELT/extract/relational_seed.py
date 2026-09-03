import asyncio

import httpx

from config import RANDOMAPI_BASE_URL
from extract.http_client import get_with_retry

RELATIONAL_SEED_ENDPOINT = f"{RANDOMAPI_BASE_URL}/relational-seed"

# Per-field ceilings enforced by the API — necessary but NOT sufficient on their
# own, see MAX_ROWS_PER_BUNDLE below (verified against the live API, 2026-08-27).
MAX_CUSTOMERS_PER_CALL = 50
MAX_ORDERS_PER_CUSTOMER = 5
MAX_ITEMS_PER_ORDER = 6

# categories(4) + products(24) — fixed regardless of customers/orders params.
FIXED_CATALOG_ROWS = 28
# The combined cap across all 5 tables in one bundle (categories, products,
# customers, orders, order_items). This is the binding constraint in practice.
MAX_ROWS_PER_BUNDLE = 400

# X-RateLimit-Limit: 60 requests / 60s window (verified via response headers).
BATCH_PACE_SECONDS = 1.2


def estimate_bundle_rows(customers: int, orders_per_customer: int, items_per_order: int) -> int:
    orders = customers * orders_per_customer
    order_items = orders * items_per_order
    return FIXED_CATALOG_ROWS + customers + orders + order_items


async def fetch_relational_seed(
    schema: str = "ecommerce",
    customers: int = 20,
    orders_per_customer: int = 3,
    items_per_order: int = 3,
    id_style: str = "uuid",
    currency: str = "USD",
    seed: int | None = None,
) -> dict:
    estimated_rows = estimate_bundle_rows(customers, orders_per_customer, items_per_order)
    if estimated_rows > MAX_ROWS_PER_BUNDLE:
        raise ValueError(
            f"customers={customers}, ordersPerCustomer={orders_per_customer}, "
            f"itemsPerOrder={items_per_order} would generate ~{estimated_rows} rows, "
            f"above the {MAX_ROWS_PER_BUNDLE}-row cap per bundle. Lower one of these, "
            "or use more --batches with smaller values instead."
        )

    params = {
        "schema": schema,
        "customers": customers,
        "ordersPerCustomer": orders_per_customer,
        "itemsPerOrder": items_per_order,
        "idStyle": id_style,
        "currency": currency,
    }
    if seed is not None:
        params["seed"] = seed

    async with httpx.AsyncClient(timeout=30) as client:
        return await get_with_retry(client, RELATIONAL_SEED_ENDPOINT, params)


async def fetch_relational_seed_batches(
    batches: int = 1,
    schema: str = "ecommerce",
    customers: int = 20,
    orders_per_customer: int = 3,
    items_per_order: int = 3,
    id_style: str = "uuid",
    currency: str = "USD",
    seed_start: int | None = None,
) -> list[dict]:
    """Fetch multiple batches sequentially, paced to stay under the API rate limit.

    Each batch is a separate call — this is how we build a bigger practice
    dataset out of a generator API that caps each single request at 400 rows.
    """
    results = []
    for i in range(batches):
        batch_seed = seed_start + i if seed_start is not None else None
        payload = await fetch_relational_seed(
            schema=schema,
            customers=customers,
            orders_per_customer=orders_per_customer,
            items_per_order=items_per_order,
            id_style=id_style,
            currency=currency,
            seed=batch_seed,
        )
        results.append(payload)
        print(f"Batch {i + 1}/{batches} done (seed={batch_seed})")

        is_last_batch = i == batches - 1
        if not is_last_batch:
            await asyncio.sleep(BATCH_PACE_SECONDS)

    return results
