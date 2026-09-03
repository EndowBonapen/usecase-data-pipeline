import asyncio

import httpx

from config import RANDOMAPI_BASE_URL
from extract.http_client import get_with_retry

RETURNS_ENDPOINT = f"{RANDOMAPI_BASE_URL}/returns"

# 'count' must be between 1 and 100 (verified against the live API, 2026-08-27).
MAX_COUNT_PER_CALL = 100
# X-RateLimit-Limit: 60 requests / 60s window (verified via response headers).
BATCH_PACE_SECONDS = 1.2


async def fetch_returns(
    count: int = MAX_COUNT_PER_CALL,
    currency: str = "USD",
    date_from: str | None = None,
    date_to: str | None = None,
    min_items: int | None = None,
    max_items: int | None = None,
) -> dict:
    params = {"count": count, "currency": currency}
    if date_from is not None:
        params["from"] = date_from
    if date_to is not None:
        params["to"] = date_to
    if min_items is not None:
        params["minItems"] = min_items
    if max_items is not None:
        params["maxItems"] = max_items

    async with httpx.AsyncClient(timeout=30) as client:
        return await get_with_retry(client, RETURNS_ENDPOINT, params)


async def fetch_returns_batches(
    batches: int = 1,
    count: int = MAX_COUNT_PER_CALL,
    currency: str = "USD",
    date_from: str | None = None,
    date_to: str | None = None,
    min_items: int | None = None,
    max_items: int | None = None,
) -> list[dict]:
    results = []
    for i in range(batches):
        payload = await fetch_returns(
            count=count,
            currency=currency,
            date_from=date_from,
            date_to=date_to,
            min_items=min_items,
            max_items=max_items,
        )
        results.append(payload)
        print(f"Batch {i + 1}/{batches} done")

        is_last_batch = i == batches - 1
        if not is_last_batch:
            await asyncio.sleep(BATCH_PACE_SECONDS)

    return results
