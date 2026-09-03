import asyncio

import httpx

from config import RANDOMAPI_BASE_URL
from extract.http_client import get_with_retry

INVENTORY_ENDPOINT = f"{RANDOMAPI_BASE_URL}/inventory"

# 'count' must be between 1 and 100 (verified against the live API, 2026-08-27).
MAX_COUNT_PER_CALL = 100
# X-RateLimit-Limit: 60 requests / 60s window (verified via response headers).
BATCH_PACE_SECONDS = 1.2


async def fetch_inventory(
    count: int = MAX_COUNT_PER_CALL,
    min_available: int | None = None,
    location_count: int | None = None,
    include_incoming: bool | None = None,
    as_of: str | None = None,
) -> dict:
    params = {"count": count}
    if min_available is not None:
        params["minAvailable"] = min_available
    if location_count is not None:
        params["locationCount"] = location_count
    if include_incoming is not None:
        params["includeIncoming"] = str(include_incoming).lower()
    if as_of is not None:
        params["asOf"] = as_of

    async with httpx.AsyncClient(timeout=30) as client:
        return await get_with_retry(client, INVENTORY_ENDPOINT, params)


async def fetch_inventory_batches(
    batches: int = 1,
    count: int = MAX_COUNT_PER_CALL,
    min_available: int | None = None,
    location_count: int | None = None,
    include_incoming: bool | None = None,
    as_of: str | None = None,
) -> list[dict]:
    results = []
    for i in range(batches):
        payload = await fetch_inventory(
            count=count,
            min_available=min_available,
            location_count=location_count,
            include_incoming=include_incoming,
            as_of=as_of,
        )
        results.append(payload)
        print(f"Batch {i + 1}/{batches} done")

        is_last_batch = i == batches - 1
        if not is_last_batch:
            await asyncio.sleep(BATCH_PACE_SECONDS)

    return results
