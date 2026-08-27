import asyncio

import httpx

from config import RANDOMAPI_BASE_URL
from extract.http_client import get_with_retry

TRANSACTIONS_ENDPOINT = f"{RANDOMAPI_BASE_URL}/transactions"

# 'count' must be between 1 and 100 (verified against the live API, 2026-08-27).
MAX_COUNT_PER_CALL = 100
# X-RateLimit-Limit: 60 requests / 60s window (verified via response headers).
BATCH_PACE_SECONDS = 1.2


async def fetch_transactions(
    count: int = MAX_COUNT_PER_CALL,
    min_amount: float | None = None,
    max_amount: float | None = None,
    currency: str = "USD",
    date_from: str | None = None,
    date_to: str | None = None,
) -> dict:
    params = {"count": count, "currency": currency}
    if min_amount is not None:
        params["minAmount"] = min_amount
    if max_amount is not None:
        params["maxAmount"] = max_amount
    if date_from is not None:
        params["from"] = date_from
    if date_to is not None:
        params["to"] = date_to

    async with httpx.AsyncClient(timeout=30) as client:
        return await get_with_retry(client, TRANSACTIONS_ENDPOINT, params)


async def fetch_transactions_batches(
    batches: int = 1,
    count: int = MAX_COUNT_PER_CALL,
    min_amount: float | None = None,
    max_amount: float | None = None,
    currency: str = "USD",
    date_from: str | None = None,
    date_to: str | None = None,
) -> list[dict]:
    results = []
    for i in range(batches):
        payload = await fetch_transactions(
            count=count,
            min_amount=min_amount,
            max_amount=max_amount,
            currency=currency,
            date_from=date_from,
            date_to=date_to,
        )
        results.append(payload)
        print(f"Batch {i + 1}/{batches} done")

        is_last_batch = i == batches - 1
        if not is_last_batch:
            await asyncio.sleep(BATCH_PACE_SECONDS)

    return results
