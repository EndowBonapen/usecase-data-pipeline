import asyncio

import httpx

MAX_RETRIES = 3


async def get_with_retry(client: httpx.AsyncClient, url: str, params: dict) -> dict:
    """GET with automatic retry-on-429, honoring the API's Retry-After header."""
    for attempt in range(1, MAX_RETRIES + 1):
        response = await client.get(url, params=params)

        if response.status_code == 429:
            retry_after = float(response.headers.get("Retry-After", 2 * attempt))
            print(f"Rate limited (attempt {attempt}/{MAX_RETRIES}), waiting {retry_after}s")
            await asyncio.sleep(retry_after)
            continue

        response.raise_for_status()
        return response.json()

    raise RuntimeError(f"Still rate limited after {MAX_RETRIES} retries")
