import asyncio

import httpx
import pandas as pd
from aiolimiter import AsyncLimiter

BASE_URL = "http://127.0.0.1:8000"
TOTAL_ITEMS = 1000
MAX_CONCURRENCY = 50
CALLS_PER_SECOND = 18
MAX_RETRIES = 5
REQUEST_TIMEOUT = 5.0

CSV_FIELDS = [
    "order_id",
    "account_id",
    "company",
    "status",
    "currency",
    "subtotal",
    "tax",
    "total",
    "created_at",
]


async def fetch_order(
    client: httpx.AsyncClient,
    item_id: int,
    limiter: AsyncLimiter,
    semaphore: asyncio.Semaphore,
) -> dict | None:
    for attempt in range(1, MAX_RETRIES + 1):
        async with semaphore, limiter:
            try:
                resp = await client.get(
                    f"{BASE_URL}/item/{item_id}",
                    timeout=REQUEST_TIMEOUT,
                )

                if resp.status_code == 200:
                    data = resp.json()
                    return {field: data[field] for field in CSV_FIELDS}

                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", "1"))
                    print(
                        f"429 Rate limited on item {item_id} (attempt {attempt}/{MAX_RETRIES}), "
                        f"sleeping {retry_after}s"
                    )
                    await asyncio.sleep(retry_after)
                    continue

                if 500 <= resp.status_code < 600:
                    print(
                        f"{resp.status_code} Server error on item {item_id} (attempt {attempt}/{MAX_RETRIES}), "
                        "retrying in 1s"
                    )
                    await asyncio.sleep(1)
                    continue

                print(f"Non-retryable {resp.status_code} error on item {item_id}, giving up")
                return None

            except (httpx.TimeoutException, httpx.TransportError) as exc:
                print(f"Transport error on item {item_id} (attempt {attempt}/{MAX_RETRIES}): {exc}, retrying in 1s")
                await asyncio.sleep(1)
                continue

    print(f"Exhausted retries for item {item_id}")
    return None


async def main() -> None:
    output_file = "items_async.csv"
    limiter = AsyncLimiter(CALLS_PER_SECOND, 1)
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

    print(
        f"Starting async client: fetching {TOTAL_ITEMS} items "
        f"(concurrency={MAX_CONCURRENCY}, rate={CALLS_PER_SECOND} req/s)"
    )

    async with httpx.AsyncClient() as client:
        tasks = [fetch_order(client, i, limiter, semaphore) for i in range(1, TOTAL_ITEMS + 1)]
        results = await asyncio.gather(*tasks)

    rows = [r for r in results if r is not None]
    df = pd.DataFrame(rows, columns=CSV_FIELDS)
    df = df.sort_values("order_id")
    df.to_csv(output_file, index=False)

    print(f"Done. Wrote {len(rows)}/{TOTAL_ITEMS} rows to {output_file}")


if __name__ == "__main__":
    asyncio.run(main())
