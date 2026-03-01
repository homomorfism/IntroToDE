# Orders API Client

## Setup

```bash
uv sync          # install all dependencies (server + client)
uv run orders_server   # start the server on http://127.0.0.1:8000
```

## Running client

For running `async` client run: `uv run python client_async.py`

## Client information

For implementation I have used:
- `httpx.AsyncClient` for async http requests
- `aiolimiter.AsyncLimiter(18, 1)` for RPS limit
- `asyncio.Semaphore(50)` for controlling concurrency

## Output

Output file is saved at `items_async.csv` file. All information was scraped successfully:

```python
In [1]: import pandas as pd
df = 
In [2]: df = pd.read_csv("items_async.csv")

In [3]: len(df)
Out[3]: 1000

In [4]: len(df.order_id.unique())
Out[4]: 1000

In [5]: 
```


## Logs


```text
> uv run python client_async.py 
Starting async client: fetching 1000 items (concurrency=50, rate=18 req/s)
500 Server error on item 2 (attempt 1/5), retrying in 1s
500 Server error on item 25 (attempt 1/5), retrying in 1s
500 Server error on item 26 (attempt 1/5), retrying in 1s
500 Server error on item 29 (attempt 1/5), retrying in 1s
500 Server error on item 47 (attempt 1/5), retrying in 1s
...
500 Server error on item 279 (attempt 2/5), retrying in 1s
500 Server error on item 284 (attempt 2/5), retrying in 1s
500 Server error on item 386 (attempt 2/5), retrying in 1s
500 Server error on item 519 (attempt 2/5), retrying in 1s
500 Server error on item 557 (attempt 2/5), retrying in 1s
500 Server error on item 806 (attempt 2/5), retrying in 1s
500 Server error on item 908 (attempt 2/5), retrying in 1s
500 Server error on item 963 (attempt 2/5), retrying in 1s
500 Server error on item 983 (attempt 2/5), retrying in 1s
500 Server error on item 254 (attempt 3/5), retrying in 1s
500 Server error on item 284 (attempt 3/5), retrying in 1s
500 Server error on item 908 (attempt 3/5), retrying in 1s
Done. Wrote 1000/1000 rows to items_async.csv
````