# API reference

Two services, two base URLs:

- ingestion: `http://localhost:8080/api/v1/events`
- analytics: `http://localhost:8082/api/v1/analytics`

Both have `GET /health` (returns `OK!`) and `GET /metrics` at the root for Prometheus.

## Sending events

`POST /api/v1/events/{click|view|purchase}` with a JSON body. On success you get `200 {"status":"ok"}`.

Every event carries `user_id`, `product_id`, `session_id` (UUIDs) and `timestamp` (RFC 3339). You can also pass your own `event_id`; if you don't, the server generates one. Sending the same `event_id` twice does not make the second request fail. Both copies go through Kafka, and ClickHouse merges them into one row later (see [data-model.md](data-model.md#where-duplicates-come-from)).

A click adds where the product was clicked:

```bash
curl -X POST localhost:8080/api/v1/events/click \
  -H 'content-type: application/json' \
  -d '{
    "user_id": "11111111-1111-1111-1111-111111111111",
    "product_id": "22222222-2222-2222-2222-222222222222",
    "session_id": "33333333-3333-3333-3333-333333333333",
    "timestamp": "2026-07-06T14:00:00Z",
    "metadata": {"source": "search", "position": 3, "category": "electronics"}
  }'
```

A view adds how long the product was on screen, and optionally where the user came from:

```bash
curl -X POST localhost:8080/api/v1/events/view \
  -H 'content-type: application/json' \
  -d '{
    "user_id": "11111111-1111-1111-1111-111111111111",
    "product_id": "22222222-2222-2222-2222-222222222222",
    "session_id": "33333333-3333-3333-3333-333333333333",
    "timestamp": "2026-07-06T14:00:00Z",
    "duration_ms": 4500,
    "referrer": "https://example.com/search"
  }'
```

A purchase adds the order. Prices are integers in cents, and `payment_method` is one of `card`, `paypal`, `apple_pay`, `google_pay`:

```bash
curl -X POST localhost:8080/api/v1/events/purchase \
  -H 'content-type: application/json' \
  -d '{
    "user_id": "11111111-1111-1111-1111-111111111111",
    "product_id": "22222222-2222-2222-2222-222222222222",
    "session_id": "33333333-3333-3333-3333-333333333333",
    "timestamp": "2026-07-06T14:00:00Z",
    "order_id": "44444444-4444-4444-4444-444444444444",
    "quantity": 2,
    "unit_price_cents": 1999,
    "currency": "USD",
    "payment_method": "card"
  }'
```

### What gets rejected

Broken JSON, a missing field or a wrong type returns `400 {"error":"Validation error"}`.

Beyond that, an event that parses fine is still checked before it goes to Kafka. If a check fails, the response is `400` and names the first field that failed:

```
400 {"error":"invalid event: quantity: must be >= 1"}
```

The checks:

| Field | Rule |
|---|---|
| `user_id`, `product_id`, `session_id` | not the nil UUID `00000000-…` |
| `timestamp` | not more than 5 minutes in the future (to allow for clock skew); old timestamps are fine |
| click `metadata.source`, `metadata.category` | not blank, at most 64 characters |
| view `referrer` | at most 2048 characters, if present |
| purchase `quantity` | at least 1 |
| purchase `currency` | three uppercase letters, like `USD` (the format is checked, not the list of real currencies) |

A price of `0` is allowed, because free items and promos exist.

## Reading analytics

All endpoints are `GET` under `/api/v1/analytics`.

| Endpoint | Parameters | Returns |
|---|---|---|
| `/top-products` | `metric` = `clicks`, `views` or `purchases`; `period` = `1h`, `24h` or `7d`; `limit` (default 10) | products with the highest count |
| `/user-activity/{user_id}` | `from`, `to` (RFC 3339), `limit` (default 50), `offset` | one user's events of all types, newest first |
| `/conversion-rate` | `from`, `to` | views, purchases, and purchases ÷ views |
| `/realtime-stats` | none | event counts for the last 5 minutes |
| `/product-revenue` | `limit` (default 10) | revenue per product and currency |

```bash
curl "localhost:8082/api/v1/analytics/top-products?metric=clicks&period=1h&limit=10"
# [{"product_id":"2222…","metric_value":42}, …]

curl "localhost:8082/api/v1/analytics/user-activity/11111111-1111-1111-1111-111111111111?from=2026-07-06T00:00:00Z&to=2026-07-06T23:59:59Z"
# [{"event_type":"click","event_id":"aaaa…","product_id":"2222…","timestamp":"2026-07-06T14:00:00Z"}, …]

curl "localhost:8082/api/v1/analytics/conversion-rate?from=2026-07-06T00:00:00Z&to=2026-07-06T23:59:59Z"
# {"views":1200,"purchases":48,"conversion_rate":0.04}

curl "localhost:8082/api/v1/analytics/realtime-stats"
# {"clicks":150,"views":90,"purchases":12}

curl "localhost:8082/api/v1/analytics/product-revenue?limit=10"
# [{"product_id":"2222…","currency":"USD","revenue_cents":399800}, …]
```

A note on `/top-products` windows. With `period=1h` the service counts raw clicks for exactly the last 60 minutes. For `24h` and `7d` it reads the hourly click totals instead, which is much cheaper, but the window starts at the top of the hour. So "last 24 hours" at 14:37 really means "since 13:00 yesterday", up to one extra hour.

### Caching

Each response is cached in memory. `realtime-stats` lives for 5 seconds, most other responses for 60 seconds, and `top-products` for 1 to 10 minutes depending on the period. When a cached entry expires and many requests arrive at once, only one of them queries ClickHouse and the others wait for its result.

### Errors

Errors always look like `{"error": "…"}`:

- `400`: bad query parameters
- `404`: unknown route
- `503`: ClickHouse is down or the query ran longer than 30 seconds
