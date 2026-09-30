#!/usr/bin/env bash
# Sends a few events through the running stack and reads the analytics back.
# Usage: ./scripts/demo.sh   (after `docker compose up -d`)
set -euo pipefail

INGEST=${INGESTION_URL:-http://localhost:8080}/api/v1/events
ANALYTICS=${ANALYTICS_URL:-http://localhost:8082}/api/v1/analytics

uuid() {
  if command -v uuidgen >/dev/null; then uuidgen | tr 'A-Z' 'a-z'; else cat /proc/sys/kernel/random/uuid; fi
}

# pretty-print JSON when jq is available, raw otherwise
show() {
  if command -v jq >/dev/null; then jq -c .; else cat; echo; fi
}

post() {
  local type=$1 body=$2
  printf '  POST %-9s -> ' "$type"
  curl -fsS -X POST "$INGEST/$type" -H 'content-type: application/json' -d "$body" | show
}

user=$(uuid)
product=$(uuid)
session=$(uuid)
now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
common="\"user_id\":\"$user\",\"product_id\":\"$product\",\"session_id\":\"$session\",\"timestamp\":\"$now\""

echo "Sending events for user $user"
post view     "{$common,\"duration_ms\":4500,\"referrer\":\"https://example.com/search\"}"
post click    "{$common,\"metadata\":{\"source\":\"search\",\"position\":3,\"category\":\"electronics\"}}"
post purchase "{$common,\"order_id\":\"$(uuid)\",\"quantity\":2,\"unit_price_cents\":1999,\"currency\":\"USD\",\"payment_method\":\"card\"}"

echo "An invalid event is rejected before it reaches Kafka:"
printf '  POST %-9s -> ' "purchase"
curl -sS -X POST "$INGEST/purchase" -H 'content-type: application/json' \
  -d "{$common,\"order_id\":\"$(uuid)\",\"quantity\":0,\"unit_price_cents\":1999,\"currency\":\"USD\",\"payment_method\":\"card\"}" | show

echo "Waiting 6s for the workers to flush the batch to ClickHouse..."
sleep 6

echo "Reading it back:"
printf '  realtime-stats  -> '
curl -fsS "$ANALYTICS/realtime-stats" | show
printf '  user-activity   -> '
curl -fsS "$ANALYTICS/user-activity/$user?from=2000-01-01T00:00:00Z&to=2100-01-01T00:00:00Z" | show
printf '  product-revenue -> '
curl -fsS "$ANALYTICS/product-revenue?limit=3" | show

echo "Dashboards: Grafana http://localhost:3000, Kafka UI http://localhost:8081"
