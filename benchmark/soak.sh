#!/usr/bin/env bash
# End-to-end soak: run nfvis, send synthetic NetFlow, sample API latency and DB size.
set -euo pipefail

DURATION="${DURATION:-5m}"
TARGET="${TARGET:-1000}"
UDP="${UDP:-127.0.0.1:20555}"
HTTP="${HTTP:-http://127.0.0.1:18080}"
HTTP_HOST="${HTTP#http://}"
HTTP_HOST="${HTTP_HOST#https://}"
WORK="${WORK:-$(mktemp -d)}"
GO_CMD="${GO:-go}"

mkdir -p "$WORK"
CONFIG="$WORK/config.yaml"
DB="$WORK/nfvis.db"
cat > "$CONFIG" <<EOF
listen:
  udp: "$UDP"
  http: "$HTTP_HOST"
database:
  path: "$DB"
  batch_size: 500
  flush_interval: 250ms
  retention:
    raw: 720h
    hourly: 8760h
    cleanup_interval: 1m
    cleanup_batch: 5000
decoder:
  workers: 4
  queue_size: 8192
  template_ttl: 30m
EOF

echo "workdir: $WORK"
(cd "$(dirname "$0")/.." && $GO_CMD build -o "$WORK/nfvis" ./cmd/nfvis)
(cd "$(dirname "$0")/.." && $GO_CMD build -o "$WORK/loadgen" ./benchmark/ingest)

"$WORK/nfvis" -config "$CONFIG" >"$WORK/nfvis.log" 2>&1 &
PID=$!
trap 'kill "$PID" 2>/dev/null || true' EXIT

for _ in $(seq 1 50); do
  if curl -fsS "$HTTP/api/v1/health" >/dev/null 2>&1; then break; fi
  sleep 0.1
done

echo "load: target=$TARGET flows/s duration=$DURATION"
"$WORK/loadgen" -addr "$UDP" -health "$HTTP/api/v1/health" -target "$TARGET" -duration "$DURATION"

echo "query latency samples (seconds):"
for path in "health" "timeseries" "top?dimension=host" "flows?limit=100" "coverage"; do
  curl -sS -o /dev/null -w "  /api/v1/$path total=%{time_total} status=%{http_code}\n" "$HTTP/api/v1/$path"
done

echo "database:"
ls -lh "$DB" "$DB-wal" 2>/dev/null || true
curl -sS "$HTTP/api/v1/health" | python3 -c 'import json,sys; d=json.load(sys.stdin); print("counters:", json.dumps(d["counters"], indent=2)); print("database:", json.dumps(d["database"], indent=2))'
echo "log tail:"
tail -20 "$WORK/nfvis.log"
