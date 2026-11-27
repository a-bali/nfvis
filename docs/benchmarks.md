# Benchmarks and capacity

## Method

All numbers below were measured on the same machine:

- Apple M1, 8 logical cores, 16 GB RAM
- Darwin 27.2.0 arm64
- Go 1.27.3
- SQLite via `modernc.org/sqlite` v1.62.0, WAL, `synchronous=NORMAL`

Repository commands:

```bash
go test ./internal/decoder -run '^$' -bench BenchmarkDecode -benchtime=3s -benchmem
GO='nix shell nixpkgs#go --command go' DURATION=60s TARGET=1000 ./benchmark/soak.sh
```

The soak run starts nfvis on alternative ports, sends synthetic v5 traffic at 1,000 flows/s (30 flows/datagram), then reads counter deltas from `/api/v1/status` and times representative API queries with `curl`.

## Decoder microbenchmarks

Packet batches contain 30 flows each. Throughput is per core.

| Benchmark | ns/op | MB/s | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| `BenchmarkDecodeV5` | 8,312 | 176.13 | 11,792 | 64 |
| `BenchmarkDecodeV9` | 38,896 | 31.57 | 66,130 | 627 |

v9 is slower because each packet carries a template set and creates field maps. In steady state, exporters send templates periodically rather than with every packet.

## End-to-end soak (60 s, target 1,000 flows/s)

| Metric | Result |
| --- | --- |
| Sent packets | 1,982 |
| Sent flows | 59,460 |
| Achieved rate | 991.0 flows/s (99.1% of target) |
| UDP send errors | 0 |
| `flows_decoded` delta | 59,460 |
| `flows_stored` delta | 59,400 (before/after sampling boundary) |
| `flows_dropped` | 0 |
| `udp_queue_drops` | 0 |
| `db_errors` / `disk_full_events` | 0 / 0 |
| Ingest health | `true` |

The generator uses a 10 ms pacing tick, so a small rate shortfall is expected. No packets or flows were dropped by nfvis.

### Query latency during/after the soak

| Endpoint | `time_total` |
| --- | ---: |
| `/api/v1/status` | 0.0058 s |
| `/api/v1/timeseries` | 0.0507 s |
| `/api/v1/top?dimension=host` | 0.0511 s |
| `/api/v1/flows?limit=100` | 0.0104 s |
| `/api/v1/coverage` | 0.0059 s |

Queries were bounded to default windows (1 hour). Latency grows with range and cardinality; use narrower ranges for large deployments.

### Database size

| File | Size after 60 s |
| --- | ---: |
| `nfvis.db` | 22 MB |
| `nfvis.db-wal` | 4.2 MB |
| `database.size_bytes` | 24,035,328 |
| `database.wal_bytes` | 4,420,792 |
| Raw flow rows | 59,460 |
| Hourly aggregate rows | 8 |

This is about 400 bytes per raw flow including indexes and aggregates. Use that figure for [disk sizing](operations.md#disk-sizing).

## Interpretation

These are historical baseline measurements, not a benchmark of the current asynchronous DNS enrichment path or schema. Re-run with representative DNS configuration, traffic and hardware before sizing a release deployment.

The 1,000 flows/s target was met on the documented machine with no drops, and the write path has headroom because the batch loop commits roughly four transactions per second at this rate. The limiting factors at higher rates will be UDP socket buffers, decode worker count, and SQLite write throughput. Monitor `udp_queue_drops` and `db_errors` when increasing the target.

Repeat the soak on your own hardware before relying on nfvis for a real exporter; synthetic flows do not exercise v9 template churn or high-cardinality host queries.
