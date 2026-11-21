# Operations

## Build and run

The dashboard is embedded via `go:embed`. Build it before the Go binary. With Nix, the flake and Makefile do both steps:

```bash
make dev                 # optional: enter the dev shell
make build               # npm run build + go build
mkdir -p data
./nfvis --config config.yaml
```

Without Nix:

```bash
cd web && npm install && npm run build && cd ..
go build -o nfvis ./cmd/nfvis
mkdir -p data
./nfvis --config config.yaml
```

Flags:

- `--config PATH` (default `config.yaml`): a missing file means "use defaults".
- `--backup`: create a consistent SQLite snapshot (including committed WAL data) in a timestamped `.bak.*` file before opening/migrating. Recommended before upgrades.
- `--version`: print the version.

Signals: `SIGINT`/`SIGTERM` stop HTTP, close the UDP socket, drain the packet/flow queues, cancel pending DNS enrichment, and close SQLite.

## Search and table sorting

The ranked-host API accepts `search` for case-insensitive IP substring or captured reverse-DNS name matching across all ranked hosts before pagination. DNS resolution runs in a bounded worker queue after flow batches are stored; it never waits in the ingest writer. The per-flow source and destination names are persisted with lookup completion times. Host rankings retain observed names in hourly buckets for aggregate retention; a partial-hour selection can therefore show a name observed outside the precise selected minutes. A host may have several observed names within a selected range. Flow filters match each flow's own captured name, and old flows collected before this feature have no historical name. No lookup is performed at search time.

The queue can overflow under load, and pending jobs are abandoned on shutdown; those flows remain stored but unnamed. Lookup failures (including timeouts) are cached as empty results for the configured TTL. The System page reports queue depth, dropped jobs, endpoint cache hits/misses, named/unresolved results, write errors, and average/maximum lag from flow receipt to lookup completion. These counters are process-local. Cache hits use a name observed earlier within the TTL, not a fresh lookup. Clearing the cache does not change stored observations. DNS is not an authoritative record of the name at the exact time of traffic; exports and lookups can be delayed.

`/api/v1/top` and `/api/v1/flows` accept `sort` and `order=asc|desc`; sorting occurs before `limit` and `offset`. Ranked tables support `key`, `label`, `bytes`, `packets`, `flows`, and `share`. Flow tables support `time`, `interface`, `vlan`, `source`, `destination`, `proto`, `bytes`, and `packets`. Flow endpoint and interface sorting uses stored IP addresses and numeric identifiers rather than displayed DNS or interface names. Non-time flow sorts may be expensive on large ranges. Pagination is not a snapshot: ongoing ingestion or DNS changes can move rows between requests.

## Configuration

Start from `config.example.yaml`. Every duration accepts Go duration syntax (`30s`, `15m`, `720h`). Unknown keys are rejected; validation runs at startup and reports the first bad field.

Important knobs:

| Field | Meaning |
| --- | --- |
| `listen.udp` | UDP bind address. Use `0.0.0.0:2055` to receive from network devices. |
| `listen.http` | HTTP bind address. Keep `127.0.0.1:8080` unless a proxy handles access control. |
| `database.batch_size` | Flows per write transaction. Higher improves throughput and increases flush latency. |
| `database.flush_interval` | Maximum time a partial batch waits before being written. |
| `database.retention.raw` | How long individual flows stay queryable. |
| `database.retention.hourly` | How long hourly aggregates stay queryable. Must be at least raw. |
| `database.retention.cleanup_interval` | How often expired rows are swept. Default `1h`; a sweep also runs at startup. |
| `database.retention.cleanup_batch` | Rows deleted per retention statement; keeps sweeps short. |
| `database.resolution` | Chart resolution tiers as `bucket`/`retention` pairs. Defaults: `1m` for `168h`, `5m` for `720h`. |
| `database.dedup.parent_child` | Collapse the parent-interface and VLAN-child copies of one frame. Requires `parent_child_links`; with no links declared it is a validation error. Off by default. |
| `database.dedup.parent_child_links` | The exporter and interface pairs that are allowed to collapse, e.g. `{exporter: "192.168.1.1", parent: 2, child: 7}`. The parent record survives. |
| `database.dedup.nat.wan_addresses` | The firewall's own address(es) on the NAT boundary. Requires `nat.exporters`; with addresses but no exporter entry it is a validation error. Off when empty. |
| `database.dedup.nat.exporters` | The firewall that reports both NAT views and the input interface of each: `{address, pre_interface, post_interface}`. A record whose exporter or input interface is not listed is never paired. |
| `database.dedup.nat.window` | How far apart the two NAT views' export timestamps may be. Default `2s`. |
| `decoder.workers` | Decode parallelism. Increase if UDP queue drops are reported. |
| `decoder.queue_size` | Packets buffered before UDP packets are dropped. |
| `decoder.template_ttl` | Time after which an unseen v9 template is forgotten. |
| `dns.enabled` | Capture reverse DNS asynchronously for newly received flows. Default `true`. |
| `dns.cache_ttl` | Reuse a cached lookup while it is younger than this. Default `5m`. |
| `dns.cache_size` | Maximum cached addresses. Default `10000`. |
| `dns.lookup_timeout` | Per-lookup timeout. Default `2s`. |
| `dns.server` | Resolver for PTR queries: an IP with optional port (`192.0.2.53`, `192.0.2.53:5353`, `[fd00::1]:5353`), port 53 by default. Empty uses the system resolver from `/etc/resolv.conf`. A hostname is rejected, because resolving it would need the resolver being configured. Lookups go straight to this server (Go's resolver, `PreferGo`), so `/etc/resolv.conf` search domains and `ndots` do not apply. |
| `exporters[].address` | UDP source IP of the device. Labels and interface names are keyed by this. |
| `vlan_rules[]` | CIDR → VLAN mapping, scoped to `src`, `dst`, or `both`. Longest prefix wins; exported VLAN wins over rules. |
| `vlan_labels` | Optional VLAN ID → display name mapping used by the VLAN tab. Unlabeled IDs show as `VLAN <id>`. |

### Duplicate suppression and its residual risk

Both mechanisms are **off by default** and both need the site's topology before
they can do anything. A matching payload is not evidence that two records
describe the same packet, so:

* `parent_child` collapses only pairs named in `parent_child_links`, per
  exporter. Records on the same interface, on an unconfigured interface, on the
  reverse order of an unconfigured pair, or for an exporter with no links are
  always stored separately.
* `nat` drops a post-NAT copy only when the record's exporter and input
  interface are listed in `nat.exporters`, exactly **one** candidate mate exists
  with the opposite WAN/non-WAN source on the other configured interface, the
  same output interface, the same protocol, destination, counters, and an export
  timestamp within `window`. Zero or two candidates means no pair is proven, and
  the record is stored.

Known limitations, stated rather than hidden:

* **NAT pairing is heuristic at the record level.** Two unrelated flows that
  share protocol, destination address and port, byte and packet counts, output
  interface and a sub-second export interval are indistinguishable from a
  translated pair. The configured interface split and the single-candidate rule
  remove the shapes measured in production (firewall-originated DNS colliding
  with LAN DNS, cross-exporter matches), but they cannot prove the negative.
  Keep `nat` off unless you need it, and if the firewall's own WAN-sourced
  traffic and forwarded traffic are exported on the same input interface, leave
  it off: nothing in the record separates them.
* **Collapsed rows are not recoverable.** Suppression drops a row instead of
  storing it; there is no shadow copy. Turning the mechanism off later does not
  restore history, and rows already collapsed by earlier releases are gone.
* **Aggregates are deltas, not re-derivations.** Every aggregate family
  (`agg_exporter`, `agg_interface`, `agg_host`, `agg_vlan`, `agg_protocol`,
  `agg_traffic`) is maintained by applying the exact contribution of each stored,
  promoted or deleted row inside the ingest transaction. Aggregate history
  therefore survives raw retention pruning, which a re-derivation from the raw
  table would destroy. The price is that a subtraction is only correct if the
  stored row it subtracts is the row that was added; that is why the writer reads
  back the stored row before acting on it.
* **Interface promotion is exact but only for interfaces.** When a child copy is
  promoted to its parent, the row keeps its id (so queued reverse-DNS lookups stay
  attached) and only `agg_interface` moves. The other families are keyed by fields
  the copies share, so they do not change.

### Chart resolution tiers

`database.resolution` controls how much detail the traffic chart can draw. Each tier is one extra row per bucket in `agg_traffic`, so tiers are cheap because they carry no dimensions:

```yaml
database:
  resolution:
    - bucket: 1m
      retention: 168h
    - bucket: 5m
      retention: 720h
```

An hourly tier always exists from `database.retention.hourly`; listing `bucket: 1h` explicitly is rejected. Buckets must be whole minutes between `1m` and `1h`, each tier's retention must be at least its bucket and at least as large as the finer tier before it, and no tier may outlive `retention.hourly`.

The chart picks a bucket width based on the covered span and coarsens as needed to stay within the point budget. A range that spans a tier boundary is drawn from both tiers, so the newest hours can be minute resolution while older days are hourly; the response reports `resolution: "mixed"` with `bucket_ms` (finest) and `aggregate_bucket_ms` (coarsest).

The first start after upgrading to schema version 2 seeds `agg_traffic` from the data already in the database. The hourly tier is copied from `agg_exporter` and finer tiers are grouped from retained raw flows, bounded by each tier's retention. The step is logged and has a ten-minute budget; time depends on database size. Resolutions already containing rows are skipped on later starts. No manual migration is needed.

Filtered charts (host, port, interface, VLAN, protocol, exporter) cannot use `agg_traffic` and read raw flows instead, so they are limited to `retention.raw` and to a 15 second request budget. Removing filters or shortening the range restores full detail.

## Unknown values

Some flows legitimately have no interface or VLAN identity:

- An interface key such as `0@10.10.10.1` means that exporter reported ifIndex `0` for that flow. This is common with OPNsense for locally generated/firewall-originated traffic. It is an unknown input interface, not a real port; the exporter address after `@` identifies which device reported it.
- VLAN `-1` means neither an exported VLAN field nor a matching `vlan_rules[]` entry supplied a VLAN. Add CIDR rules if you want those endpoints classified. The dashboard flags these rows as unknown and shows `-` for missing VLAN values on the Flows page.

Both values are retained so byte and flow totals stay accurate. They are labeled as unknown rather than being dropped or mapped to a guessed interface/VLAN.

## Health monitoring

`GET /api/v1/health` returns status, uptime, HTTP/UDP addresses, collector counters, database size/row counts, coverage, and warnings.

Watch these counters:

- `udp_queue_drops`: packet queue overflow. Increase `decoder.workers` or `decoder.queue_size`.
- `flows_dropped`: batch insert failures after retries. Check disk space and `db_errors`/`disk_full_events`.
- `unknown_template_sets`: normal at startup or after template expiry; a persistent increase means records could not be decoded. Investigate template delivery, loss, exporter behavior and collector decoding; do not assume the exporter is at fault.
- `timestamp_fallbacks`, `timestamp_wraps`: timestamp normalization activity.
- `sampling_options`, `unknown_fields`: decoder coverage signals.
- `last_successful_insert_ms`, `ingest_healthy`: liveness of the storage path.

`status` is `degraded` when the database/coverage query fails, the last insert failed, or UDP packets were dropped.

## Disk sizing

A historical DNS-disabled v5 soak on the documented benchmark machine used roughly **400 bytes per stored flow**, including indexes and hourly aggregates (22 MB for 59,460 flows). Current schema, DNS observations and workload cardinality can increase this; measure your deployment.

Approximate raw database size:

```
flows/day  = rate × 86,400
bytes/day  = flows/day × 400
raw total  = bytes/day × retention_days
```

Examples at 1,000 flows/s:

| Raw retention | Approximate size |
| --- | --- |
| 1 day | ~35 GB |
| 7 days | ~240 GB |
| 30 days (default) | ~1.0 TB |

Hourly aggregates are negligible by comparison. If disk is limited, reduce `retention.raw`; aggregates still preserve long-term traffic totals. Monitor `database.size_bytes` and `wal_bytes` from health.

SQLite WAL files grow until checkpoints. A clean shutdown checkpoints the WAL. If the WAL grows without bound while running, verify retention sweeps are executing (`retention_raw_deleted`) and that the process is the only writer.

## DNS and addresses

`dns.server` pins the resolver used for PTR lookups. Leaving it empty keeps the host resolver from `/etc/resolv.conf`; setting it sends every query to that address instead, which is what you want when the collector runs in a container whose `/etc/resolv.conf` points at a stub resolver that cannot see your internal zones.

The value must be a literal IP with an optional port (`192.0.2.53`, `192.0.2.53:5353`, `fd00::1`, `[fd00::1]:5353`); port 53 is assumed. A hostname is rejected at startup rather than resolved, because resolving it would need the resolver being configured. An unreachable server is not fatal: lookups time out, the negative result is cached for `dns.cache_ttl`, and `/api/v1/system` keeps counting unresolved endpoints while flows are stored unnamed. The System page shows the active server next to the cache statistics.

## Recovery

- Do not delete `-wal` or `-shm` files while nfvis is running.
- Use `--backup` before migrations or upgrades; it creates a consistent SQLite online snapshot, including committed WAL data, with restrictive permissions.
- If the database is corrupt, stop the service, restore from the backup, and restart.
- After an unclean shutdown, SQLite replays the WAL automatically on open.
- Migrations are forward-only. Downgrading the binary against a newer schema is not supported.

## Benchmark/soak

```bash
# Microbenchmarks
go test ./internal/decoder -run '^$' -bench . -benchmem

# 60-second end-to-end run at 1,000 flows/s
DURATION=60s TARGET=1000 ./benchmark/soak.sh
```

`soak.sh` builds the binary and load generator, starts nfvis on alternative ports, sends v5 traffic, then samples API latency, counter deltas and database size. Override `UDP`, `HTTP`, `DURATION`, `TARGET`, and `GO` as needed. Use `GO='nix shell nixpkgs#go --command go'` when Go is provided by Nix.

See [benchmarks.md](benchmarks.md) for recorded results.
