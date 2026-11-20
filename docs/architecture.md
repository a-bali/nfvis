# Architecture and data model

## Process model

```
UDP :2055 ──▶ readLoop ──▶ packet queue ──▶ N decode workers ──▶ flow channel ──▶ batch loop ──▶ SQLite
                                                                                     │
HTTP :8080 ──▶ JSON API ──▶ SQLite reads ◀────────────────────────────────────────────┘
                         └─▶ embedded React/ECharts assets
```

One process owns the UDP socket and database. Decoder workers feed a batch loop that writes raw flows and aggregates transactionally. A bounded DNS worker queue runs separately after insertion and writes name observations asynchronously; it can drop work under load or on shutdown. SQLite serializes writes from ingestion, DNS enrichment and retention. Retention and template expiry run in the same process on a maintenance ticker.

## Decoding

- v5 is decoded with `goflow2/decoders/netflowlegacy`. Exporter identity is the UDP peer address. Sampling comes from the header sampling interval.
- v9 is decoded with `goflow2/decoders/netflow`. Templates are keyed by `(exporter, sourceID, version, obsDomainID, templateID)`, so two exporters reusing template ID 256 cannot decode each other's data.
- Template refreshes replace the stored definition. TTL expiry and a sufficiently large change in the uptime-derived boot-time anchor clear templates; a sequence-number regression alone does not. Validate reboot detection against the exporter in use.
- Unknown templates are counted as a normal condition; the packet is not treated as fatal.
- Timestamps use the device uptime anchor with a signed 32-bit delta to tolerate 32-bit counter wraps. If the exported window is implausible or absent, the receive time is used and `time_source` records which happened.
- VLANs are resolved from exported fields first, then from longest-prefix CIDR rules with optional source/direction scoping. Unknown VLANs use `-1` with `vlan_source=unknown`.

## Normalized flow

Each flow keeps:

- time: `received_ms`, `start_ms`, `end_ms`, `time_source`
- identity: `version`, `exporter`, `exporter_ip`, `exporter_id`, `sequence`, `engine_type/id`
- interfaces: `input_if`, `input_label`, `output_if`, `output_label`
- endpoints: `src_addr`, `dst_addr`, ports, protocol, TCP flags, TOS fields, ASNs
- VLANs: `src_vlan`, `dst_vlan` plus `src_vlan_source`, `dst_vlan_source`
- direction: `direction`, `direction_known` (v9 direction field when exported; never guessed)
- sampling: `sampled`, `sampling_interval`, `sampling_algorithm`, `sampling_source`
- bytes and packets as received
- asynchronous reverse-DNS observations: `src_name`, `dst_name`, `src_dns_ms`, `dst_dns_ms` (empty for unresolved or uncaptured endpoints)

Exporter and interface labels come from configuration and are attached at decode time (Flows page) or join time (ranked API). Interface aggregation keys include the exporter address so interface index 1 on two routers remains two distinct rows.

## Storage

SQLite schema version 3:

- `flows`: raw normalized flows, indexed by end time, exporter, addresses and VLANs.
- `agg_exporter`, `agg_interface`, `agg_host`, `agg_vlan`, `agg_protocol`: hourly counters (`bytes`, `packets`, `flows`, `sampled_flows`, `unknown_flows`). Host and VLAN rows carry a `direction` column.
- `dns_host_history`: names observed per host, direction and hour; retained with hourly aggregates, independently of raw-flow deletion.
- `agg_traffic`: global traffic totals for the dashboard charts, keyed by `(res_ms, bucket_ms)` with one row per bucket per resolution tier. This table is small because it stores no dimensions: a one-minute/one-week tier is about 10,000 rows.
- `collector_state`: last successful insert and maintenance metadata.
- `schema_migrations`: versioned, forward-only migrations.

Upgrading from schema version 1 creates `agg_traffic` and seeds it once at startup (`backfill.go`). The hourly tier is copied from `agg_exporter`; finer tiers are grouped from the raw flows that are still inside each tier's retention window. The backfill runs with a ten-minute budget, skips resolutions already containing rows, and inserts with `ON CONFLICT DO NOTHING`, so restarting it is harmless and newly configured resolutions can be seeded from retained data. Existing data therefore appears at the finer resolutions immediately instead of after the retention window has rolled over. Progress and outcome are logged as `seeded traffic resolution tier` and `traffic tier backfill complete`.

Raw flows and hourly aggregates are written in the same transaction per batch, so a crash cannot leave aggregates without matching raw data for that batch. WAL mode with `synchronous=NORMAL` is used; the tradeoff is that an OS-level power loss may lose the most recent committed transactions, not corrupt the database.

## Query semantics

- All API ranges are half-open UTC intervals: `[from, to)`.
- `from/to` accept RFC3339 or relative strings such as `-1h`; `now` is the default upper bound.
- Aggregates are bucketed on UTC hour boundaries.
- `top` uses the hourly aggregate tables for every complete hour it covers, even while raw flows are retained. Only the current, still-changing hour is read from raw flows, so long ranges stay bounded. A flow is never counted from both families. Rankings without an explicit direction use the raw path because aggregate tables hold both sides of each flow.
- `timeseries` reads `agg_traffic` instead, using the resolution tiers from `database.resolution` plus the implicit hourly tier. The requested range is cut at tier boundaries (rounded down to the next coarser bucket so a bucket never straddles two segments) and each segment is read from its own tier, so a long range shows minute buckets for recent hours and hourly buckets for older days without double counting.
- The rendered width of a segment is the coarser of its tier bucket and the tier chosen for the covered span. The choice is made from the covered span, not the requested span, so asking for a year does not force hourly buckets onto the day that happens to be the only retained data. Widths then coarsen uniformly along the 1/2/5/10/15/30/60-minute ladder to keep a series at or below 20,000 points.
- `resolution` is `raw`, `aggregate`, or `mixed`; `bucket_ms` reports the finest rendered width and `aggregate_bucket_ms` the coarsest. Each total point includes `duration_ms`, used by the dashboard to display average bytes, packets or flows per second; API counts remain bucket totals. `limitations` explains missing coverage, and a range with no retained data returns an empty series with `No traffic was recorded in this period.`
- Flow-level timeseries filters (host, port, interface, VLAN, protocol) use retained raw flows only, since `agg_traffic` holds no dimensions. Raw scans are chunked at six hours and share a 15 second request budget; when the budget is exhausted the response returns the complete leading part of the series plus a limitation saying the chart stopped early, instead of failing the whole request.

`GET /api/v1/timeseries?split_by=<dimension>&metric=<bytes|packets|flows>` returns the same range as stacked bands for `input_if`, `output_if`, `input_vlan`, `output_vlan`, `src_host`, `dst_host` or `protocol`. A dimension cannot be recovered from `agg_traffic`, but the hourly dimension tables (`agg_interface`, `agg_vlan`, `agg_host`, `agg_protocol`) keep every key per hour. An unfiltered stacked chart therefore reads whole hours from those tables and only scans individual flows for the most recent six hours plus a partial hour at the start of the range, which keeps a 30-day chart in the tens of milliseconds. That part of the chart is hourly, which the response explains in `limitations`. A filtered stacked chart cannot use the aggregates, so it scans raw flows for the whole range and stops with a partial series when the 15 second budget runs out.

Each pass ranks the keys by the requested metric, then buckets the top ten and folds every other key into one `other` band, coarsening the bucket width until bucket × band stays inside the payload budget. The response keeps `points` as the stack's bucket totals and per-bucket durations. The stacked and unstacked queries can use different sources and bucket widths, so their points need not coincide exactly.
- `flows` returns retained individual flows for the covered portion of the requested range, with a limitation when earlier detail is missing. It returns HTTP 410 with `code=aggregate_only` when no raw detail covers the period.
- Network totals come from `agg_exporter` (or raw flow counts). Source/destination host and VLAN views are two views of the same flows and must not be summed into a "total".

The dashboard polls every five seconds, so the read path avoids work proportional to the raw table:

- Row counts in `Stats` (health and system endpoints) are cached for 15 seconds and invalidated by inserts and retention sweeps; only database and WAL sizes are read fresh each time.
- Coverage asks for the oldest and newest flow with two scalar subqueries (`SELECT (SELECT MIN(end_ms) …), (SELECT MAX(end_ms) …)`). Written as one aggregate over both columns, SQLite scans the whole end-time index instead of seeking its ends, which costs most of a second on a few million rows.
- The exporter list comes from `agg_exporter`, which has a handful of rows per exporter, instead of `SELECT DISTINCT exporter FROM flows`; the raw table is only consulted while the aggregates are still empty.

## Retention

`retention.raw` and `retention.hourly` are independent. A bounded sweep (`cleanup_batch` rows per statement) deletes raw flows older than the raw deadline, hourly aggregates and historical DNS observations older than the hourly deadline, and `agg_traffic` rows older than their own tier retention (cutoffs aligned to the tier bucket). Because aggregates are written alongside raw flows, deleting old raw rows does not remove the surviving summaries.

Retention does not need to be monotonic with resolution, but the configuration requires each tier's retention to be at least the retention of the finer tier before it, so a chart never falls back to a coarser bucket for a period where a finer one is still available.
