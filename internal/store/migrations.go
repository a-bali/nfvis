package store

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"
)

// schemaVersion is incremented for every compatibility-breaking schema change.
const schemaVersion = 5

var migrations = []string{
	`CREATE TABLE IF NOT EXISTS flows (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		received_ms INTEGER NOT NULL,
		start_ms INTEGER,
		end_ms INTEGER NOT NULL,
		time_source TEXT NOT NULL,
		version INTEGER NOT NULL,
		exporter TEXT NOT NULL,
		exporter_ip TEXT NOT NULL,
		exporter_id INTEGER NOT NULL DEFAULT 0,
		sequence INTEGER NOT NULL DEFAULT 0,
		engine_type INTEGER NOT NULL DEFAULT 0,
		engine_id INTEGER NOT NULL DEFAULT 0,
		input_if INTEGER NOT NULL DEFAULT 0,
		input_label TEXT NOT NULL DEFAULT '',
		output_if INTEGER NOT NULL DEFAULT 0,
		output_label TEXT NOT NULL DEFAULT '',
		direction INTEGER NOT NULL DEFAULT 0,
		direction_known INTEGER NOT NULL DEFAULT 0,
		src_addr TEXT NOT NULL DEFAULT '',
		dst_addr TEXT NOT NULL DEFAULT '',
		src_port INTEGER NOT NULL DEFAULT 0,
		dst_port INTEGER NOT NULL DEFAULT 0,
		protocol INTEGER NOT NULL DEFAULT 0,
		tcp_flags INTEGER NOT NULL DEFAULT 0,
		src_tos INTEGER NOT NULL DEFAULT 0,
		dst_tos INTEGER NOT NULL DEFAULT 0,
		bytes INTEGER NOT NULL DEFAULT 0,
		packets INTEGER NOT NULL DEFAULT 0,
		src_as INTEGER NOT NULL DEFAULT 0,
		dst_as INTEGER NOT NULL DEFAULT 0,
		src_vlan INTEGER NOT NULL DEFAULT -1,
		dst_vlan INTEGER NOT NULL DEFAULT -1,
		src_vlan_source TEXT NOT NULL DEFAULT 'unknown',
		dst_vlan_source TEXT NOT NULL DEFAULT 'unknown',
		sampled INTEGER NOT NULL DEFAULT 0,
		sampling_interval INTEGER NOT NULL DEFAULT 0,
		sampling_algorithm INTEGER NOT NULL DEFAULT 0,
		sampling_source TEXT NOT NULL DEFAULT 'unknown'
	);
	CREATE INDEX IF NOT EXISTS idx_flows_end ON flows(end_ms);
	CREATE INDEX IF NOT EXISTS idx_flows_exporter_end ON flows(exporter, end_ms);
	CREATE INDEX IF NOT EXISTS idx_flows_src_end ON flows(src_addr, end_ms);
	CREATE INDEX IF NOT EXISTS idx_flows_dst_end ON flows(dst_addr, end_ms);
	CREATE INDEX IF NOT EXISTS idx_flows_input_end ON flows(input_if, end_ms);
	CREATE INDEX IF NOT EXISTS idx_flows_output_end ON flows(output_if, end_ms);
	CREATE INDEX IF NOT EXISTS idx_flows_src_vlan_end ON flows(src_vlan, end_ms);
	CREATE INDEX IF NOT EXISTS idx_flows_dst_vlan_end ON flows(dst_vlan, end_ms);
	CREATE INDEX IF NOT EXISTS idx_flows_protocol_end ON flows(protocol, end_ms);

	CREATE TABLE IF NOT EXISTS agg_exporter (
		hour_ms INTEGER NOT NULL,
		exporter TEXT NOT NULL,
		bytes INTEGER NOT NULL DEFAULT 0,
		packets INTEGER NOT NULL DEFAULT 0,
		flows INTEGER NOT NULL DEFAULT 0,
		sampled_flows INTEGER NOT NULL DEFAULT 0,
		unknown_flows INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY (hour_ms, exporter)
	);
	CREATE INDEX IF NOT EXISTS idx_agg_exporter_hour ON agg_exporter(hour_ms);

	CREATE TABLE IF NOT EXISTS agg_interface (
		hour_ms INTEGER NOT NULL,
		exporter TEXT NOT NULL,
		if_index INTEGER NOT NULL,
		direction TEXT NOT NULL,
		label TEXT NOT NULL DEFAULT '',
		bytes INTEGER NOT NULL DEFAULT 0,
		packets INTEGER NOT NULL DEFAULT 0,
		flows INTEGER NOT NULL DEFAULT 0,
		sampled_flows INTEGER NOT NULL DEFAULT 0,
		unknown_flows INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY (hour_ms, exporter, if_index, direction)
	);
	CREATE INDEX IF NOT EXISTS idx_agg_interface_hour ON agg_interface(hour_ms);
	CREATE INDEX IF NOT EXISTS idx_agg_interface_if ON agg_interface(exporter, if_index, hour_ms);

	CREATE TABLE IF NOT EXISTS agg_host (
		hour_ms INTEGER NOT NULL,
		host TEXT NOT NULL,
		direction TEXT NOT NULL,
		bytes INTEGER NOT NULL DEFAULT 0,
		packets INTEGER NOT NULL DEFAULT 0,
		flows INTEGER NOT NULL DEFAULT 0,
		sampled_flows INTEGER NOT NULL DEFAULT 0,
		unknown_flows INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY (hour_ms, host, direction)
	);
	CREATE INDEX IF NOT EXISTS idx_agg_host_hour ON agg_host(hour_ms);
	CREATE INDEX IF NOT EXISTS idx_agg_host_host ON agg_host(host, hour_ms);

	CREATE TABLE IF NOT EXISTS agg_vlan (
		hour_ms INTEGER NOT NULL,
		vlan INTEGER NOT NULL,
		direction TEXT NOT NULL,
		bytes INTEGER NOT NULL DEFAULT 0,
		packets INTEGER NOT NULL DEFAULT 0,
		flows INTEGER NOT NULL DEFAULT 0,
		sampled_flows INTEGER NOT NULL DEFAULT 0,
		unknown_flows INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY (hour_ms, vlan, direction)
	);
	CREATE INDEX IF NOT EXISTS idx_agg_vlan_hour ON agg_vlan(hour_ms);

	CREATE TABLE IF NOT EXISTS agg_protocol (
		hour_ms INTEGER NOT NULL,
		protocol INTEGER NOT NULL,
		bytes INTEGER NOT NULL DEFAULT 0,
		packets INTEGER NOT NULL DEFAULT 0,
		flows INTEGER NOT NULL DEFAULT 0,
		sampled_flows INTEGER NOT NULL DEFAULT 0,
		unknown_flows INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY (hour_ms, protocol)
	);
	CREATE INDEX IF NOT EXISTS idx_agg_protocol_hour ON agg_protocol(hour_ms);

	CREATE TABLE IF NOT EXISTS collector_state (
		key TEXT PRIMARY KEY,
		value TEXT NOT NULL
	);`,
	// Migration 2: traffic totals at configurable resolutions. Each row is one
	// bucket of one tier, so charts can mix a 1-minute, 5-minute, and hourly
	// tier in a single range without double counting.
	`CREATE TABLE IF NOT EXISTS agg_traffic (
		res_ms INTEGER NOT NULL,
		bucket_ms INTEGER NOT NULL,
		bytes INTEGER NOT NULL DEFAULT 0,
		packets INTEGER NOT NULL DEFAULT 0,
		flows INTEGER NOT NULL DEFAULT 0,
		sampled_flows INTEGER NOT NULL DEFAULT 0,
		unknown_flows INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY (res_ms, bucket_ms)
	) WITHOUT ROWID;`,
	`ALTER TABLE flows ADD COLUMN src_name TEXT NOT NULL DEFAULT '';
	ALTER TABLE flows ADD COLUMN dst_name TEXT NOT NULL DEFAULT '';
	ALTER TABLE flows ADD COLUMN src_dns_ms INTEGER NOT NULL DEFAULT 0;
	ALTER TABLE flows ADD COLUMN dst_dns_ms INTEGER NOT NULL DEFAULT 0;
	CREATE TABLE dns_host_history (hour_ms INTEGER NOT NULL, host TEXT NOT NULL, direction TEXT NOT NULL, name TEXT NOT NULL, last_ms INTEGER NOT NULL, PRIMARY KEY(hour_ms, host, direction, name)) WITHOUT ROWID;
	CREATE INDEX idx_dns_history_hour ON dns_host_history(hour_ms);`,
	// Migration 4: parent/child duplicate suppression. `dedup_key` is the
	// payload fingerprint without the interfaces (see flowDedupKey). The unique
	// index is the cross-batch memory that makes dedup hold across the 250 ms
	// flush boundary; it is partial so rows written before this migration — which
	// have no key and cannot be identified retroactively — are never collapsed.
	`ALTER TABLE flows ADD COLUMN dedup_key BLOB;
	CREATE UNIQUE INDEX idx_flows_dedup_key ON flows(dedup_key) WHERE dedup_key IS NOT NULL;`,
	// Migration 5: the unique partial index on dedup_key made the payload
	// fingerprint a global identity, so two genuinely identical records — the same
	// flow reported twice, or the same 5-tuple on two unconfigured interfaces —
	// could not both be stored. Duplicate suppression is a decision the writer
	// makes against configured topology, not a constraint the table enforces, so
	// the index becomes an ordinary lookup index. Existing rows keep their keys;
	// rows already collapsed by the old constraint cannot be recovered.
	`DROP INDEX IF EXISTS idx_flows_dedup_key;
	CREATE INDEX idx_flows_dedup_key_lookup ON flows(dedup_key) WHERE dedup_key IS NOT NULL;`,
}

func migrate(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version INTEGER PRIMARY KEY,
		applied_at TEXT NOT NULL
	)`); err != nil {
		return fmt.Errorf("create migrations table: %w", err)
	}
	var current int
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&current); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	if current > schemaVersion {
		return fmt.Errorf("database schema version %d is newer than supported version %d", current, schemaVersion)
	}
	if current == schemaVersion {
		slog.Info("database schema is current", "version", current)
		return nil
	}
	// Migrations run before the HTTP listener starts, so the process is deaf on
	// its health port for the whole duration. Emitting progress is what lets an
	// operator distinguish "indexing 12M rows" from "hung" — silence here cost a
	// release once, when a 5-minute migration looked identical to a dead pod.
	slog.Info("applying database migrations",
		"from", current, "to", schemaVersion, "pending", schemaVersion-current)
	started := time.Now()
	for version := current + 1; version <= schemaVersion; version++ {
		migrationStarted := time.Now()
		slog.Info("applying migration", "version", version, "of", schemaVersion)
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin migration %d: %w", version, err)
		}
		if _, err := tx.ExecContext(ctx, migrations[version-1]); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("apply migration %d: %w", version, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations(version, applied_at) VALUES(?, ?)`,
			version, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("record migration %d: %w", version, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %d: %w", version, err)
		}
		slog.Info("migration applied",
			"version", version, "elapsed", time.Since(migrationStarted).Round(time.Millisecond).String())
	}
	slog.Info("database migrations complete",
		"schema", schemaVersion,
		"applied", schemaVersion-current,
		"elapsed", time.Since(started).Round(time.Millisecond).String())
	return nil
}
