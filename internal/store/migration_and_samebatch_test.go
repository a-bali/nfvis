package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-bali/nfvis/internal/config"
	"github.com/a-bali/nfvis/internal/model"
)

// Upgrading a v4 database must convert the unique payload index into the lookup
// index without touching existing rows, and must not re-collapse anything: rows
// already collapsed by v4 stay collapsed (their duplicates were never stored and
// cannot be reconstructed), and rows that already exist keep their keys.
func TestMigration5ReplacesUniqueDedupIndex(t *testing.T) {
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "nfvis.db")
	cfg.Database.Retention.Raw = config.Duration(24 * time.Hour)
	cfg.Database.Retention.Hourly = config.Duration(72 * time.Hour)
	// Dedup on, so the row carries a key: keys are only written when a mechanism
	// is configured, which is also why enabling one later never retroactively
	// collapses older rows.
	cfg.Database.Dedup.ParentChild = true
	cfg.Database.Dedup.ParentChildLinks = []config.ParentChildLink{
		{Exporter: "192.0.2.10", Parent: 2, Child: 7},
	}
	st, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Two rows that would violate the v4 unique index: same key, written while the
	// index was plain. This is the state v5 has to be able to hold.
	end := time.Now().UTC().Truncate(time.Second)
	rec := pingRecord(end, 2, "10.10.10.194", 3535)
	key := flowDedupKey(&rec)
	if err := st.InsertBatch(context.Background(), []model.Flow{rec}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	// Recreate the v4 index on the current data, which has one keyed row.
	if _, err := st.DB().Exec(`DROP INDEX IF EXISTS idx_flows_dedup_key_lookup;
		CREATE UNIQUE INDEX idx_flows_dedup_key ON flows(dedup_key) WHERE dedup_key IS NOT NULL;
		DELETE FROM schema_migrations WHERE version >= 5;`); err != nil {
		t.Fatalf("simulate v4 schema: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := Open(cfg)
	if err != nil {
		t.Fatalf("reopen (migration 5): %v", err)
	}
	defer reopened.Close()

	// The unique index is gone and the lookup index exists.
	var unique, lookup int
	if err := reopened.DB().QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_flows_dedup_key'`).Scan(&unique); err != nil {
		t.Fatal(err)
	}
	if err := reopened.DB().QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_flows_dedup_key_lookup'`).Scan(&lookup); err != nil {
		t.Fatal(err)
	}
	if unique != 0 || lookup != 1 {
		t.Fatalf("indexes after migration: unique=%d lookup=%d, want 0 and 1", unique, lookup)
	}
	// The existing row kept its key.
	var stored []byte
	if err := reopened.DB().QueryRow(`SELECT dedup_key FROM flows`).Scan(&stored); err != nil {
		t.Fatalf("read key: %v", err)
	}
	if len(stored) == 0 {
		t.Fatal("migration dropped the existing dedup key")
	}
	// A second identical record can now be stored, which the unique index forbade.
	if err := reopened.InsertBatch(context.Background(), []model.Flow{rec}); err != nil {
		t.Fatalf("insert identical record after migration: %v", err)
	}
	if got := countRows(t, reopened, "flows"); got != 2 {
		t.Fatalf("rows = %d, want 2", got)
	}
	_ = key
}

// A batch that has to replace a stored row while other rows of the same batch are
// already in the transaction must keep every aggregate family exact. This is the
// same-batch double-count shape.
func TestSameBatchReplacementAggregatesAreExact(t *testing.T) {
	st := natStore(t)
	hour := time.Now().UTC().Truncate(time.Hour).Add(-7 * time.Hour)
	end := hour.Add(20 * time.Minute)

	batch := []model.Flow{postNATRecord(end)}
	batch[0].DstAddr = "203.0.113.77"
	for i := 0; i < 20; i++ {
		f := pingRecord(end.Add(time.Duration(i)*time.Millisecond), 2, "10.10.10.194", uint16(2000+i))
		f.DstAddr = "203.0.113.100"
		batch = append(batch, f)
	}
	pre := preNATRecord(end)
	pre.DstAddr = batch[0].DstAddr
	batch = append(batch, pre)

	if err := st.InsertBatch(context.Background(), batch); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if got := countRows(t, st, "flows"); got != 21 {
		t.Fatalf("rows = %d, want 21 (20 unrelated + 1 surviving view)", got)
	}
	assertAggregatesMatchFlows(t, st, "same-batch replacement")
}

// Deleting a stored post-NAT row must not leave the row's sampled/unknown
// contributions behind in any family.
func TestReplacementClearsSampledAndUnknownContributions(t *testing.T) {
	st := natStore(t)
	end := time.Now().UTC().Truncate(time.Second)

	post := postNATRecord(end)
	post.Sampled = true
	post.InputIf = 7
	insertOne(t, st, post)
	pre := preNATRecord(end)
	insertOne(t, st, pre)

	assertAggregatesMatchFlows(t, st, "sampled replacement")

	var sampled int64
	if err := st.DB().QueryRow(`SELECT COALESCE(SUM(sampled_flows),0) FROM agg_exporter`).Scan(&sampled); err != nil {
		t.Fatal(err)
	}
	// The surviving pre-NAT record is not sampled, so the sampled count must be 0.
	if sampled != 0 {
		t.Fatalf("agg_exporter still counts %d sampled flows for a non-sampled survivor", sampled)
	}
}
