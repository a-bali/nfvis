package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-bali/nfvis/internal/config"
	"github.com/a-bali/nfvis/internal/model"
)

// storeWith opens a store whose dedup configuration the caller controls.
func storeWith(t *testing.T, mutate func(*config.Config)) *Store {
	t.Helper()
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "nfvis.db")
	cfg.Database.Retention.Raw = config.Duration(24 * time.Hour)
	cfg.Database.Retention.Hourly = config.Duration(72 * time.Hour)
	if mutate != nil {
		mutate(&cfg)
	}
	st, err := Open(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// parent_child: false must disable the collapse, so an operator who needs every
// interface traversal preserved can have it.
func TestParentChildDedupCanBeDisabled(t *testing.T) {
	st := storeWith(t, func(c *config.Config) { c.Database.Dedup.ParentChild = false })
	end := time.Now().UTC().Truncate(time.Second)
	if err := st.InsertBatch(context.Background(), pingRecords(end)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	// parent, VLAN child and the post-NAT copy all survive.
	if got := countRows(t, st, "flows"); got != 3 {
		t.Fatalf("stored %d rows with parent_child disabled, want 3", got)
	}
	// And the interface aggregate counts both traversals.
	var ingress int64
	if err := st.DB().QueryRow(`SELECT COALESCE(SUM(flows),0) FROM agg_interface WHERE direction='ingress' AND if_index IN (2,7)`).Scan(&ingress); err != nil {
		t.Fatal(err)
	}
	if ingress != 3 {
		t.Fatalf("ingress flows for if 2 and 7 = %d, want 3 (2 parent + 1 child)", ingress)
	}
}

// With dedup off, a repeated identical record is stored twice: the feature is
// genuinely bypassed rather than silently always-on.
func TestParentChildDisabledStoresVerbatimDuplicates(t *testing.T) {
	st := storeWith(t, func(c *config.Config) { c.Database.Dedup.ParentChild = false })
	end := time.Now().UTC().Truncate(time.Second)
	rec := pingRecord(end, 2, "10.10.10.194", 0)
	if err := st.InsertBatch(context.Background(), []model.Flow{rec, rec}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if got := countRows(t, st, "flows"); got != 2 {
		t.Fatalf("stored %d rows with dedup disabled, want 2", got)
	}
}
