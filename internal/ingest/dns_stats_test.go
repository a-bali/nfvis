package ingest

import (
	"context"
	"testing"
	"time"

	"github.com/a-bali/nfvis/internal/config"
	"github.com/a-bali/nfvis/internal/model"
	"github.com/a-bali/nfvis/internal/store"
)

// storeWithOneFlowForDNS opens a store and inserts one flow so DNS annotations
// have a row to update.
func storeWithOneFlowForDNS(t *testing.T) (*store.Store, int64) {
	t.Helper()
	cfg := config.Default()
	cfg.Database.Path = t.TempDir() + "/dns.db"
	st, err := store.Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	now := time.Now().UTC()
	f := model.Flow{
		ReceivedAtMS: now.UnixMilli(), StartMS: now.UnixMilli(), EndMS: now.UnixMilli(),
		TimeSource: "start", Version: 9,
		Exporter: "192.0.2.1", ExporterIP: "192.0.2.1",
		InputIf: 2, OutputIf: 7,
		SrcAddr: "10.0.0.1", DstAddr: "10.0.0.2",
		SrcPort: 1024, DstPort: 443, Protocol: 6,
		Bytes: 100, Packets: 1,
	}
	if err := st.InsertBatch(context.Background(), []model.Flow{f}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	var id int64
	if err := st.DB().QueryRow(`SELECT MAX(id) FROM flows`).Scan(&id); err != nil {
		t.Fatalf("read id: %v", err)
	}
	return st, id
}

// TestSaveDNSBatchReportsWhatItWrote pins the number the DNS counters depend on.
//
// An earlier verification counted only `completed` — incremented after
// SaveDNSBatch returned no error — so a guard bug that silently wrote zero rows
// looked perfectly healthy: 11,960 completed with nothing stamped. The store now
// reports its write count, and this test fixes the values so the statistic cannot
// drift back to "the call did not error".
func TestSaveDNSBatchReportsWhatItWrote(t *testing.T) {
	st, id := storeWithOneFlowForDNS(t)
	ctx := context.Background()
	now := time.Now().UTC()
	hourMS := (now.UnixMilli() / 3600000) * 3600000

	// First observation of an unresolved address: an unstamped row must be
	// written even though the name is empty.
	wrote, err := st.SaveDNSBatch(ctx, []store.DNSObservation{
		{ID: id, IP: "10.0.0.1", Direction: "source", Name: "", HourMS: hourMS, CompletedMS: now.UnixMilli()},
	})
	if err != nil {
		t.Fatalf("first batch: %v", err)
	}
	if wrote != 1 {
		t.Fatalf("first batch reported written=%d, want 1: an unresolved first lookup must stamp "+
			"the row, else 'never looked up' and 'no PTR' are indistinguishable", wrote)
	}

	// Identical repeat: skipped, so written must be 0. This is the case that makes
	// the counter useful — a batch that does nothing is not a failure.
	wrote, err = st.SaveDNSBatch(ctx, []store.DNSObservation{
		{ID: id, IP: "10.0.0.1", Direction: "source", Name: "", HourMS: hourMS, CompletedMS: now.Add(time.Second).UnixMilli()},
	})
	if err != nil {
		t.Fatalf("second batch: %v", err)
	}
	if wrote != 0 {
		t.Errorf("repeat batch reported written=%d, want 0", wrote)
	}

	// Both endpoints in one call.
	wrote, err = st.SaveDNSBatch(ctx, []store.DNSObservation{
		{ID: id, IP: "10.0.0.1", Direction: "source", Name: "a.lan", HourMS: hourMS, CompletedMS: now.UnixMilli()},
		{ID: id, IP: "10.0.0.2", Direction: "destination", Name: "b.lan", HourMS: hourMS, CompletedMS: now.UnixMilli()},
	})
	if err != nil {
		t.Fatalf("third batch: %v", err)
	}
	if wrote != 2 {
		t.Errorf("two-endpoint batch reported written=%d, want 2", wrote)
	}
}
