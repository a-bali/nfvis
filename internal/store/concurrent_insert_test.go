package store

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/a-bali/nfvis/internal/config"
	"github.com/a-bali/nfvis/internal/model"
)

// TestConcurrentInsertBatchResultLosesNothing exercises the real ingest path —
// InsertBatchResult, which reads dedup candidates and then writes — from many
// goroutines at once, which is what happens in production when the DNS writers
// and the flow flusher contend.
//
// With a deferred transaction this loses whole batches to SQLITE_BUSY, and the
// collector answers that by discarding them (flows_dropped). With _txlock=
// immediate every batch commits. Combined with TestStoreUsesImmediateTxlock,
// this pins the fix to the code path that actually failed in production rather
// than to a synthetic read-then-write.
func TestConcurrentInsertBatchResultLosesNothing(t *testing.T) {
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "concurrent.db")
	cfg.Database.Dedup.ParentChild = true
	cfg.Database.Dedup.ParentChildLinks = []config.ParentChildLink{
		{Exporter: "192.0.2.10", Parent: 2, Child: 7},
	}
	st, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()

	const writers = 6
	const batches = 8
	const perBatch = 20

	var mu sync.Mutex
	var errs []error
	var stored, merged int

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for b := 0; b < batches; b++ {
				end := time.Now().UTC().Truncate(time.Second).Add(-time.Duration(w*batches+b) * time.Second)
				flows := make([]model.Flow, 0, perBatch)
				for i := 0; i < perBatch; i++ {
					// Distinct sources per goroutine+iteration so records are not
					// dedup candidates of each other; the read still happens.
					src := fmt.Sprintf("10.%d.%d.%d", 20+w, b, i)
					flows = append(flows, pingRecord(end, 2, src, uint16(3535+i)))
				}
				res, err := st.InsertBatchResult(context.Background(), flows)

				mu.Lock()
				if err != nil {
					errs = append(errs, err)
				} else {
					stored += res.NewRows
					merged += res.Merged
				}
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()

	if len(errs) > 0 {
		t.Fatalf("%d/%d concurrent InsertBatchResult calls failed, first: %v\n"+
			"These are the failures that make the collector discard whole batches "+
			"(flows_dropped). store.Open must request _txlock=immediate so busy_timeout "+
			"applies to the read→write upgrade.",
			len(errs), writers*batches, errs[0])
	}
	if want := writers * batches * perBatch; stored+merged != want {
		t.Errorf("accounted for %d flows (stored %d + merged %d), want %d",
			stored+merged, stored, merged, want)
	}
}
