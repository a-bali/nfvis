package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/a-bali/nfvis/internal/config"
	"github.com/a-bali/nfvis/internal/model"
)

// These two tests are the black-box RED proof for the payload-identity and
// batch-id findings. They touch only APIs and configuration fields that exist
// both before and after the fix, so the same file can be dropped into the
// unfixed tree and must fail there:
//
//	git stash && go test ./internal/store/ -run 'TestBlackBox'
//
// Both cases are stated as invariants, not as implementation details: "two
// records the store cannot prove are the same packet are both stored", and "the
// id for a batch position belongs to that position's record".

// Two byte-identical records concerning the same interface are not provably the
// same packet: the exporter can legitimately report a repeated flow, and a
// record whose interface is not part of a declared link is not a copy of
// anything. Both must be stored.
func TestBlackBoxIdenticalRecordsArePreserved(t *testing.T) {
	cfg := config.Default()
	cfg.Database.Path = t.TempDir() + "/nfvis.db"
	cfg.Database.Retention.Raw = config.Duration(24 * time.Hour)
	cfg.Database.Retention.Hourly = config.Duration(72 * time.Hour)
	st, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()

	end := time.Now().UTC().Truncate(time.Second)
	rec := pingRecord(end, 2, "10.10.10.194", 3535)
	if err := st.InsertBatch(context.Background(), []model.Flow{rec, rec}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	var n int64
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM flows`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("stored %d of 2 identical records: the store discarded a record it cannot prove is a duplicate", n)
	}
}

// InsertBatchIDs must be positional against the batch even when records merge,
// because the caller attaches reverse-DNS results by position.
func TestBlackBoxBatchIDsArePositional(t *testing.T) {
	cfg := config.Default()
	cfg.Database.Path = t.TempDir() + "/nfvis.db"
	cfg.Database.Retention.Raw = config.Duration(24 * time.Hour)
	cfg.Database.Retention.Hourly = config.Duration(72 * time.Hour)
	st, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()

	end := time.Now().UTC().Truncate(time.Second)
	a := pingRecord(end, 2, "10.10.10.1", 1000)
	a.DstAddr = "198.51.100.1"
	b := a
	c := pingRecord(end, 2, "10.10.10.3", 3000)
	c.DstAddr = "198.51.100.3"

	batch := []model.Flow{a, b, c}
	ids, err := st.InsertBatchIDs(context.Background(), batch)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if len(ids) != len(batch) {
		t.Fatalf("returned %d ids for %d records: the caller cannot tell which id belongs to which record", len(ids), len(batch))
	}
	for i, id := range ids {
		if id == 0 {
			continue
		}
		var src string
		if err := st.DB().QueryRow(`SELECT src_addr FROM flows WHERE id = ?`, id).Scan(&src); err != nil {
			t.Fatalf("position %d returned id %d, which is not a stored row: %v", i, id, err)
		}
		if want := batch[i].SrcAddr; src != want {
			t.Fatalf("position %d is %s but its id %d holds %s: a DNS lookup would be written to the wrong row",
				i, want, id, src)
		}
	}
	_ = fmt.Sprint()
}
