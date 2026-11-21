package store

import (
	"context"
	"testing"
	"time"

	"github.com/balia/nfvis/internal/model"
)

// One batch exercising every outcome at once: a fresh record, a parent/child
// promotion, a NAT supersede (row created then deleted), a committed merge, and a
// dropped post-NAT view. The accounting identities and the id contract must hold
// exactly.
func TestMixedBatchAccountingIdentities(t *testing.T) {
	st := combinedStore(t)
	hour := time.Now().UTC().Truncate(time.Hour).Add(-4 * time.Hour)

	// Pre-existing committed state: a parent record whose child copy will arrive
	// in this batch (committed merge), and a post-NAT row of another packet that
	// this batch's pre-NAT record will supersede (created-then-deleted).
	committedParent := pingRecord(hour.Add(5*time.Minute), 2, "10.0.0.10", 1001)
	committedParent.DstAddr = "198.51.100.10"
	insertOne(t, st, committedParent)

	fresh := pingRecord(hour.Add(6*time.Minute), 2, "10.0.0.20", 1002)
	fresh.DstAddr = "198.51.100.20"

	childOfCommitted := pingRecord(hour.Add(5*time.Minute), 7, "10.0.0.10", 1001)
	childOfCommitted.DstAddr = "198.51.100.10"

	childCreatedThenPromoted := pingRecord(hour.Add(7*time.Minute), 7, "10.0.0.30", 1003)
	childCreatedThenPromoted.DstAddr = "198.51.100.30"
	parentOfCreated := pingRecord(hour.Add(7*time.Minute), 2, "10.0.0.30", 1003)
	parentOfCreated.DstAddr = "198.51.100.30"

	batch := []model.Flow{childOfCommitted, fresh, childCreatedThenPromoted, parentOfCreated}
	res, err := st.InsertBatchResult(context.Background(), batch)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if res.NewRows+res.Merged != len(batch) {
		t.Fatalf("NewRows(%d) + Merged(%d) = %d, want %d", res.NewRows, res.Merged, res.NewRows+res.Merged, len(batch))
	}
	var rows int64
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM flows`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	// committedParent, fresh, parentOfCreated (was childCreatedThenPromoted's row,
	// promoted in place) = 3.
	if rows != 3 {
		t.Fatalf("rows = %d, want 3", rows)
	}
	if int64(res.NewRows)+1 != rows {
		// NewRows excludes the one pre-existing committed row that the batch
		// merged into rather than created.
		t.Fatalf("NewRows = %d but %d rows exist (one was already committed)", res.NewRows, rows)
	}
	assertNoStaleIDs(t, st, res.IDs)
	assertAggregatesMatchFlows(t, st, "mixed batch")
}

// Every non-zero id in a batch that inserts, promotes and supersedes must resolve
// to a row holding that record's own endpoints, and a superseded record must have
// no id at all.
func TestIDContractUnderSupersedeAndPromote(t *testing.T) {
	st := combinedStore(t)
	hour := time.Now().UTC().Truncate(time.Hour).Add(-3 * time.Hour)

	// R3 child, R2 post-NAT, R1 parent: the order that used to leave two rows.
	recs := pingRecords(hour.Add(10 * time.Minute))
	batch := []model.Flow{recs[2], recs[1], recs[0]}
	res, err := st.InsertBatchResult(context.Background(), batch)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	var rows int64
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM flows`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("rows = %d, want 1", rows)
	}
	if res.NewRows != 1 || res.Merged != 2 {
		t.Fatalf("NewRows=%d Merged=%d, want 1 and 2", res.NewRows, res.Merged)
	}
	assertNoStaleIDs(t, st, res.IDs)
	// Positions 0 and 2 (both pre-NAT, same endpoints) must resolve to the one
	// surviving row; position 1 (the post-NAT view) must have no id.
	if res.IDs[0] == 0 || res.IDs[0] != res.IDs[2] {
		t.Fatalf("ids = %v, want positions 0 and 2 to report the surviving row", res.IDs)
	}
	if res.IDs[1] != 0 {
		t.Fatalf("post-NAT record got id %d, but no row holds its endpoints", res.IDs[1])
	}
	var src string
	if err := st.DB().QueryRow(`SELECT src_addr FROM flows WHERE id = ?`, res.IDs[0]).Scan(&src); err != nil {
		t.Fatal(err)
	}
	if src != natInnerSrc {
		t.Fatalf("surviving row src = %s, want %s", src, natInnerSrc)
	}
}
