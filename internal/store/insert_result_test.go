package store

import (
	"context"
	"testing"
	"time"

	"github.com/balia/nfvis/internal/model"
)

// InsertResult accounting and positional ids.
//
// Semantics this file pins down:
//
//	IDs[i]  — the row that holds record i *after the batch commits*, or 0 when no
//	          row holds it (a dropped non-equivalent view such as a post-NAT copy).
//	          Never an id of a row the batch deleted.
//	NewRows — rows this batch created that still exist at commit. A row created
//	          and then deleted or replaced inside the same batch is not counted.
//	Merged  — records that did not end up as their own new row: dropped views,
//	          promotions into an existing row, and records whose row was replaced
//	          by a later record in the batch.
//	          NewRows + Merged == len(flows).
//
// The invariant a caller can rely on: every non-zero IDs[i] resolves to a row
// that exists at commit and whose endpoints are the record's own.

// A record inserted and then superseded in the same batch leaves one row, not
// two, and the accounting says so.
func TestSameBatchReplacementAccounting(t *testing.T) {
	st := natStore(t)
	end := time.Now().UTC().Truncate(time.Second)

	res, err := st.InsertBatchResult(context.Background(), []model.Flow{postNATRecord(end), preNATRecord(end)})
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
	if res.NewRows != 1 {
		t.Fatalf("NewRows = %d, want 1 (the post-NAT row was created then deleted)", res.NewRows)
	}
	if res.Merged != 1 {
		t.Fatalf("Merged = %d, want 1 (the post-NAT record was superseded)", res.Merged)
	}
	if res.NewRows+res.Merged != 2 {
		t.Fatalf("NewRows+Merged = %d, want len(flows)=2", res.NewRows+res.Merged)
	}
	if int64(res.NewRows) != rows {
		t.Fatalf("NewRows = %d but %d rows exist: flows_stored would overcount", res.NewRows, rows)
	}
	// The superseded post-NAT record has a different source address than the
	// surviving pre-NAT rows, so it must carry no id at all.
	if res.IDs[0] != 0 {
		t.Fatalf("superseded post-NAT record got id %d, which was deleted mid-batch", res.IDs[0])
	}
	if res.IDs[1] == 0 {
		t.Fatal("the surviving pre-NAT record has no id")
	}
	assertNoStaleIDs(t, st, res.IDs)
}

// The same shape with a bystander in the batch: the bystander's id must not shift
// and its accounting must be unaffected.
func TestSameBatchReplacementAccountingWithBystander(t *testing.T) {
	st := natStore(t)
	end := time.Now().UTC().Truncate(time.Second)

	bystander := pingRecord(end, 2, "10.0.0.77", 777)
	bystander.DstAddr, bystander.DstPort = "198.51.100.77", 443
	bystander.Protocol, bystander.Bytes = 6, 1500

	post := postNATRecord(end)
	pre := preNATRecord(end)
	res, err := st.InsertBatchResult(context.Background(), []model.Flow{bystander, post, pre})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	var rows int64
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM flows`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 2 {
		t.Fatalf("rows = %d, want 2 (bystander + surviving pre-NAT)", rows)
	}
	if res.NewRows != 2 || res.Merged != 1 {
		t.Fatalf("NewRows=%d Merged=%d, want 2 and 1", res.NewRows, res.Merged)
	}
	if int64(res.NewRows) != rows {
		t.Fatalf("NewRows = %d but %d rows exist", res.NewRows, rows)
	}
	if res.IDs[0] == 0 {
		t.Fatal("bystander got no id")
	}
	if res.IDs[1] != 0 {
		t.Fatalf("superseded post-NAT record got id %d", res.IDs[1])
	}
	assertNoStaleIDs(t, st, res.IDs)
	assertIDsHoldTheirRecord(t, st, []model.Flow{bystander, post, pre}, res.IDs)
}

// A record whose row is created in this batch and then promoted by a later record
// must keep one row, and both records must report that row's id.
func TestSameBatchCreateThenPromoteAccounting(t *testing.T) {
	st := pcStore(t)
	end := time.Now().UTC().Truncate(time.Second)

	child := pingRecord(end, 7, "10.10.10.194", 3535)
	parent := pingRecord(end, 2, "10.10.10.194", 3535)
	res, err := st.InsertBatchResult(context.Background(), []model.Flow{child, parent})
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
	if res.NewRows != 1 || res.Merged != 1 {
		t.Fatalf("NewRows=%d Merged=%d, want 1 and 1", res.NewRows, res.Merged)
	}
	if res.IDs[0] == 0 || res.IDs[0] != res.IDs[1] {
		t.Fatalf("ids = %v, want both positions to report the one surviving row", res.IDs)
	}
	assertNoStaleIDs(t, st, res.IDs)
	assertIDsHoldTheirRecord(t, st, []model.Flow{child, parent}, res.IDs)
}

// A record that is dropped because an equivalent row is already committed maps to
// that committed row, which is what a DNS lookup must be written to.
func TestCommittedMergeReportsTheStoredRow(t *testing.T) {
	st := pcStore(t)
	end := time.Now().UTC().Truncate(time.Second)
	parent := pingRecord(end, 2, "10.10.10.194", 3535)
	insertOne(t, st, parent)

	var parentID int64
	if err := st.DB().QueryRow(`SELECT id FROM flows`).Scan(&parentID); err != nil {
		t.Fatal(err)
	}
	child := pingRecord(end, 7, "10.10.10.194", 3535)
	res, err := st.InsertBatchResult(context.Background(), []model.Flow{child})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if res.NewRows != 0 || res.Merged != 1 {
		t.Fatalf("NewRows=%d Merged=%d, want 0 and 1", res.NewRows, res.Merged)
	}
	if res.IDs[0] != parentID {
		t.Fatalf("id = %d, want the stored parent row %d", res.IDs[0], parentID)
	}
	assertNoStaleIDs(t, st, res.IDs)
}

// assertNoStaleIDs fails when any non-zero id does not resolve to a row at commit.
func assertNoStaleIDs(t *testing.T, st *Store, ids []int64) {
	t.Helper()
	for i, id := range ids {
		if id == 0 {
			continue
		}
		var exists int64
		if err := st.DB().QueryRow(`SELECT COUNT(*) FROM flows WHERE id = ?`, id).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists != 1 {
			t.Fatalf("ids[%d] = %d does not resolve to a row: a DNS lookup would be written to nothing", i, id)
		}
	}
}

// assertIDsHoldTheirRecord fails when an id resolves to a row whose endpoints are
// not the record's own.
func assertIDsHoldTheirRecord(t *testing.T, st *Store, flows []model.Flow, ids []int64) {
	t.Helper()
	if len(flows) != len(ids) {
		t.Fatalf("ids = %d entries for %d records", len(ids), len(flows))
	}
	for i, id := range ids {
		if id == 0 {
			continue
		}
		var src, dst string
		if err := st.DB().QueryRow(`SELECT src_addr, dst_addr FROM flows WHERE id = ?`, id).Scan(&src, &dst); err != nil {
			t.Fatalf("ids[%d] = %d: %v", i, id, err)
		}
		if src != flows[i].SrcAddr || dst != flows[i].DstAddr {
			t.Fatalf("ids[%d] = %d holds %s -> %s, but the record is %s -> %s",
				i, id, src, dst, flows[i].SrcAddr, flows[i].DstAddr)
		}
	}
}
