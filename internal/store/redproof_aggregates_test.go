package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/a-bali/nfvis/internal/config"
	"github.com/a-bali/nfvis/internal/model"
)

// Black-box RED proof for the aggregate findings. It asserts two invariants that
// hold regardless of how duplicate suppression is configured or implemented:
//
//	a reverse-order NAT pair must not abort the batch, and
//	every aggregate family must equal the totals of the rows that exist.
//
// It uses only InsertBatch and SQL, so it runs unchanged against the unfixed
// tree. On the unfixed tree the second InsertBatch fails with
// "rebuild agg_protocol: UNIQUE constraint failed"; on the fixed tree no batch
// is lost and the aggregates agree with the rows.

func redNatStore(t *testing.T, wanSrc string) *Store {
	t.Helper()
	cfg := config.Default()
	cfg.Database.Path = t.TempDir() + "/nfvis.db"
	cfg.Database.Retention.Raw = config.Duration(24 * time.Hour)
	cfg.Database.Retention.Hourly = config.Duration(72 * time.Hour)
	cfg.Database.Dedup.NAT.WANAddresses = []string{wanSrc}
	st, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func TestBlackBoxAggregatesMatchRowsAfterReverseNAT(t *testing.T) {
	st := redNatStore(t, "192.168.0.2")
	hour := time.Now().UTC().Truncate(time.Hour).Add(-6 * time.Hour)

	// A bystander in the same hour that differs from the pair only in protocol:
	// its agg_protocol row is already committed when the pair is processed.
	bystander := pingRecord(hour.Add(2*time.Minute), 2, "10.0.0.61", 1234)
	bystander.Protocol = 17
	bystander.DstAddr, bystander.DstPort = "198.51.100.200", 53
	if err := st.InsertBatch(context.Background(), []model.Flow{bystander}); err != nil {
		t.Fatalf("insert bystander: %v", err)
	}

	end := hour.Add(30 * time.Minute)
	post := pingRecord(end.Add(700*time.Millisecond), 7, "192.168.0.2", 0)
	post.DstAddr = "198.51.100.42"
	if err := st.InsertBatch(context.Background(), []model.Flow{post}); err != nil {
		t.Fatalf("insert post-NAT: %v", err)
	}
	pre := pingRecord(end, 2, "10.10.10.194", 0)
	pre.DstAddr = "198.51.100.42"
	if err := st.InsertBatch(context.Background(), []model.Flow{pre}); err != nil {
		t.Fatalf("the reverse-order NAT replacement aborted the batch: %v", err)
	}

	var rows, rawBytes int64
	if err := st.DB().QueryRow(`SELECT COUNT(*), COALESCE(SUM(bytes),0) FROM flows`).Scan(&rows, &rawBytes); err != nil {
		t.Fatal(err)
	}
	// Whatever the policy decided, the bystander and the pre-NAT record are
	// unaffected records and must both be present.
	if rows < 2 {
		t.Fatalf("rows = %d, want at least 2 (the bystander and the pre-NAT record)", rows)
	}
	for _, table := range []string{"agg_exporter", "agg_protocol"} {
		var flows, bytes int64
		if err := st.DB().QueryRow(fmt.Sprintf(`SELECT COALESCE(SUM(flows),0), COALESCE(SUM(bytes),0) FROM %s`, table)).Scan(&flows, &bytes); err != nil {
			t.Fatal(err)
		}
		if flows != rows || bytes != rawBytes {
			t.Fatalf("%s says %d flows / %d bytes but the stored rows are %d / %d",
				table, flows, bytes, rows, rawBytes)
		}
	}
	// agg_traffic must agree per tier.
	rowsQ, err := st.DB().Query(`SELECT res_ms, COALESCE(SUM(flows),0), COALESCE(SUM(bytes),0) FROM agg_traffic GROUP BY res_ms`)
	if err != nil {
		t.Fatal(err)
	}
	defer rowsQ.Close()
	for rowsQ.Next() {
		var res, flows, bytes int64
		if err := rowsQ.Scan(&res, &flows, &bytes); err != nil {
			t.Fatal(err)
		}
		if flows != rows || bytes != rawBytes {
			t.Fatalf("agg_traffic res_ms=%d says %d flows / %d bytes, want %d / %d",
				res, flows, bytes, rows, rawBytes)
		}
	}
}
