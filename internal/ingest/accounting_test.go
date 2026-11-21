package ingest

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/a-bali/nfvis/internal/config"
	"github.com/a-bali/nfvis/internal/model"
	"github.com/a-bali/nfvis/internal/store"
)

// natDedupConfig is a collector configuration with NAT suppression only: the
// firewall reports the pre-NAT view from input 2 and the post-NAT view from
// input 7.
func natDedupConfig(t *testing.T) config.Config {
	t.Helper()
	cfg := dedupConfig(t)
	cfg.Database.Dedup.ParentChild = false
	cfg.Database.Dedup.ParentChildLinks = nil
	cfg.Database.Dedup.NAT.WANAddresses = []string{"192.168.0.2"}
	cfg.Database.Dedup.NAT.Exporters = []config.NATExporter{
		{Address: "127.0.0.1", PreInterface: 2, PostInterface: 7},
	}
	return cfg
}

// natFlow is one view of a forwarded packet as the decoder would produce it.
func natFlow(end time.Time, in uint32, src string, sport uint16, dst string) model.Flow {
	return model.Flow{
		ReceivedAtMS: end.UnixMilli() + 20, StartMS: end.UnixMilli() - 1000, EndMS: end.UnixMilli(),
		TimeSource: "exported", Version: 9, Exporter: "127.0.0.1", ExporterIP: "127.0.0.1",
		InputIf: in, OutputIf: 1, SrcAddr: src, SrcPort: sport, DstAddr: dst, DstPort: 443,
		Protocol: 6, Bytes: 1500, Packets: 2, SrcVLAN: -1, DstVLAN: -1,
		SrcVLANSource: "unknown", DstVLANSource: "unknown", Direction: 1, DirectionKnown: true,
	}
}

// A batch whose post-NAT view is inserted and then superseded by its pre-NAT mate
// must report one stored flow and one duplicate — not two stored, and not one of
// each with a row that no longer exists.
func TestCollectorAccountingForSameBatchReplacement(t *testing.T) {
	cfg := natDedupConfig(t)
	st, err := store.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	c := New(cfg, nil, st, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	end := time.Now().UTC().Truncate(time.Second)
	post := natFlow(end.Add(700*time.Millisecond), 7, "192.168.0.2", 40000, "198.51.100.9")
	pre := natFlow(end, 2, "10.0.0.194", 40000, "198.51.100.9")

	if err := c.insertWithRetry(context.Background(), []model.Flow{post, pre}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	var rows int64
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM flows`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("rows = %d, want 1", rows)
	}
	if got := c.Counters().FlowsStored.Load(); got != 1 {
		t.Fatalf("flows_stored = %d, want 1 (the superseded row was deleted in the batch)", got)
	}
	if got := c.Counters().FlowsDuplicates.Load(); got != 1 {
		t.Fatalf("flows_duplicates = %d, want 1 (the post-NAT view was superseded)", got)
	}
	if got := c.Counters().FlowsDropped.Load(); got != 0 {
		t.Fatalf("flows_dropped = %d, want 0", got)
	}
}

// The same shape with a bystander: two rows exist, one created, one superseded.
func TestCollectorAccountingWithBystander(t *testing.T) {
	cfg := natDedupConfig(t)
	st, err := store.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	c := New(cfg, nil, st, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	end := time.Now().UTC().Truncate(time.Second)
	bystander := natFlow(end, 2, "10.0.0.77", 777, "198.51.100.77")
	post := natFlow(end.Add(700*time.Millisecond), 7, "192.168.0.2", 40000, "198.51.100.9")
	pre := natFlow(end, 2, "10.0.0.194", 40000, "198.51.100.9")

	if err := c.insertWithRetry(context.Background(), []model.Flow{bystander, post, pre}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	var rows int64
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM flows`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 2 {
		t.Fatalf("rows = %d, want 2", rows)
	}
	if got := c.Counters().FlowsStored.Load(); got != 2 {
		t.Fatalf("flows_stored = %d, want 2", got)
	}
	if got := c.Counters().FlowsDuplicates.Load(); got != 1 {
		t.Fatalf("flows_duplicates = %d, want 1", got)
	}
}

// A DNS lookup queued for a record whose row was deleted later in the same batch
// must not be attached to anything: the queue is keyed by row id, and a deleted
// id would either write nothing or — worse — be reused by SQLite.
func TestCollectorDoesNotQueueDNSForDeletedRows(t *testing.T) {
	cfg := natDedupConfig(t)
	st, err := store.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	c := New(cfg, nil, st, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c.dns = &dnsWorker{jobs: make(chan dnsJob, 64)}

	end := time.Now().UTC().Truncate(time.Second)
	post := natFlow(end.Add(700*time.Millisecond), 7, "192.168.0.2", 40000, "198.51.100.9")
	pre := natFlow(end, 2, "10.0.0.194", 40000, "198.51.100.9")
	bystander := natFlow(end, 2, "10.0.0.77", 777, "198.51.100.77")

	if err := c.insertWithRetry(context.Background(), []model.Flow{post, pre, bystander}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	close(c.dns.jobs)

	postIDs := map[int64]string{} // id -> src the caller believes is there
	for job := range c.dns.jobs {
		if prev, ok := postIDs[job.id]; ok && prev != job.flow.SrcAddr {
			t.Fatalf("row %d queued for both %s and %s", job.id, prev, job.flow.SrcAddr)
		}
		postIDs[job.id] = job.flow.SrcAddr
	}
	if len(postIDs) != 2 {
		t.Fatalf("queued %d rows, want 2 (no job for the superseded record)", len(postIDs))
	}
	for id, src := range postIDs {
		var stored srcRow
		if err := st.DB().QueryRow(`SELECT src_addr FROM flows WHERE id = ?`, id).Scan(&stored.src); err != nil {
			t.Fatalf("queued against id %d which has no row: %v", id, err)
		}
		if stored.src != src {
			t.Fatalf("queued for %s against id %d which holds %s", src, id, stored.src)
		}
	}
}

type srcRow struct{ src string }
