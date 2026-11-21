package ingest

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-bali/nfvis/internal/config"
	"github.com/a-bali/nfvis/internal/model"
	"github.com/a-bali/nfvis/internal/store"
)

// The row ids returned for a batch must be positional against the batch, so the
// reverse-DNS lookup for each record is written onto the row that holds *that*
// record. The old contract returned a compacted list, which shifted every record
// after a collapsed one onto its neighbour's row.
func TestDNSLookupsAreAttachedToTheirOwnRow(t *testing.T) {
	cfg := dedupConfig(t)
	st, err := store.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	c := New(cfg, nil, st, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c.dns = &dnsWorker{jobs: make(chan dnsJob, 64)}

	now := time.Now().UTC().Truncate(time.Second)
	base := model.Flow{
		ReceivedAtMS: now.UnixMilli(), StartMS: now.UnixMilli() - 1000, EndMS: now.UnixMilli(),
		TimeSource: "exported", Version: 9, Exporter: "127.0.0.1", ExporterIP: "127.0.0.1",
		InputIf: 2, OutputIf: 1, SrcAddr: "10.0.0.1", DstAddr: "198.51.100.1", SrcPort: 1000,
		Protocol: 17, Bytes: 74, Packets: 1, SrcVLAN: -1, DstVLAN: -1,
	}
	child := base
	child.InputIf = 7 // the configured child of 2: this copy is merged
	// A third, distinct record after the merged one: under the old compact-id
	// contract its lookup would land on the merged record's row.
	third := base
	third.SrcAddr = "10.0.0.3"
	third.SrcPort = 3000
	third.DstAddr = "198.51.100.3"

	flows := []model.Flow{base, child, third}
	if err := c.insertWithRetry(context.Background(), flows); err != nil {
		t.Fatal(err)
	}
	close(c.dns.jobs)
	jobs := map[int64]string{}
	for job := range c.dns.jobs {
		if prev, ok := jobs[job.id]; ok && prev != job.flow.SrcAddr {
			t.Fatalf("row %d queued for both %s and %s", job.id, prev, job.flow.SrcAddr)
		}
		jobs[job.id] = job.flow.SrcAddr
	}
	if len(jobs) != 2 {
		t.Fatalf("queued %d rows, want 2 (the merged copy shares the parent's row)", len(jobs))
	}
	for id, src := range jobs {
		var storedSrc string
		if err := st.DB().QueryRow(`SELECT src_addr FROM flows WHERE id = ?`, id).Scan(&storedSrc); err != nil {
			t.Fatalf("id %d is not a stored row: %v", id, err)
		}
		if storedSrc != src {
			t.Fatalf("DNS lookup for %s queued against row %d, which is %s", src, id, storedSrc)
		}
	}

	// Counters: two rows created, one record merged, nothing dropped.
	if got := c.Counters().FlowsStored.Load(); got != 2 {
		t.Fatalf("flows_stored = %d, want 2", got)
	}
	if got := c.Counters().FlowsDuplicates.Load(); got != 1 {
		t.Fatalf("flows_duplicates = %d, want 1", got)
	}
	if got := c.Counters().FlowsDropped.Load(); got != 0 {
		t.Fatalf("flows_dropped = %d, want 0", got)
	}
}

// With no topology configured, every record is stored and every lookup goes to
// its own row: nothing is merged, so no id can be reused.
func TestDNSLookupsAllRowsWithoutTopology(t *testing.T) {
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "t.db")
	cfg.DNS.Enabled = false
	st, err := store.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	c := New(cfg, nil, st, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c.dns = &dnsWorker{jobs: make(chan dnsJob, 64)}

	now := time.Now().UTC()
	mk := func(src string, port uint16) model.Flow {
		return model.Flow{ReceivedAtMS: now.UnixMilli(), EndMS: now.UnixMilli(), TimeSource: "received",
			Exporter: "192.0.2.10", SrcAddr: src, SrcPort: port}
	}
	flows := []model.Flow{mk("10.0.0.1", 1), mk("10.0.0.1", 1), mk("10.0.0.3", 3)}
	if err := c.insertWithRetry(context.Background(), flows); err != nil {
		t.Fatal(err)
	}
	close(c.dns.jobs)
	queued := 0
	for range c.dns.jobs {
		queued++
	}
	if queued != 3 {
		t.Fatalf("queued %d lookups, want 3 (identical records are both stored)", queued)
	}
	if got := c.Counters().FlowsDuplicates.Load(); got != 0 {
		t.Fatalf("flows_duplicates = %d, want 0", got)
	}
}
