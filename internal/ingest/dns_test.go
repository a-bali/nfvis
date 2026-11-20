package ingest

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/balia/nfvis/internal/config"
	dnsresolver "github.com/balia/nfvis/internal/dns"
	"github.com/balia/nfvis/internal/model"
	"github.com/balia/nfvis/internal/store"
)

func TestDNSWorkerPersistsHistoricalNamesAndMetrics(t *testing.T) {
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "test.db")
	cfg.DNS.Enabled = true
	cfg.Decoder.QueueSize = 32
	st, err := store.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := time.Now()
	flows := []model.Flow{{ReceivedAtMS: now.UnixMilli(), EndMS: now.UnixMilli(), TimeSource: "received", SrcAddr: "192.0.2.1"}, {ReceivedAtMS: now.UnixMilli(), EndMS: now.UnixMilli(), TimeSource: "received", SrcAddr: "192.0.2.1"}}
	ids, err := st.InsertBatchIDs(context.Background(), flows)
	if err != nil {
		t.Fatal(err)
	}
	c := New(cfg, nil, st, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c.initDNS()
	// Swap before queuing work; the fake avoids depending on external DNS.
	c.dns.resolver = dnsresolver.NewWithLookup(time.Hour, 100, time.Second, func(context.Context, string) ([]string, error) { return []string{"old.example."}, nil })
	c.queueDNS(ids, flows)
	deadline := time.Now().Add(5 * time.Second)
	for c.DNSStats().Completed < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	stats := c.DNSStats()
	if stats.Completed != 2 || stats.Named != 2 || stats.CacheHits+stats.CacheMisses != 2 {
		t.Fatalf("stats: %+v", stats)
	}
	close(c.dns.jobs)
	c.dns.wg.Wait()
	c.dns.cancel()
	page, err := st.Flows(context.Background(), store.FlowQuery{Range: store.Range{FromMS: now.Add(-time.Minute).UnixMilli(), ToMS: now.Add(time.Minute).UnixMilli()}, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range page.Rows {
		if row.SrcName != "old.example" || row.SrcDNSMS == 0 {
			t.Fatalf("row: %+v", row)
		}
	}
	names, err := st.HistoricalNames(context.Background(), store.Range{FromMS: now.Add(-time.Hour).UnixMilli(), ToMS: now.Add(time.Hour).UnixMilli()}, "source")
	if err != nil || len(names["192.0.2.1"]) != 1 {
		t.Fatalf("names %v %v", names, err)
	}
}
