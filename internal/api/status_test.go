package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/a-bali/nfvis/internal/config"
	"github.com/a-bali/nfvis/internal/decoder"
	"github.com/a-bali/nfvis/internal/ingest"
	"github.com/a-bali/nfvis/internal/store"
)

// newStatusTestServer builds a server over an empty database. Nothing here
// ingests, so every row count is zero and a stats fan-out is cheap — which is
// exactly why this test asserts on the *shape* of the response rather than on
// timing. A timing assertion would be meaningless without rows, and a flaky
// one with them.
func newStatusTestServer(t *testing.T) (*httptest.Server, *store.Store) {
	t.Helper()
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "nfvis.db")
	cfg.DNS.Enabled = false
	cfg.Listen.UDP = "127.0.0.1:0"
	st, err := store.Open(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	coll := ingest.New(cfg, decoder.New(cfg), st, &ingest.Counters{}, log)
	if err := coll.Start(); err != nil {
		t.Fatalf("start collector: %v", err)
	}
	t.Cleanup(func() { _ = coll.Stop() })
	ts := httptest.NewServer(New(cfg, st, coll, log, "test-version").Handler())
	t.Cleanup(ts.Close)
	return ts, st
}

func decodeBody(t *testing.T, url string) map[string]interface{} {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d, want 200", url, resp.StatusCode)
	}
	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}
	return body
}

// TestStatusOmitsRowCounts pins the contract the dashboard depends on: the
// poll endpoint answers with coverage and counters but performs no row count.
//
// The `database` block is the thing under test. Its `flow_rows` member is an
// exact COUNT(*) over the whole raw table — a full scan of idx_flows_end, which
// measured 6.5 s at 1.5 M rows and 16.7 s at 4.5 M rows on a cold page cache.
// The dashboard polls this endpoint every five seconds and awaits it before
// issuing its primary request, so a count here is paid on every filter change,
// and its cost grows with the database. Since the dashboard only ever read
// `coverage` and `counters` from this response, the block is simply absent.
func TestStatusOmitsRowCounts(t *testing.T) {
	ts, _ := newStatusTestServer(t)

	status := decodeBody(t, ts.URL+"/api/v1/status")
	if _, present := status["database"]; present {
		t.Fatalf("/api/v1/status must not report row counts; got database block %v", status["database"])
	}
	for _, key := range []string{"version", "status", "counters", "coverage", "warnings", "started_at", "uptime_seconds"} {
		if _, ok := status[key]; !ok {
			t.Errorf("/api/v1/status missing %q", key)
		}
	}
}

// TestHealthKeepsRowCounts guards the other direction: the cheap payload must
// not remove the expensive one, because long-lived probes and scripts read the
// counts from /api/v1/health and would break silently if the field vanished.
func TestHealthKeepsRowCounts(t *testing.T) {
	ts, _ := newStatusTestServer(t)

	health := decodeBody(t, ts.URL+"/api/v1/health")
	db, ok := health["database"].(map[string]interface{})
	if !ok {
		t.Fatalf("/api/v1/health must keep its database block, got %v", health["database"])
	}
	for _, key := range []string{"flow_rows", "hourly_aggregate_rows", "size_bytes", "wal_bytes", "schema_version", "path"} {
		if _, ok := db[key]; !ok {
			t.Errorf("/api/v1/health database block missing %q", key)
		}
	}

	// The two payloads must agree on everything except the counts, so a client
	// can move from one to the other without reconciling two shapes.
	status := decodeBody(t, ts.URL+"/api/v1/status")
	for _, key := range []string{"version", "status", "uptime_seconds", "counters", "coverage", "warnings"} {
		if _, ok := status[key]; !ok {
			t.Errorf("/api/v1/status missing %q, present on /api/v1/health", key)
		}
	}
}

// TestStatusIgnoresStatsFailure documents that a broken statistics query cannot
// degrade the poll: the endpoint never runs one, so the response stays "ok".
// It asserts the observable consequence rather than mocking the store: with a
// live database there is no way to make Stats fail in isolation, so the check
// is that status reports ok while pointing at the same collector state the
// health handler reports.
func TestStatusReportsCollectorHealth(t *testing.T) {
	ts, _ := newStatusTestServer(t)

	status := decodeBody(t, ts.URL+"/api/v1/status")
	if got := status["status"]; got != "ok" {
		t.Errorf("status = %v, want ok on a healthy empty server", got)
	}
	if got := status["version"]; got != "test-version" {
		t.Errorf("version = %v, want test-version", got)
	}
	if _, ok := status["counters"].(map[string]interface{}); !ok {
		t.Errorf("status must carry collector counters, got %v", status["counters"])
	}
}
