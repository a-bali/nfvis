package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-bali/nfvis/internal/config"
	"github.com/a-bali/nfvis/internal/decoder"
	"github.com/a-bali/nfvis/internal/ingest"
	"github.com/a-bali/nfvis/internal/model"
	"github.com/a-bali/nfvis/internal/store"
)

// newLabelTestStore opens a store with reverse DNS enabled so DNS observations
// can be written and read back.
func newLabelTestStore(t *testing.T) (*store.Store, config.Config) {
	t.Helper()
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "labels.db")
	cfg.DNS.Enabled = true
	st, err := store.Open(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st, cfg
}

// TestSplitSeriesLabelUsesSingleName is the regression test for a legend that
// read "hostname, hostname" for one host.
//
// HistoricalNames returns every name observed for an IP inside the range,
// most-recent-first, because a PTR record can change during a window. The label
// code joined all of them with ", ", so one host whose name changed appeared as
// two hosts in the legend and the band list. A band is one host, so it must
// carry one name — the most recent.
func TestSplitSeriesLabelUsesSingleName(t *testing.T) {
	st, cfg := newLabelTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	hourMS := (now.UnixMilli() / 3600000) * 3600000

	// A real flow row is needed for SaveDNS's UPDATE to touch; the historical
	// row is written either way, but using a real row keeps the fixture honest.
	flow := model.Flow{
		ReceivedAtMS: now.UnixMilli(), StartMS: now.UnixMilli(), EndMS: now.UnixMilli(),
		TimeSource: "start", Version: 9,
		Exporter: "192.0.2.1", ExporterIP: "192.0.2.1",
		InputIf: 2, OutputIf: 7,
		SrcAddr: "10.0.0.5", DstAddr: "192.0.2.9",
		SrcPort: 1024, DstPort: 443, Protocol: 6,
		Bytes: 100, Packets: 1,
	}
	if err := st.InsertBatch(ctx, []model.Flow{flow}); err != nil {
		t.Fatalf("insert flow: %v", err)
	}

	// Two names for the same address: an older hour and the current one.
	observations := []store.DNSObservation{
		{ID: 1, IP: "10.0.0.5", Direction: "source", Name: "old-name.lan",
			HourMS: hourMS - 3600000, CompletedMS: now.Add(-2 * time.Hour).UnixMilli()},
		{ID: 1, IP: "10.0.0.5", Direction: "source", Name: "new-name.lan",
			HourMS: hourMS, CompletedMS: now.UnixMilli()},
	}
	for _, o := range observations {
		if err := st.SaveDNS(ctx, o); err != nil {
			t.Fatalf("save dns: %v", err)
		}
	}

	rng := store.Range{FromMS: now.Add(-24 * time.Hour).UnixMilli(), ToMS: now.Add(time.Hour).UnixMilli()}
	names, err := st.HistoricalNames(ctx, rng, "source")
	if err != nil {
		t.Fatalf("historical names: %v", err)
	}
	history := names["10.0.0.5"]
	if len(history) < 2 {
		t.Fatalf("fixture did not produce two names for one IP, got %v", history)
	}

	a := &Server{cfg: cfg, store: st, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	result := store.SplitResult{
		SplitBy: "src_host",
		Range:   rng,
		Series:  []store.SplitSeries{{Key: "10.0.0.5"}},
	}
	a.labelSplitSeries(ctx, &result)

	label := result.Series[0].Label
	if label == "" {
		t.Fatalf("label empty; expected the most recent name, history was %v", history)
	}
	if strings.Contains(label, ",") {
		t.Errorf("label %q joins multiple names; one band is one host (history %v)", label, history)
	}
	// Assert the literal newest name, not history[0]. Comparing against history[0]
	// asserts only that the output matches whatever HistoricalNames returned
	// first — which is true for ORDER BY DESC *and* for ASC. The earlier version
	// of this test passed with the query flipped to ASC, i.e. it would not have
	// caught the fix picking the oldest name.
	if label != "new-name.lan" {
		t.Errorf("label = %q, want the most recent name %q (history %v)", label, "new-name.lan", history)
	}
}

// TestHistoricalNamesNewestFirst pins the ordering the labelling relies on.
//
// The label fix takes found[0] on the assumption that the query returns names
// most-recent-first. That assumption is load-bearing and was previously only
// implied by the label test comparing against history[0], which is circular.
func TestHistoricalNamesNewestFirst(t *testing.T) {
	st, _ := newLabelTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	hourMS := (now.UnixMilli() / 3600000) * 3600000

	// SaveDNS only writes the historical row when its UPDATE matched a flow, so
	// the fixture needs a real row first.
	if err := st.InsertBatch(ctx, []model.Flow{{
		ReceivedAtMS: now.UnixMilli(), StartMS: now.UnixMilli(), EndMS: now.UnixMilli(),
		TimeSource: "start", Version: 9,
		Exporter: "192.0.2.1", ExporterIP: "192.0.2.1",
		InputIf: 2, OutputIf: 7,
		SrcAddr: "10.0.0.7", DstAddr: "192.0.2.9",
		SrcPort: 1024, DstPort: 443, Protocol: 6,
		Bytes: 100, Packets: 1,
	}}); err != nil {
		t.Fatalf("insert flow: %v", err)
	}

	// Two names for one address, written oldest-last so insertion order cannot
	// accidentally satisfy the assertion.
	if err := st.SaveDNS(ctx, store.DNSObservation{
		ID: 1, IP: "10.0.0.7", Direction: "source", Name: "newer.lan",
		HourMS: hourMS, CompletedMS: now.UnixMilli(),
	}); err != nil {
		t.Fatalf("save newer: %v", err)
	}
	if err := st.SaveDNS(ctx, store.DNSObservation{
		ID: 1, IP: "10.0.0.7", Direction: "source", Name: "older.lan",
		HourMS: hourMS - 3600000, CompletedMS: now.Add(-2 * time.Hour).UnixMilli(),
	}); err != nil {
		t.Fatalf("save older: %v", err)
	}

	names, err := st.HistoricalNames(ctx, store.Range{
		FromMS: now.Add(-24 * time.Hour).UnixMilli(),
		ToMS:   now.Add(time.Hour).UnixMilli(),
	}, "source")
	if err != nil {
		t.Fatalf("historical names: %v", err)
	}
	got := names["10.0.0.7"]
	if len(got) < 2 {
		t.Fatalf("expected two names for one address, got %v", got)
	}
	if got[0] != "newer.lan" {
		t.Errorf("HistoricalNames returned %v; index 0 must be the most recently seen name "+
			"because the labels take it as the current one", got)
	}
}

// TestSplitSeriesLabelIgnoresOtherBand checks the "Other" fold keeps its own
// label rather than being renamed after whichever IP it absorbed.
func TestSplitSeriesLabelIgnoresOtherBand(t *testing.T) {
	st, cfg := newLabelTestStore(t)
	a := &Server{cfg: cfg, store: st, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	result := store.SplitResult{
		SplitBy: "src_host",
		Series:  []store.SplitSeries{{Key: "10.0.0.9", Other: true, Label: "Other"}},
	}
	a.labelSplitSeries(context.Background(), &result)
	if result.Series[0].Label != "Other" {
		t.Errorf("Other band label = %q, want %q", result.Series[0].Label, "Other")
	}
}

// TestSystemIncludesAPIMetrics proves the System response carries the block the
// UI section reads, and that requests to /api/ are counted while static assets
// are not.
func TestSystemIncludesAPIMetrics(t *testing.T) {
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "api-metrics.db")
	cfg.Listen.UDP = "127.0.0.1:0"
	st, err := store.Open(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	coll := ingest.New(cfg, decoder.New(cfg), st, &ingest.Counters{}, log)
	if err := coll.Start(); err != nil {
		t.Fatalf("start collector: %v", err)
	}
	defer coll.Stop()
	srv := New(cfg, st, coll, log, "test-version")

	handler := srv.Handler()
	for i := 0; i < 3; i++ {
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/v1/health", nil))
	}
	// A static asset must not be counted.
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/system", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}

	var body struct {
		API struct {
			WindowSeconds int64   `json:"window_seconds"`
			Requests      int64   `json:"requests"`
			RequestsTotal uint64  `json:"requests_total"`
			DurationMS    summary `json:"duration_ms"`
			SizeBytes     summary `json:"size_bytes"`
		} `json:"api"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.API.WindowSeconds != 86400 {
		t.Errorf("window_seconds = %d, want 86400", body.API.WindowSeconds)
	}
	// The three health calls plus this system call are API requests; the "/"
	// fetch is not counted, so requests must be exactly 3 at snapshot time
	// (the system response excludes itself).
	if body.API.Requests != 3 {
		t.Errorf("requests = %d, want 3 (the static asset must not be counted)", body.API.Requests)
	}
	if body.API.SizeBytes.Median <= 0 {
		t.Errorf("size median = %v, want > 0", body.API.SizeBytes.Median)
	}
	if body.API.DurationMS.Max <= 0 {
		t.Errorf("duration max = %v, want > 0", body.API.DurationMS.Max)
	}
}

type summary struct {
	Avg    float64 `json:"avg"`
	Median float64 `json:"median"`
	P90    float64 `json:"p90"`
	Max    float64 `json:"max"`
}
