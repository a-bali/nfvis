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

// TestTopHostLabelIsOneName is the regression test for the Hosts view's Label
// column repeating a name: "talos2.lan, talos2.lan".
//
// HistoricalNames returns one row per (hour, host, direction, name), so a
// hostname that stayed the same across two hours of the requested range comes
// back twice. The top handler joined every entry with ", ", which turned a
// single host into a label that reads as two. This drives the handler the
// browser calls, so it covers the whole path: store query, DNS join, JSON.
func TestTopHostLabelIsOneName(t *testing.T) {
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "top-label.db")
	cfg.DNS.Enabled = true
	cfg.Listen.UDP = "127.0.0.1:0"
	st, err := store.Open(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	now := time.Now().UTC()
	hourMS := (now.UnixMilli() / 3600000) * 3600000
	// The same host sending traffic in two different hours.
	flows := []model.Flow{
		{ReceivedAtMS: now.UnixMilli(), StartMS: now.UnixMilli(), EndMS: now.UnixMilli(),
			TimeSource: "start", Version: 9, Exporter: "192.0.2.1", ExporterIP: "192.0.2.1",
			SrcAddr: "10.0.0.5", DstAddr: "192.0.2.9", SrcPort: 1024, DstPort: 443,
			Protocol: 6, Bytes: 100, Packets: 1},
		{ReceivedAtMS: now.Add(-time.Hour).UnixMilli(), StartMS: now.Add(-time.Hour).UnixMilli(),
			EndMS: now.Add(-time.Hour).UnixMilli(), TimeSource: "start", Version: 9,
			Exporter: "192.0.2.1", ExporterIP: "192.0.2.1",
			SrcAddr: "10.0.0.5", DstAddr: "192.0.2.9", SrcPort: 1025, DstPort: 443,
			Protocol: 6, Bytes: 200, Packets: 2},
	}
	if err := st.InsertBatch(context.Background(), flows); err != nil {
		t.Fatalf("insert flows: %v", err)
	}
	// One lookup per hour, both resolving to the same name — the ordinary case
	// for a host that keeps talking across an hour boundary.
	for i, hour := range []int64{hourMS, hourMS - 3600000} {
		if err := st.SaveDNS(context.Background(), store.DNSObservation{
			ID: int64(i + 1), IP: "10.0.0.5", Direction: "source", Name: "talos2.lan",
			HourMS: hour, CompletedMS: now.Add(-time.Duration(i) * time.Hour).UnixMilli(),
		}); err != nil {
			t.Fatalf("save dns: %v", err)
		}
	}

	s := New(cfg, st, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "test-version")
	from := now.Add(-3 * time.Hour).Format(time.RFC3339)
	to := now.Add(time.Hour).Format(time.RFC3339)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet,
		"/api/v1/top?dimension=host&direction=source&from="+from+"&to="+to, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var result store.TopResult
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("rows = %+v, want the one host", result.Rows)
	}
	if got := result.Rows[0].Label; got != "talos2.lan" {
		t.Errorf("label = %q, want %q; one host must carry one name even when the "+
			"same name was recorded in several hours", got, "talos2.lan")
	}
}

// TestHistoricalNamesCollapsesRepeatedName pins the store-side half of the
// "talos2.lan, talos2.lan" label bug.
//
// dns_host_history keeps one row per hour, so a name that persisted across
// several hours was returned several times. Consumers that list names (the
// Hosts table, the chart legend) showed the repeat, and the API payload grew
// with the range width for no reason: a 30-day window returned hundreds of
// copies of one name per address. One distinct name per address, however many
// hours it was observed in.
func TestHistoricalNamesCollapsesRepeatedName(t *testing.T) {
	st, _ := newLabelTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	hourMS := (now.UnixMilli() / 3600000) * 3600000

	if err := st.InsertBatch(ctx, []model.Flow{{
		ReceivedAtMS: now.UnixMilli(), StartMS: now.UnixMilli(), EndMS: now.UnixMilli(),
		TimeSource: "start", Version: 9,
		Exporter: "192.0.2.1", ExporterIP: "192.0.2.1",
		SrcAddr: "10.0.0.8", DstAddr: "192.0.2.9",
		SrcPort: 1024, DstPort: 443, Protocol: 6,
		Bytes: 100, Packets: 1,
	}}); err != nil {
		t.Fatalf("insert flow: %v", err)
	}

	// The same name in three different hours, plus one older, different name.
	names := []struct {
		name    string
		hourAgo int64
	}{
		{"steady.lan", 0}, {"steady.lan", 1}, {"steady.lan", 2},
		{"former.lan", 3},
	}
	for _, n := range names {
		if err := st.SaveDNS(ctx, store.DNSObservation{
			ID: 1, IP: "10.0.0.8", Direction: "source", Name: n.name,
			HourMS: hourMS - n.hourAgo*3600000, CompletedMS: now.Add(-time.Duration(n.hourAgo) * time.Hour).UnixMilli(),
		}); err != nil {
			t.Fatalf("save dns %s: %v", n.name, err)
		}
	}

	got, err := st.HistoricalNames(ctx, store.Range{
		FromMS: now.Add(-6 * time.Hour).UnixMilli(),
		ToMS:   now.Add(time.Hour).UnixMilli(),
	}, "source")
	if err != nil {
		t.Fatalf("historical names: %v", err)
	}
	list := got["10.0.0.8"]
	if len(list) != 2 {
		t.Fatalf("names = %v, want two distinct names", list)
	}
	if list[0] != "steady.lan" || list[1] != "former.lan" {
		t.Errorf("names = %v, want [steady.lan former.lan] (newest first)", list)
	}
}

// TestHostLabelIgnoresRepeats pins the display half: even if the store hands
// back the same name more than once, a row or band labels one host with one
// name. The API must not depend on the store deduplicating for correctness.
func TestHostLabelIgnoresRepeats(t *testing.T) {
	names := map[string][]string{
		"10.0.0.5": {"talos2.lan", "talos2.lan", "talos2.lan"},
		"10.0.0.6": {"newer.lan", "older.lan"},
		"10.0.0.7": {"", "named.lan"},
		"10.0.0.8": {},
	}
	cases := map[string]string{
		"10.0.0.5": "talos2.lan",
		"10.0.0.6": "newer.lan",
		"10.0.0.7": "named.lan",
		"10.0.0.8": "",
		"10.0.0.9": "",
	}
	for key, want := range cases {
		if got := hostLabel(names, key); got != want {
			t.Errorf("hostLabel(%s) = %q, want %q", key, got, want)
		}
	}
}

// TestTopHostSearchFindsRenamedHost pins the search path against the display
// fix. The Hosts table shows one name per host — the newest — so filtering on
// the displayed label alone would stop finding a host by a name it used earlier
// in the window. Collapsing the joined label must not cost that searchability.
func TestTopHostSearchFindsRenamedHost(t *testing.T) {
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "search-label.db")
	cfg.DNS.Enabled = true
	cfg.Listen.UDP = "127.0.0.1:0"
	st, err := store.Open(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	ctx := context.Background()

	now := time.Now().UTC()
	hourMS := (now.UnixMilli() / 3600000) * 3600000
	if err := st.InsertBatch(ctx, []model.Flow{{
		ReceivedAtMS: now.UnixMilli(), StartMS: now.UnixMilli(), EndMS: now.UnixMilli(),
		TimeSource: "start", Version: 9, Exporter: "192.0.2.1", ExporterIP: "192.0.2.1",
		SrcAddr: "10.0.0.60", DstAddr: "192.0.2.9", SrcPort: 1024, DstPort: 443,
		Protocol: 6, Bytes: 100, Packets: 1,
	}}); err != nil {
		t.Fatalf("insert flow: %v", err)
	}
	// Renamed during the window: the current name is the newest, the old one is
	// still in the history.
	for _, o := range []store.DNSObservation{
		{ID: 1, IP: "10.0.0.60", Direction: "source", Name: "was-nas.lan",
			HourMS: hourMS - 3600000, CompletedMS: now.Add(-2 * time.Hour).UnixMilli()},
		{ID: 1, IP: "10.0.0.60", Direction: "source", Name: "is-storage.lan",
			HourMS: hourMS, CompletedMS: now.UnixMilli()},
	} {
		if err := st.SaveDNS(ctx, o); err != nil {
			t.Fatalf("save dns: %v", err)
		}
	}

	s := New(cfg, st, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "test-version")
	from := now.Add(-6 * time.Hour).Format(time.RFC3339)
	to := now.Add(time.Hour).Format(time.RFC3339)
	search := func(term string) (int, string) {
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet,
			"/api/v1/top?dimension=host&direction=source&from="+from+"&to="+to+"&search="+term, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("search %q: status %d: %s", term, rec.Code, rec.Body.String())
		}
		var result store.TopResult
		if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(result.Rows) == 0 {
			return 0, ""
		}
		return result.Total, result.Rows[0].Label
	}

	for _, term := range []string{"is-storage", "was-nas"} {
		total, label := search(term)
		if total != 1 {
			t.Errorf("search %q found %d rows, want the one host (old names must stay searchable)", term, total)
			continue
		}
		if label != "is-storage.lan" {
			t.Errorf("search %q returned label %q, want the current name %q", term, label, "is-storage.lan")
		}
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
