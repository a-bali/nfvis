package api

import (
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// bruteNearestRank is an independent implementation of the definition, written
// from the prose ("the smallest value v such that at least p of the sample is
// <= v") rather than from the shipped code, so the test does not just restate the
// implementation it is checking.
func bruteNearestRank(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	target := p * float64(len(sorted))
	atLeast := 0
	for i, v := range sorted {
		// count of values <= v is i+1
		if float64(i+1) >= target {
			atLeast = int(v)
			break
		}
	}
	return float64(atLeast)
}

// TestPercentileMatchesNearestRankDefinition pins percentile() against the
// definition for the cases where a truncating implementation goes wrong.
//
// An earlier version computed int(p*n) and indexed 0-based, which returns one
// rank too high whenever p*n is a whole number. At n=10 that made p90 equal the
// maximum — the single slowest request — while the UI labels the figure "9 in 10
// requests are faster". The test that accompanied it asserted those wrong numbers,
// so the bug was protected by its own test.
func TestPercentileMatchesNearestRankDefinition(t *testing.T) {
	// n values 1..n, so the expected percentile is a number the reader can predict.
	for _, n := range []int{5, 7, 10, 11, 20, 100, 1000} {
		values := make([]float64, n)
		for i := range values {
			values[i] = float64(i + 1)
		}
		for _, p := range []float64{0.5, 0.9, 0.99} {
			got := percentile(values, p)
			want := float64(int(math.Ceil(p * float64(n))))
			if want < 1 {
				want = 1
			}
			if got != want {
				t.Errorf("n=%d p=%.2f: percentile = %v, want %v (rank ceil(p*n), 1-based)", n, p, got, want)
			}
			// Cross-check against the independent brute-force definition.
			if brute := bruteNearestRank(values, p); got != brute {
				t.Errorf("n=%d p=%.2f: percentile = %v, brute-force definition says %v", n, p, got, brute)
			}
		}
	}
}

// TestPercentileTenSampleIsNotTheMaximum is the specific regression: with ten
// samples p90 must not be the largest, because that is what the old off-by-one
// produced and it made the UI's claim false.
func TestPercentileTenSampleIsNotTheMaximum(t *testing.T) {
	values := []float64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	if got := percentile(values, 0.90); got == 100 {
		t.Errorf("p90 of 10 samples = %v, which is the maximum; rank ceil(0.9*10)=9 so it must be 90", got)
	}
	if got := percentile(values, 0.90); got != 90 {
		t.Errorf("p90 = %v, want 90", got)
	}
	if got := percentile(values, 0.50); got != 50 {
		t.Errorf("median = %v, want 50", got)
	}
	if got := percentile(values, 1.0); got != 100 {
		t.Errorf("p100 = %v, want the maximum 100", got)
	}
}

// TestAPIMetricsPercentiles pins the summary arithmetic on the sample the
// dashboard shows. These are the numbers a user reads, so they are checked
// against hand-computed values rather than against the implementation.
func TestAPIMetricsPercentiles(t *testing.T) {
	m := newAPIMetrics()
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	m.now = func() time.Time { return now }

	// Durations 10..100 ms in steps of 10, sizes the same.
	for i := 1; i <= 10; i++ {
		m.observe(time.Duration(i*10)*time.Millisecond, int64(i*10))
	}

	s := m.snapshot()
	if s.Requests != 10 {
		t.Fatalf("requests = %d, want 10", s.Requests)
	}
	if s.DurationMS.Avg != 55 {
		t.Errorf("duration avg = %v, want 55", s.DurationMS.Avg)
	}
	// n=10: rank ceil(0.5*10)=5 -> 50ms; ceil(0.9*10)=9 -> 90ms.
	if s.DurationMS.Median != 50 {
		t.Errorf("duration median = %v, want 50", s.DurationMS.Median)
	}
	if s.DurationMS.P90 != 90 {
		t.Errorf("duration p90 = %v, want 90", s.DurationMS.P90)
	}
	if s.DurationMS.Max != 100 {
		t.Errorf("duration max = %v, want 100", s.DurationMS.Max)
	}
	if s.SizeBytes.Avg != 55 || s.SizeBytes.P90 != 90 || s.SizeBytes.Max != 100 {
		t.Errorf("size summary = %+v, want avg 55 p90 90 max 100", s.SizeBytes)
	}
}

// TestAPIMetricsWindowDropsOldSamples covers the 24-hour boundary.
func TestAPIMetricsWindowDropsOldSamples(t *testing.T) {
	m := newAPIMetrics()
	t0 := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	clock := t0
	m.now = func() time.Time { return clock }

	m.observe(100*time.Millisecond, 1000) // will age out
	clock = t0.Add(23 * time.Hour)
	m.observe(10*time.Millisecond, 100)
	m.observe(20*time.Millisecond, 200)

	clock = t0.Add(25 * time.Hour)
	s := m.snapshot()
	if s.Requests != 2 {
		t.Fatalf("requests in window = %d, want 2", s.Requests)
	}
	if s.DurationMS.Max != 20 {
		t.Errorf("max = %v, want 20 (the 100ms sample should have aged out)", s.DurationMS.Max)
	}
	if s.RequestsTotal != 3 {
		t.Errorf("requests_total = %d, want 3 (lifetime, not windowed)", s.RequestsTotal)
	}
}

// TestAPIMetricsRingWraps verifies the fixed-size ring keeps the newest samples.
func TestAPIMetricsRingWraps(t *testing.T) {
	m := newAPIMetrics()
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	m.now = func() time.Time { return now }

	over := apiMetricsCapacity + 500
	for i := 0; i < over; i++ {
		m.observe(time.Duration(i)*time.Microsecond, int64(i))
	}
	if len(m.samples) != apiMetricsCapacity {
		t.Fatalf("ring length = %d, want the cap %d", len(m.samples), apiMetricsCapacity)
	}
	s := m.snapshot()
	if s.Requests != apiMetricsCapacity {
		t.Errorf("requests in window = %d, want %d", s.Requests, apiMetricsCapacity)
	}
	if s.RequestsTotal != uint64(over) {
		t.Errorf("requests_total = %d, want %d", s.RequestsTotal, over)
	}
	if s.SizeBytes.Max != float64(over-1) {
		t.Errorf("size max = %v, want %v (newest sample)", s.SizeBytes.Max, over-1)
	}
}

// TestAPIMetricsEmpty makes sure a fresh server reports zeroes.
func TestAPIMetricsEmpty(t *testing.T) {
	m := newAPIMetrics()
	s := m.snapshot()
	if s.Requests != 0 {
		t.Errorf("requests = %d, want 0", s.Requests)
	}
	if s.DurationMS.Avg != 0 || s.DurationMS.P90 != 0 {
		t.Errorf("summary on empty sample = %+v, want zeroes", s.DurationMS)
	}
	if s.WindowSeconds != int64(apiMetricsWindow/time.Second) {
		t.Errorf("window = %d, want %d", s.WindowSeconds, int64(apiMetricsWindow/time.Second))
	}
}

// TestMetricsMiddlewareRecordsAPIOnly proves static assets are excluded.
func TestMetricsMiddlewareRecordsAPIOnly(t *testing.T) {
	a := &Server{metrics: newAPIMetrics()}
	a.metrics.now = func() time.Time { return time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC) }

	handler := a.metricsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/health":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case "/api/v1/missing":
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"nope"}`))
		default:
			_, _ = w.Write([]byte("<html>asset</html>"))
		}
	}))

	for _, p := range []string{"/api/v1/health", "/api/v1/missing", "/assets/index.js", "/"} {
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, p, nil))
	}

	s := a.metrics.snapshot()
	if s.Requests != 2 {
		t.Fatalf("recorded %d requests, want 2 (only /api/ paths)", s.Requests)
	}
	if s.SizeBytes.Max != 16 {
		t.Errorf("size max = %v, want 16", s.SizeBytes.Max)
	}
}

// TestMetricsMiddlewareRecordsPanics covers a gap: the handler is
// recoverMiddleware(metricsMiddleware(mux)), so a panicking handler unrolled the
// metrics frame and the request was never recorded at all — a 500 caused by a
// panic was invisible in requests_total.
func TestMetricsMiddlewareRecordsPanics(t *testing.T) {
	a := &Server{metrics: newAPIMetrics(), log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	a.metrics.now = func() time.Time { return time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC) }

	handler := a.recoverMiddleware(a.metricsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	})))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/health", nil))

	s := a.metrics.snapshot()
	if s.Requests != 1 {
		t.Errorf("panicking request was not recorded: requests = %d, want 1", s.Requests)
	}
	if s.RequestsTotal != 1 {
		t.Errorf("requests_total = %d, want 1", s.RequestsTotal)
	}
}

// TestRecordingWriterDefaultsTo200 covers a handler that writes without calling
// WriteHeader.
func TestRecordingWriterDefaultsTo200(t *testing.T) {
	rec := &recordingResponseWriter{ResponseWriter: httptest.NewRecorder()}
	_, _ = rec.Write([]byte("hello"))
	if rec.status != 200 {
		t.Errorf("status = %d, want 200", rec.status)
	}
	if rec.bytes != 5 {
		t.Errorf("bytes = %d, want 5", rec.bytes)
	}
}

// TestRecordingWriterKeepsFlusher ensures the wrapper does not hide the
// underlying writer's optional interfaces.
func TestRecordingWriterKeepsFlusher(t *testing.T) {
	underlying := httptest.NewRecorder()
	rec := &recordingResponseWriter{ResponseWriter: underlying}
	if _, ok := interface{}(rec).(http.Flusher); !ok {
		t.Error("wrapper does not implement http.Flusher")
	}
	rec.Flush() // must not panic
	if rec.Unwrap() != underlying {
		t.Error("Unwrap did not return the underlying writer")
	}
}
