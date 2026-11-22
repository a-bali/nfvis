package api

import (
	"math"
	"net/http"
	"sort"
	"sync"
	"time"
)

// apiSample is one recorded request. The ring keeps the raw samples for 24
// hours so the percentiles are exact rather than bucketed: at a few requests per
// second the volume is trivial (a dashboard polling every 5s produces ~17k
// samples a day, well under a megabyte), and exact quantiles avoid the
// reporting errors a coarse histogram introduces at p90.
type apiSample struct {
	atMS       int64
	durationMS float64
	sizeBytes  int64
}

// apiMetrics retains the last 24 hours of API request observations and reports
// the totals the System page shows. It is safe for concurrent use: every request
// records from its own goroutine.
type apiMetrics struct {
	mu      sync.Mutex
	samples []apiSample
	// next is the ring write position; when samples is full it wraps.
	next int
	full bool

	window time.Duration
	now    func() time.Time // injectable for tests

	total uint64 // requests since start, including ones aged out of the window
}

const apiMetricsWindow = 24 * time.Hour

// apiMetricsCapacity bounds memory. At 200 requests/s for 24h this would be
// 17M entries, which is too many; the ring is sized for the realistic case
// (a dashboard polling plus a few humans) and older samples are dropped in
// favour of the most recent ones, which is what a rolling window should do
// anyway. 100k entries is ~4 MB.
const apiMetricsCapacity = 100_000

func newAPIMetrics() *apiMetrics {
	return &apiMetrics{window: apiMetricsWindow, now: time.Now}
}

// observe records one request.
func (m *apiMetrics) observe(duration time.Duration, size int64) {
	if m == nil {
		return
	}
	s := apiSample{atMS: m.now().UnixMilli(), durationMS: float64(duration.Microseconds()) / 1000, sizeBytes: size}
	m.mu.Lock()
	m.total++
	if len(m.samples) < apiMetricsCapacity {
		m.samples = append(m.samples, s)
	} else {
		m.samples[m.next] = s
		m.next = (m.next + 1) % apiMetricsCapacity
		m.full = true
	}
	m.mu.Unlock()
}

// APIMetricsSnapshot is what the System endpoint serves.
type APIMetricsSnapshot struct {
	WindowSeconds int64      `json:"window_seconds"`
	Requests      int64      `json:"requests"`
	RequestsTotal uint64     `json:"requests_total"`
	DurationMS    APISummary `json:"duration_ms"`
	SizeBytes     APISummary `json:"size_bytes"`
}

// APISummary is the set of order statistics the dashboard asked for.
type APISummary struct {
	Avg    float64 `json:"avg"`
	Median float64 `json:"median"`
	P90    float64 `json:"p90"`
	Max    float64 `json:"max"`
}

// snapshot returns the aggregate over the retained window, discarding samples
// that have aged out. It sorts two copies of the window — durations and sizes —
// because percentiles of different fields are not co-monotonic.
func (m *apiMetrics) snapshot() APIMetricsSnapshot {
	if m == nil {
		return APIMetricsSnapshot{}
	}
	cutoff := m.now().Add(-m.window).UnixMilli()

	m.mu.Lock()
	total := m.total
	kept := make([]apiSample, 0, len(m.samples))
	for _, s := range m.samples {
		if s.atMS >= cutoff {
			kept = append(kept, s)
		}
	}
	m.mu.Unlock()

	out := APIMetricsSnapshot{
		WindowSeconds: int64(m.window / time.Second),
		Requests:      int64(len(kept)),
		RequestsTotal: total,
	}
	if len(kept) == 0 {
		return out
	}

	durations := make([]float64, len(kept))
	sizes := make([]float64, len(kept))
	var sumDur, sumSize float64
	for i, s := range kept {
		durations[i] = s.durationMS
		sizes[i] = float64(s.sizeBytes)
		sumDur += s.durationMS
		sumSize += float64(s.sizeBytes)
	}
	out.DurationMS = summarize(durations, sumDur)
	out.SizeBytes = summarize(sizes, sumSize)
	return out
}

// summarize computes avg/median/p90/max. sum is passed in so the caller does
// not pay for a second pass. It sorts in place.
func summarize(values []float64, sum float64) APISummary {
	sort.Float64s(values)
	n := len(values)
	return APISummary{
		Avg:    round2(sum / float64(n)),
		Median: round2(percentile(values, 0.50)),
		P90:    round2(percentile(values, 0.90)),
		Max:    round2(values[n-1]),
	}
}

// percentile uses nearest-rank: the smallest value v such that at least p
// fraction of the sample is <= v, i.e. the value at 1-based rank ceil(p*n).
//
// Getting this right matters because p90 is displayed as a latency figure with
// the hint "9 in 10 requests are faster". Truncating (int(p*n)) and indexing
// 0-based returns one rank too high whenever p*n is a whole number — at n=10 that
// made p90 equal max, i.e. the single slowest request rather than the 90th
// percentile, and the claim above the number was false.
func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(p * float64(len(sorted))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

func round2(v float64) float64 {
	return float64(int64(v*100+0.5)) / 100
}

// recordingResponseWriter captures the status and byte count of a response so
// the middleware can record them. Flush and Unwrap are forwarded so the
// underlying ResponseWriter's optional interfaces keep working — the static
// handler and the JSON writers rely on plain Write, but a proxy in front may
// use Flush, and losing it would silently degrade streaming.
type recordingResponseWriter struct {
	http.ResponseWriter
	status int
	bytes  int64
	wrote  bool
}

func (w *recordingResponseWriter) WriteHeader(code int) {
	if !w.wrote {
		w.status = code
		w.wrote = true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *recordingResponseWriter) Write(b []byte) (int, error) {
	if !w.wrote {
		w.status = 200
		w.wrote = true
	}
	n, err := w.ResponseWriter.Write(b)
	w.bytes += int64(n)
	return n, err
}

func (w *recordingResponseWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (w *recordingResponseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
