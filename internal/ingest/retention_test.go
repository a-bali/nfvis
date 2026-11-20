package ingest

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/a-bali/nfvis/internal/config"
	"github.com/a-bali/nfvis/internal/decoder"
	"github.com/a-bali/nfvis/internal/model"
	"github.com/a-bali/nfvis/internal/store"
)

func retentionCollector(t *testing.T, cleanupInterval time.Duration, rawRetention time.Duration) (*Collector, *store.Store, config.Config) {
	t.Helper()
	cfg := config.Default()
	cfg.Listen.UDP = "127.0.0.1:0"
	cfg.Database.Path = filepath.Join(t.TempDir(), "nfvis.db")
	cfg.Database.Retention.Raw = config.Duration(rawRetention)
	cfg.Database.Retention.CleanupInterval = config.Duration(cleanupInterval)
	cfg.DNS.Enabled = false
	st, err := store.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	coll := New(cfg, decoder.New(cfg), st, &Counters{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return coll, st, cfg
}

// The startup sweep must publish a last-run summary and a next-run time, and a
// manual run must record its own duration and deleted-row counts.
func TestRetentionStatusAfterStartupSweep(t *testing.T) {
	coll, st, cfg := retentionCollector(t, time.Hour, time.Hour)
	now := time.Now().UTC()
	// One flow well outside the raw window, one inside it.
	if err := st.InsertBatch(context.Background(), []model.Flow{
		flowAt(now.Add(-3*time.Hour), "10.0.0.1"),
		flowAt(now.Add(-time.Minute), "10.0.0.2"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := coll.Start(); err != nil {
		t.Fatal(err)
	}
	defer coll.Stop()

	deadline := time.Now().Add(10 * time.Second)
	var state RetentionState
	for time.Now().Before(deadline) {
		state = coll.RetentionStatus()
		if state.Runs > 0 && !state.InProgress {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if state.Runs == 0 {
		t.Fatalf("startup sweep did not run: %+v", state)
	}
	if state.LastFinishedMS == 0 || state.LastStartedMS == 0 || state.LastFinishedMS < state.LastStartedMS {
		t.Fatalf("last run timestamps = %+v", state)
	}
	if state.LastRowsDeleted != 1 || state.LastRawDeleted != 1 {
		t.Fatalf("startup sweep deleted %+v, want 1 raw row", state)
	}
	if state.LastError != "" || state.Failures != 0 {
		t.Fatalf("startup sweep reported %q", state.LastError)
	}
	wantInterval := cfg.Database.Retention.CleanupInterval.Duration().Milliseconds()
	if state.IntervalMS != float64(wantInterval) {
		t.Fatalf("interval = %v ms, want %d", state.IntervalMS, wantInterval)
	}
	if state.NextRunMS <= state.LastFinishedMS {
		t.Fatalf("next run %d is not after last finish %d", state.NextRunMS, state.LastFinishedMS)
	}

	// A manual run now has nothing to delete but must still be recorded. The
	// startup sweep just finished, so clear the debounce that would otherwise
	// fold this request into it.
	coll.retentionDebounce = 0
	before := state.Runs
	if _, err := coll.RunRetention(context.Background()); err != nil {
		t.Fatalf("manual sweep: %v", err)
	}
	after := coll.RetentionStatus()
	if after.Runs != before+1 {
		t.Fatalf("runs after manual sweep = %d, want %d", after.Runs, before+1)
	}
	if after.LastRowsDeleted != 0 {
		t.Fatalf("second sweep deleted %d rows, want 0", after.LastRowsDeleted)
	}
	if after.LastDurationMS < 0 {
		t.Fatalf("duration = %d", after.LastDurationMS)
	}
	if after.LastFinishedMS < after.LastStartedMS {
		t.Fatalf("manual sweep timestamps = %+v", after)
	}
	if after.RawTotal != 1 {
		t.Fatalf("lifetime raw deletions = %d, want 1", after.RawTotal)
	}
}

// A manual sweep must respect the caller's deadline while the sweep itself is
// allowed to finish, and the status must report progress rather than a hang.
func TestRunRetentionHonoursCallerDeadline(t *testing.T) {
	coll, _, _ := retentionCollector(t, time.Hour, time.Hour)
	if err := coll.Start(); err != nil {
		t.Fatal(err)
	}
	defer coll.Stop()
	// Wait for the startup sweep so the manual request is the only one running.
	deadline := time.Now().Add(5 * time.Second)
	for coll.RetentionStatus().Runs == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	coll.retentionDebounce = 0
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	state, err := coll.RunRetention(ctx)
	if err == nil {
		t.Fatalf("cancelled context returned no error: %+v", state)
	}
	if state.IntervalMS == 0 {
		t.Fatalf("status returned without schedule info: %+v", state)
	}
}

// runRetention must keep the scheduled cadence: a manual sweep does not push
// the next ticker run further out than one interval.
func TestManualSweepKeepsInterval(t *testing.T) {
	coll, _, cfg := retentionCollector(t, time.Hour, 30*24*time.Hour)
	if err := coll.Start(); err != nil {
		t.Fatal(err)
	}
	defer coll.Stop()
	deadline := time.Now().Add(5 * time.Second)
	for coll.RetentionStatus().Runs == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := coll.RunRetention(context.Background()); err != nil {
		t.Fatal(err)
	}
	state := coll.RetentionStatus()
	interval := cfg.Database.Retention.CleanupInterval.Duration()
	horizon := time.Now().Add(interval + 5*time.Second).UnixMilli()
	if state.NextRunMS > horizon {
		t.Fatalf("next run %d is beyond one interval from now (%d)", state.NextRunMS, horizon)
	}
}

func flowAt(when time.Time, src string) model.Flow {
	return model.Flow{
		ReceivedAtMS: when.UnixMilli(),
		StartMS:      when.UnixMilli(),
		EndMS:        when.UnixMilli(),
		TimeSource:   "exported",
		Version:      5,
		Exporter:     "192.0.2.1",
		ExporterIP:   "192.0.2.1",
		SrcAddr:      src,
		DstAddr:      "203.0.113.1",
		Bytes:        100,
		Packets:      1,
	}
}

// Concurrent manual runs must not stack sweeps: callers that arrive while a
// request is pending share it, and a burst that follows a just-finished sweep is
// debounced, so N clicks produce one sweep, not N.
func TestConcurrentRunRetentionCoalesces(t *testing.T) {
	coll, _, _ := retentionCollector(t, time.Hour, 30*24*time.Hour)
	if err := coll.Start(); err != nil {
		t.Fatal(err)
	}
	defer coll.Stop()
	deadline := time.Now().Add(5 * time.Second)
	for coll.RetentionStatus().Runs == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	// The debounce window must outlast the burst by a wide margin, so the test
	// does not depend on how fast a sweep of an empty database happens to be.
	coll.retentionDebounce = 30 * time.Second
	before := coll.RetentionStatus().Runs

	const callers = 5
	var wg sync.WaitGroup
	errs := make([]error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = coll.RunRetention(context.Background())
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("caller %d: %v", i, err)
		}
	}
	time.Sleep(100 * time.Millisecond)
	after := coll.RetentionStatus().Runs
	if after-before > 1 {
		t.Fatalf("%d concurrent callers produced %d sweeps, want at most 1", callers, after-before)
	}
}

// A manual run outside the debounce window must still sweep, so the debounce
// cannot silently turn the button into a no-op.
func TestManualRunAfterDebounceSweeps(t *testing.T) {
	coll, _, _ := retentionCollector(t, time.Hour, 30*24*time.Hour)
	if err := coll.Start(); err != nil {
		t.Fatal(err)
	}
	defer coll.Stop()
	deadline := time.Now().Add(5 * time.Second)
	for coll.RetentionStatus().Runs == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	coll.retentionDebounce = time.Millisecond
	before := coll.RetentionStatus().Runs
	if _, err := coll.RunRetention(context.Background()); err != nil {
		t.Fatal(err)
	}
	if after := coll.RetentionStatus().Runs; after != before+1 {
		t.Fatalf("runs = %d, want %d", after, before+1)
	}
}
