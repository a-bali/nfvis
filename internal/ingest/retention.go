package ingest

import (
	"context"
	"fmt"
	"time"
)

// RetentionState describes the retention sweep schedule and the most recent
// run. Timestamps are Unix milliseconds in UTC; a zero last-run timestamp means
// this process has not completed a sweep since it started.
type RetentionState struct {
	InProgress         bool    `json:"in_progress"`
	LastStartedMS      int64   `json:"last_started_ms"`
	LastFinishedMS     int64   `json:"last_finished_ms"`
	LastDurationMS     int64   `json:"last_duration_ms"`
	LastRowsDeleted    int64   `json:"last_rows_deleted"`
	LastRawDeleted     int64   `json:"last_raw_deleted"`
	LastAggDeleted     int64   `json:"last_aggregate_deleted"`
	LastTrafficDeleted int64   `json:"last_traffic_deleted"`
	LastError          string  `json:"last_error,omitempty"`
	Runs               uint64  `json:"runs"`
	Failures           uint64  `json:"failures"`
	RawTotal           int64   `json:"raw_deleted_total"`
	AggTotal           int64   `json:"aggregate_deleted_total"`
	TrafficTotal       int64   `json:"traffic_deleted_total"`
	IntervalMS         float64 `json:"interval_ms"`
	NextRunMS          int64   `json:"next_run_ms"`
}

// retentionRequest is one queued manual sweep. The maintenance goroutine fills
// in state and closes done, so a caller can wait for the outcome.
type retentionRequest struct {
	done  chan struct{}
	state RetentionState
	// enqueued guards against sending the same shared request twice: a second
	// caller that arrives after the maintenance goroutine already consumed the
	// first send would otherwise queue a duplicate sweep.
	enqueued bool
}

// RetentionStatus returns the current retention schedule and last-run summary.
// Totals are process-lifetime counters, matching the health endpoint.
func (c *Collector) RetentionStatus() RetentionState {
	c.retentionMu.Lock()
	state := c.retention
	c.retentionMu.Unlock()
	state.RawTotal = int64(c.counters.RetentionRawDeleted.Load())
	state.AggTotal = int64(c.counters.RetentionAggDeleted.Load())
	state.TrafficTotal = int64(c.counters.RetentionTrafficDeleted.Load())
	state.IntervalMS = float64(c.cfg.Database.Retention.CleanupInterval.Duration().Milliseconds())
	return state
}

// retentionManualDebounce collapses a burst of manual run requests into one
// sweep. Overlapping callers already share a single queued request, but a sweep
// of an idle database finishes in milliseconds, so staggered clicks would each
// start their own sweep. A request arriving this soon after a completed sweep
// reports that sweep's result instead of queueing another. The interval is short
// enough to be invisible to a person and long enough to absorb double clicks.
const retentionManualDebounce = time.Second

// RunRetention asks the maintenance goroutine for a sweep and waits for it.
//
// The sweep is not cancelled when ctx expires: a caller that gives up (an HTTP
// client disconnecting) must not abort a half-finished delete, so on timeout the
// caller gets the in-progress state and the sweep finishes in the background.
// Concurrent and rapid callers share one sweep rather than stacking sweeps.
func (c *Collector) RunRetention(ctx context.Context) (RetentionState, error) {
	c.retentionMu.Lock()
	// A sweep that just finished makes this burst redundant: report it as is.
	if c.pendingRetention == nil && !c.retention.InProgress && c.retention.LastFinishedMS > 0 {
		finished := time.UnixMilli(c.retention.LastFinishedMS)
		if time.Since(finished) < c.retentionDebounce {
			c.retentionMu.Unlock()
			return c.RetentionStatus(), nil
		}
	}
	req := c.pendingRetention
	if req == nil {
		req = &retentionRequest{done: make(chan struct{})}
		c.pendingRetention = req
	}
	// Only the caller that created the request may enqueue it, and only once.
	mine := !req.enqueued
	req.enqueued = true
	c.retentionMu.Unlock()

	if mine {
		select {
		case c.retentionQueue <- req:
		default:
			// A request is already queued; this sweep covers both callers
			// because they share the same request.
		}
	}

	select {
	case <-req.done:
		c.retentionMu.Lock()
		message := req.state.LastError
		c.retentionMu.Unlock()
		if message != "" {
			return c.RetentionStatus(), fmt.Errorf("retention sweep: %s", message)
		}
		return c.RetentionStatus(), nil
	case <-ctx.Done():
		return c.RetentionStatus(), ctx.Err()
	}
}

// beginRetention marks a sweep as running.
func (c *Collector) beginRetention(started, nextRun time.Time) {
	c.retentionMu.Lock()
	defer c.retentionMu.Unlock()
	c.retention.InProgress = true
	c.retention.LastStartedMS = started.UnixMilli()
	if !nextRun.IsZero() {
		c.retention.NextRunMS = nextRun.UnixMilli()
	}
}

// endRetention records the outcome and releases a waiting manual request.
func (c *Collector) endRetention(started, finished time.Time, raw, aggregate, traffic int64, sweepErr error) {
	c.retentionMu.Lock()
	defer c.retentionMu.Unlock()
	c.retention.InProgress = false
	c.retention.LastStartedMS = started.UnixMilli()
	c.retention.LastFinishedMS = finished.UnixMilli()
	c.retention.LastDurationMS = finished.Sub(started).Milliseconds()
	c.retention.LastRawDeleted = raw
	c.retention.LastAggDeleted = aggregate
	c.retention.LastTrafficDeleted = traffic
	c.retention.LastRowsDeleted = raw + aggregate + traffic
	c.retention.Runs++
	if sweepErr != nil {
		c.retention.Failures++
		c.retention.LastError = sweepErr.Error()
	} else {
		c.retention.LastError = ""
	}
	if req := c.pendingRetention; req != nil {
		req.state = c.retention
		c.pendingRetention = nil
		close(req.done)
	}
}
