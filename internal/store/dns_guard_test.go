package store

import (
	"context"
	"testing"
	"time"
)

// TestFirstUnresolvedObservationStampsTheRow is the regression test for a guard
// that made the first lookup of an unresolved address write nothing.
//
// src_dns_ms and dst_dns_ms are declared `INTEGER NOT NULL DEFAULT 0`, so a scan
// into sql.NullInt64 always reports Valid == true — 0 is not NULL. A guard
// conditioned on `!currentStamp.Valid` therefore never fires, and the "a first
// observation must write even when the name is empty" branch was dead code: no
// row was ever stamped for an address with no PTR record. That destroys the
// distinction the stamp exists to record, between "never looked up" and "looked
// up, nothing found".
//
// The old test scanned into *int64 and asserted non-nil, which cannot fail on a
// NOT NULL column — measured nil=false on both the fixed and the broken code. So
// this test asserts the VALUE, not the nullability.
func TestFirstUnresolvedObservationStampsTheRow(t *testing.T) {
	st, id := storeWithOneFlow(t, nil)
	ctx := context.Background()
	now := time.Now().UTC()
	hourMS := (now.UnixMilli() / 3600000) * 3600000
	completed := now.UnixMilli()

	// An unresolved lookup: the resolver found no PTR, so the name is empty.
	if err := st.SaveDNS(ctx, DNSObservation{
		ID: id, IP: "10.0.0.1", Direction: "source", Name: "",
		HourMS: hourMS, CompletedMS: completed,
	}); err != nil {
		t.Fatalf("save: %v", err)
	}

	var stamp int64
	if err := st.DB().QueryRow(`SELECT src_dns_ms FROM flows WHERE id=?`, id).Scan(&stamp); err != nil {
		t.Fatalf("read stamp: %v", err)
	}
	if stamp == 0 {
		t.Fatal("src_dns_ms is still 0 after the first unresolved lookup: the row was never " +
			"annotated, so a later retry cannot tell 'never looked up' from 'looked up, no PTR'")
	}
	if stamp != completed {
		t.Errorf("src_dns_ms = %d, want the completion stamp %d", stamp, completed)
	}
}

// TestSecondUnresolvedObservationIsSkipped is the counterpart: the guard must
// still suppress the repeat once the row carries a stamp, which is where the
// write saving actually comes from.
func TestSecondUnresolvedObservationIsSkipped(t *testing.T) {
	st, id := storeWithOneFlow(t, nil)
	ctx := context.Background()
	now := time.Now().UTC()
	hourMS := (now.UnixMilli() / 3600000) * 3600000

	obs := DNSObservation{ID: id, IP: "10.0.0.1", Direction: "source", Name: "",
		HourMS: hourMS, CompletedMS: now.UnixMilli()}
	if err := st.SaveDNS(ctx, obs); err != nil {
		t.Fatalf("first: %v", err)
	}
	before := writeRowCounter(t, st)

	obs.CompletedMS = now.Add(time.Second).UnixMilli()
	if err := st.SaveDNS(ctx, obs); err != nil {
		t.Fatalf("second: %v", err)
	}
	if got := writeRowCounter(t, st) - before; got != 0 {
		t.Errorf("a repeated unresolved observation changed %d rows, want 0", got)
	}
}

// TestUnresolvedLookupKeepsHistoryForLaterHours covers a consequence of the
// guard: a cached name that crosses an hour boundary must still record its hour.
//
// dns_host_history is keyed by (hour_ms, host, direction, name) and read with
// `hour_ms >= from AND hour_ms < to`, so a name resolved at hour H is only
// visible for a range covering H. If the guard skips the observation because the
// name is unchanged, hour H+1 never gets a row and that hour's chart falls back
// to the bare IP.
func TestUnresolvedLookupKeepsHistoryForLaterHours(t *testing.T) {
	st, id := storeWithOneFlow(t, nil)
	ctx := context.Background()
	now := time.Now().UTC()
	hourMS := (now.UnixMilli() / 3600000) * 3600000

	// A name resolved in the current hour...
	if err := st.SaveDNS(ctx, DNSObservation{
		ID: id, IP: "10.0.0.1", Direction: "source", Name: "printer.lan",
		HourMS: hourMS, CompletedMS: now.UnixMilli(),
	}); err != nil {
		t.Fatalf("hour 0: %v", err)
	}
	// ...and the same cached name observed in the next hour.
	if err := st.SaveDNS(ctx, DNSObservation{
		ID: id, IP: "10.0.0.1", Direction: "source", Name: "printer.lan",
		HourMS: hourMS + 3600000, CompletedMS: now.Add(time.Hour).UnixMilli(),
	}); err != nil {
		t.Fatalf("hour 1: %v", err)
	}

	var hours []int64
	rows, err := st.DB().Query(`SELECT hour_ms FROM dns_host_history WHERE host='10.0.0.1' ORDER BY hour_ms`)
	if err != nil {
		t.Fatalf("read history: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var h int64
		if err := rows.Scan(&h); err != nil {
			t.Fatalf("scan: %v", err)
		}
		hours = append(hours, h)
	}
	if len(hours) != 2 {
		t.Fatalf("dns_host_history has rows for hours %v, want both H and H+1: an unchanged name "+
			"crossing an hour boundary must still be recorded, or that hour's chart loses the name", hours)
	}

	// And the query the dashboard uses for hour H+1 must find it.
	names, err := st.HistoricalNames(ctx, Range{
		FromMS: hourMS + 3600000,
		ToMS:   hourMS + 2*3600000,
	}, "source")
	if err != nil {
		t.Fatalf("historical names: %v", err)
	}
	if got := names["10.0.0.1"]; len(got) == 0 {
		t.Errorf("no name for hour H+1; the hour's chart would show the bare IP")
	}
}

// TestSaveDNSBatchIsOneTransaction is the test the previous one was not: it
// asserts a batched call commits once, by counting AuthTransaction events through
// a driver authorizer rather than by counting rows.
//
// SELECT total_changes() counts rows modified, so the earlier "one transaction"
// test passed even when the batch opened a transaction per observation.
func TestSaveDNSBatchIsOneTransaction(t *testing.T) {
	st, id := storeWithOneFlow(t, nil)
	ctx := context.Background()
	now := time.Now().UTC()
	hourMS := (now.UnixMilli() / 3600000) * 3600000

	before := txCommitCount()
	_, err := st.SaveDNSBatch(ctx, []DNSObservation{
		{ID: id, IP: "10.0.0.1", Direction: "source", Name: "a.lan", HourMS: hourMS, CompletedMS: now.UnixMilli()},
		{ID: id, IP: "10.0.0.2", Direction: "destination", Name: "b.lan", HourMS: hourMS, CompletedMS: now.UnixMilli()},
	})
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	got := txCommitCount() - before
	if got != 1 {
		t.Errorf("SaveDNSBatch made %d commits for two observations, want 1", got)
	}

	// Both endpoints still annotated.
	var src, dst string
	if err := st.DB().QueryRow(`SELECT COALESCE(src_name,''), COALESCE(dst_name,'') FROM flows WHERE id=?`, id).Scan(&src, &dst); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if src != "a.lan" || dst != "b.lan" {
		t.Errorf("annotations = %q / %q, want a.lan / b.lan", src, dst)
	}
}

// TestSaveDNSBatchSplittingIsDetected proves the transaction test above can fail:
// it is written so that a per-observation transaction shape is distinguishable.
func TestSaveDNSBatchSplittingIsDetected(t *testing.T) {
	if !txCountAvailable() {
		t.Skip("connection authorizer unavailable; transaction counting not possible here")
	}
	// Sanity: two separate single-observation calls must register two commits, so a
	// batched call registering one is meaningful rather than an artifact of the
	// counter never moving.
	st, id := storeWithOneFlow(t, nil)
	ctx := context.Background()
	now := time.Now().UTC()
	hourMS := (now.UnixMilli() / 3600000) * 3600000

	before := txCommitCount()
	for _, o := range []DNSObservation{
		{ID: id, IP: "10.0.0.1", Direction: "source", Name: "a.lan", HourMS: hourMS, CompletedMS: now.UnixMilli()},
		{ID: id, IP: "10.0.0.2", Direction: "destination", Name: "b.lan", HourMS: hourMS, CompletedMS: now.UnixMilli()},
	} {
		if err := st.SaveDNS(ctx, o); err != nil {
			t.Fatalf("save: %v", err)
		}
	}
	if got := txCommitCount() - before; got != 2 {
		t.Errorf("two single-observation calls made %d commits, want 2 (the counter must "+
			"be able to distinguish 1 from 2 for the batching assertion to mean anything)", got)
	}
}
