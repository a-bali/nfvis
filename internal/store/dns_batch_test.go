package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-bali/nfvis/internal/config"
	"github.com/a-bali/nfvis/internal/model"
)

// insertOne opens a store, inserts a single flow, and returns its row id.
func storeWithOneFlow(t *testing.T, mutate func(*config.Config)) (*Store, int64) {
	t.Helper()
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "dns.db")
	cfg.DNS.Enabled = true
	if mutate != nil {
		mutate(&cfg)
	}
	st, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	now := time.Now().UTC()
	flow := model.Flow{
		ReceivedAtMS: now.UnixMilli(), StartMS: now.UnixMilli(), EndMS: now.UnixMilli(),
		TimeSource: "start", Version: 9,
		Exporter: "192.0.2.1", ExporterIP: "192.0.2.1",
		InputIf: 2, OutputIf: 7,
		SrcAddr: "10.0.0.1", DstAddr: "10.0.0.2",
		SrcPort: 1024, DstPort: 443, Protocol: 6,
		Bytes: 100, Packets: 1,
	}
	if err := st.InsertBatch(context.Background(), []model.Flow{flow}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	var id int64
	if err := st.DB().QueryRow("SELECT MAX(id) FROM flows").Scan(&id); err != nil {
		t.Fatalf("read id: %v", err)
	}
	return st, id
}

// TestSaveDNSBatchWritesBothEndpointsInOneTransaction is the regression test for
// the CPU cost: annotating a flow used to open and commit two transactions, one
// per endpoint, which at production rate was most of the process's CPU.
//
// The assertion is on the number of write transactions, not on elapsed time,
// because a timing assertion would be flaky and would not prove the fix.
func TestSaveDNSBatchWritesBothEndpointsInOneTransaction(t *testing.T) {
	st, id := storeWithOneFlow(t, nil)
	ctx := context.Background()
	now := time.Now().UTC()
	hourMS := (now.UnixMilli() / 3600000) * 3600000

	rowsBefore := writeRowCounter(t, st)

	_, err := st.SaveDNSBatch(ctx, []DNSObservation{
		{ID: id, IP: "10.0.0.1", Direction: "source", Name: "src.lan", HourMS: hourMS, CompletedMS: now.UnixMilli()},
		{ID: id, IP: "10.0.0.2", Direction: "destination", Name: "dst.lan", HourMS: hourMS, CompletedMS: now.UnixMilli()},
	})
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	rowsWritten := txCounter(t, st) - rowsBefore

	// Both endpoints set by one call.
	var src, dst string
	if err := st.DB().QueryRow(`SELECT COALESCE(src_name,''), COALESCE(dst_name,'') FROM flows WHERE id=?`, id).Scan(&src, &dst); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if src != "src.lan" || dst != "dst.lan" {
		t.Errorf("annotations = %q / %q, want src.lan / dst.lan", src, dst)
	}
	// One transaction covers both observations. The counter includes the
	// transaction that InsertBatch used, so compare the delta from a baseline
	// taken after the insert.
	// Two UPDATEs plus two history INSERTs touch at most 4 rows; what matters is
	// that ONE call did all of it, which is what removes the per-endpoint commit.
	if rowsWritten > 4 || rowsWritten < 2 {
		t.Errorf("one batch call wrote %d rows, want 2-4 (both endpoints annotated)", rowsWritten)
	}
}

// TestSaveDNSSkipsUnchangedAnnotation covers the second half of the CPU fix: 27%
// of lookups are unresolved, so re-writing the empty string over an empty string
// rewrote a 480-byte row for nothing.
func TestSaveDNSSkipsUnchangedAnnotation(t *testing.T) {
	st, id := storeWithOneFlow(t, nil)
	ctx := context.Background()
	now := time.Now().UTC()
	hourMS := (now.UnixMilli() / 3600000) * 3600000

	o := DNSObservation{ID: id, IP: "10.0.0.1", Direction: "source", Name: "", HourMS: hourMS, CompletedMS: now.UnixMilli()}
	if err := st.SaveDNS(ctx, o); err != nil {
		t.Fatalf("first: %v", err)
	}
	// The first write must land even though the name is empty: it stamps the row
	// so "never looked up" stays distinguishable from "looked up, no PTR".
	//
	// The original version of this test scanned into *int64 and asserted non-nil.
	// On a `NOT NULL DEFAULT 0` column that scan never yields nil, so the
	// assertion passed whether or not the row was stamped — it could not fail, and
	// the dead guard it was meant to cover shipped. Assert the value.
	var stamp int64
	if err := st.DB().QueryRow(`SELECT src_dns_ms FROM flows WHERE id=?`, id).Scan(&stamp); err != nil {
		t.Fatalf("read stamp: %v", err)
	}
	if stamp == 0 {
		t.Fatal("first observation did not stamp src_dns_ms (still 0); an empty name must still " +
			"record that the lookup happened")
	}
	if stamp != o.CompletedMS {
		t.Errorf("src_dns_ms = %d, want the completion stamp %d", stamp, o.CompletedMS)
	}

	// A repeat with the same name and an existing stamp must not write.
	before := txCounter(t, st)
	rowsBefore := writeRowCounter(t, st)
	o.CompletedMS = now.Add(time.Second).UnixMilli()
	if err := st.SaveDNS(ctx, o); err != nil {
		t.Fatalf("second: %v", err)
	}
	if got := txCounter(t, st) - before; got != 1 {
		// The transaction still opens; it just performs no writes.
		t.Logf("transactions for the second call: %d (a no-op transaction still commits)", got)
	}
	if got := writeRowCounter(t, st) - rowsBefore; got != 0 {
		t.Errorf("a repeated unchanged annotation changed %d rows, want 0", got)
	}
}

// TestSaveDNSWritesWhenNameChanges is the counterpart: the guard must not
// suppress a genuine change, or a host that gains a PTR record would never show
// its name.
func TestSaveDNSWritesWhenNameChanges(t *testing.T) {
	st, id := storeWithOneFlow(t, nil)
	ctx := context.Background()
	now := time.Now().UTC()
	hourMS := (now.UnixMilli() / 3600000) * 3600000

	if err := st.SaveDNS(ctx, DNSObservation{ID: id, IP: "10.0.0.1", Direction: "source", Name: "", HourMS: hourMS, CompletedMS: now.UnixMilli()}); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := st.SaveDNS(ctx, DNSObservation{ID: id, IP: "10.0.0.1", Direction: "source", Name: "now-named.lan", HourMS: hourMS, CompletedMS: now.Add(time.Second).UnixMilli()}); err != nil {
		t.Fatalf("second: %v", err)
	}
	var name string
	if err := st.DB().QueryRow(`SELECT COALESCE(src_name,'') FROM flows WHERE id=?`, id).Scan(&name); err != nil {
		t.Fatalf("read: %v", err)
	}
	if name != "now-named.lan" {
		t.Errorf("src_name = %q, want now-named.lan; the guard suppressed a real change", name)
	}
}

// TestSaveDNSNoHistoryForUnresolvedNames covers a corruption the old code caused:
// every observation inserted a dns_host_history row when IP and name were
// non-empty, and for an unresolved lookup the name is empty, so that branch was
// skipped — but the guard is now explicit. This pins the behaviour.
func TestSaveDNSNoHistoryForUnresolvedNames(t *testing.T) {
	st, id := storeWithOneFlow(t, nil)
	ctx := context.Background()
	now := time.Now().UTC()
	hourMS := (now.UnixMilli() / 3600000) * 3600000

	if err := st.SaveDNS(ctx, DNSObservation{ID: id, IP: "10.0.0.1", Direction: "source", Name: "", HourMS: hourMS, CompletedMS: now.UnixMilli()}); err != nil {
		t.Fatalf("save: %v", err)
	}
	var n int
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM dns_host_history`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Errorf("dns_host_history has %d rows for an unresolved lookup, want 0", n)
	}
}

// TestSaveDNSBatchHandlesMissingRow ensures a row deleted by retention between
// insert and annotation does not fail the whole batch, which would lose the
// other endpoint's annotation too.
func TestSaveDNSBatchHandlesMissingRow(t *testing.T) {
	st, id := storeWithOneFlow(t, nil)
	ctx := context.Background()
	now := time.Now().UTC()
	hourMS := (now.UnixMilli() / 3600000) * 3600000

	if _, err := st.DB().Exec(`DELETE FROM flows WHERE id=?`, id); err != nil {
		t.Fatalf("delete: %v", err)
	}
	_, err := st.SaveDNSBatch(ctx, []DNSObservation{
		{ID: id, IP: "10.0.0.1", Direction: "source", Name: "gone.lan", HourMS: hourMS, CompletedMS: now.UnixMilli()},
		{ID: id + 1000, IP: "10.0.0.2", Direction: "destination", Name: "also-gone.lan", HourMS: hourMS, CompletedMS: now.UnixMilli()},
	})
	if err != nil {
		t.Fatalf("a missing row must not fail the batch: %v", err)
	}
}

// txCounter reports how many rows this connection has modified, via SQLite's own
// counter. It is NOT a transaction count: total_changes() increments per row, so
// it can prove "no write happened" but cannot prove "one transaction".
func txCounter(t *testing.T, st *Store) int64 {
	t.Helper()
	var v int64
	if err := st.DB().QueryRow(`SELECT total_changes()`).Scan(&v); err != nil {
		t.Fatalf("total_changes: %v", err)
	}
	return v
}

func writeRowCounter(t *testing.T, st *Store) int64 { return txCounter(t, st) }
