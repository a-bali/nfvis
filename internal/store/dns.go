package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// DNSObservation is one endpoint lookup associated with a stored flow.
type DNSObservation struct {
	ID                  int64
	IP, Direction, Name string
	HourMS, CompletedMS int64
}

// SaveDNS writes flow labels and historical host observations atomically.
func (s *Store) SaveDNS(ctx context.Context, o DNSObservation) error {
	_, err := s.SaveDNSBatch(ctx, []DNSObservation{o})
	return err
}

// SaveDNSBatch writes several observations in one transaction, and skips those
// that would write nothing new.
//
// Both callers of this path are hot: a flow's two endpoints are annotated
// separately, so a naive per-observation transaction cost two commits and two
// full-row rewrites per decoded flow — 236 transactions/s to annotate 106
// flows/s on one measured instance, which was essentially the entire CPU the
// process used. Grouping them halves the commits; the guard removes the rest.
//
// The guard matters because a SQLite UPDATE rewrites the whole row (there is no
// in-place column update) and 27% of lookups are unresolved, so a conditional
// write avoids rewriting a ~480-byte, 43-column row to store the empty string it
// already holds.
func (s *Store) SaveDNSBatch(ctx context.Context, obs []DNSObservation) (int, error) {
	if len(obs) == 0 {
		return 0, nil
	}
	// Decide which observations would actually write before opening a
	// transaction. Two reasons:
	//
	//  1. A transaction that only reads still takes the write lock under
	//     BEGIN IMMEDIATE and holds it to COMMIT, so a whole batch of no-op
	//     annotations would block the ingest writer for nothing.
	//  2. The pre-read inside the transaction widens the window in which the
	//     transaction holds the lock; doing it outside keeps the write phase as
	//     short as it was before batching.
	pending := make([]DNSObservation, 0, len(obs))
	for _, o := range obs {
		write, err := s.dnsNeedsWrite(ctx, o)
		if err != nil {
			return 0, err
		}
		if write {
			pending = append(pending, o)
		}
	}
	if len(pending) == 0 {
		return 0, nil
	}

	tx, err := s.beginImmediate(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	for _, o := range pending {
		if err := s.applyDNSObservation(ctx, tx, o); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return len(pending), nil
}

// dnsNeedsWrite reports whether an observation would change the row. It reads
// outside the write transaction so a no-op batch never takes the write lock.
func (s *Store) dnsNeedsWrite(ctx context.Context, o DNSObservation) (bool, error) {
	column, stamp := "src_name", "src_dns_ms"
	if o.Direction == "destination" {
		column, stamp = "dst_name", "dst_dns_ms"
	}
	var currentName string
	var currentStamp int64
	query := fmt.Sprintf("SELECT COALESCE(%s,''), COALESCE(%s,0) FROM flows WHERE id=?", column, stamp)
	switch err := s.DB().QueryRowContext(ctx, query, o.ID).Scan(&currentName, &currentStamp); {
	case errors.Is(err, sql.ErrNoRows):
		// The row is gone (retention, or a dedup merge): nothing to annotate.
		return false, nil
	case err != nil:
		return false, err
	}
	// A stamp of 0 means this endpoint was never looked up. It must be written even
	// when the name is empty, or "never looked up" and "looked up, no PTR" become
	// indistinguishable.
	//
	// The columns are `INTEGER NOT NULL DEFAULT 0`, so scanning into
	// sql.NullInt64 would always report Valid and this branch would be dead code —
	// which is exactly what an earlier version did, leaving every unresolved
	// address unstamped.
	if currentStamp == 0 {
		return true, nil
	}
	if currentName != o.Name {
		return true, nil
	}
	// The name is unchanged. Still write when the hour differs, because
	// dns_host_history is keyed by hour and a cached name crossing an hour
	// boundary would otherwise leave that hour with no row — the chart for that
	// hour would fall back to the bare IP.
	history, err := s.hasHistoryForHour(ctx, o)
	if err != nil {
		return false, err
	}
	return !history, nil
}

// hasHistoryForHour reports whether dns_host_history already holds this name for
// this host and hour.
func (s *Store) hasHistoryForHour(ctx context.Context, o DNSObservation) (bool, error) {
	if o.Name == "" || o.IP == "" {
		return true, nil // nothing to record for an unresolved lookup
	}
	var n int
	err := s.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM dns_host_history WHERE hour_ms=? AND host=? AND direction=? AND name=?`,
		o.HourMS, o.IP, o.Direction, o.Name).Scan(&n)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// applyDNSObservation performs one observation's writes inside an existing
// transaction. The caller has already established that a write is needed.
func (s *Store) applyDNSObservation(ctx context.Context, tx *sql.Tx, o DNSObservation) error {
	column, stamp := "src_name", "src_dns_ms"
	if o.Direction == "destination" {
		column, stamp = "dst_name", "dst_dns_ms"
	}

	res, err := tx.ExecContext(ctx,
		fmt.Sprintf("UPDATE flows SET %s=?, %s=? WHERE id=?", column, stamp),
		o.Name, o.CompletedMS, o.ID)
	if err != nil {
		return err
	}
	count, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		// The row vanished between the decision and the write (retention, or a
		// dedup merge). Nothing to annotate.
		return nil
	}
	// Only a resolved name is history worth keeping; an unresolved lookup adds no
	// row and would otherwise write an empty-name entry per hour per address.
	if o.Name != "" && o.IP != "" {
		if _, err = tx.ExecContext(ctx, `INSERT INTO dns_host_history(hour_ms,host,direction,name,last_ms) VALUES(?,?,?,?,?) ON CONFLICT(hour_ms,host,direction,name) DO UPDATE SET last_ms=MAX(last_ms,excluded.last_ms)`, o.HourMS, o.IP, o.Direction, o.Name, o.CompletedMS); err != nil {
			return err
		}
	}
	return nil
}

// HistoricalNames returns captured names by IP for a range and direction, one
// entry per distinct name, most recently observed first.
//
// The table stores one row per hour, so a host whose name never changed would
// otherwise contribute an entry per hour it was looked up in: a 30-day range
// returned hundreds of copies of the same name per address, and every consumer
// that joined the list showed the name repeated ("talos2.lan, talos2.lan"). The
// repetition carried no information — the same name observed in five hours is
// one name — so the query collapses it and keeps MAX(last_ms) for ordering.
func (s *Store) HistoricalNames(ctx context.Context, r Range, direction string) (map[string][]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT host, name FROM dns_host_history WHERE hour_ms >= ? AND hour_ms < ? AND direction=? GROUP BY host, name ORDER BY MAX(last_ms) DESC`, (r.FromMS/hourMS)*hourMS, r.ToMS, direction)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][]string{}
	for rows.Next() {
		var ip, name string
		if err := rows.Scan(&ip, &name); err != nil {
			return nil, err
		}
		out[ip] = append(out[ip], name)
	}
	return out, rows.Err()
}
