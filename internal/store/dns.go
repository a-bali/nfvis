package store

import (
	"context"
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
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	column, stamp := "src_name", "src_dns_ms"
	if o.Direction == "destination" {
		column, stamp = "dst_name", "dst_dns_ms"
	}
	res, err := tx.ExecContext(ctx, fmt.Sprintf("UPDATE flows SET %s=?, %s=? WHERE id=?", column, stamp), o.Name, o.CompletedMS, o.ID)
	if err != nil {
		return err
	}
	count, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return tx.Commit()
	}
	if o.Name != "" && o.IP != "" {
		_, err = tx.ExecContext(ctx, `INSERT INTO dns_host_history(hour_ms,host,direction,name,last_ms) VALUES(?,?,?,?,?) ON CONFLICT(hour_ms,host,direction,name) DO UPDATE SET last_ms=MAX(last_ms,excluded.last_ms)`, o.HourMS, o.IP, o.Direction, o.Name, o.CompletedMS)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

// HistoricalNames returns captured names by IP for a range and direction.
func (s *Store) HistoricalNames(ctx context.Context, r Range, direction string) (map[string][]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT host,name FROM dns_host_history WHERE hour_ms >= ? AND hour_ms < ? AND direction=? ORDER BY last_ms DESC`, (r.FromMS/hourMS)*hourMS, r.ToMS, direction)
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
