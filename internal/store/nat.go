package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/a-bali/nfvis/internal/model"
)

// NatPolicy configures pre-NAT/post-NAT duplicate suppression.
//
// ng_netflow exports both views of one forwarded packet from the same interface
// node: the source address and source port rewritten by PAT, the same
// destination, packets and bytes, and export timestamps up to a second apart.
// Only one of the two carries a source address that is not the firewall's own
// WAN address, and that one — the pre-NAT view — is the survivor, because it
// holds the real source identity.
type NatPolicy struct {
	// WANAddresses are the firewall's own addresses on the NAT boundary. A pair
	// is one record whose source is in this set and one whose source is not.
	WANAddresses []string
	// Window bounds the distance between the two export timestamps.
	Window time.Duration
}

// natState is the resolved form of NatPolicy.
type natState struct {
	wan      map[string]struct{}
	windowMS int64
}

func newNatState(p NatPolicy) *natState {
	if len(p.WANAddresses) == 0 {
		return nil
	}
	n := &natState{wan: make(map[string]struct{}, len(p.WANAddresses)), windowMS: p.Window.Milliseconds()}
	for _, a := range p.WANAddresses {
		n.wan[a] = struct{}{}
	}
	if n.windowMS <= 0 {
		n.windowMS = natPairWindowMS
	}
	return n
}

// natPairWindowMS is the default window between the two views of one packet.
// Measured pairs sat under a second apart; two seconds leaves room for export
// jitter without merging unrelated flows.
const natPairWindowMS = int64(2000)

func (n *natState) isWANSource(src string) bool {
	if n == nil {
		return false
	}
	_, ok := n.wan[src]
	return ok
}

// natMateColumns is the subset of flow columns a NAT comparison needs.
const natMateColumns = `input_if, output_if, start_ms, end_ms, src_addr, src_port,
	dst_addr, dst_port, protocol, bytes, packets, src_vlan, dst_vlan`

// storedMate is the stored row that pairs with an incoming record.
type storedMate struct {
	id   int64
	flow model.Flow
}

// findStoredMate returns the stored record that is the other NAT view of f:
// same protocol, destination, destination port, packets and bytes, within the
// window, and with a source address on the opposite side of the NAT boundary.
func (s *Store) findStoredMate(ctx context.Context, tx *sql.Tx, f *model.Flow) (*storedMate, error) {
	if s.nat == nil {
		return nil, nil
	}
	incomingIsWAN := s.nat.isWANSource(f.SrcAddr)
	rows, err := tx.QueryContext(ctx,
		`SELECT id, `+natMateColumns+` FROM flows
		 WHERE protocol = ? AND dst_addr = ? AND dst_port = ? AND packets = ? AND bytes = ?
		   AND ABS(end_ms - ?) <= ?
		 ORDER BY id`,
		f.Protocol, f.DstAddr, f.DstPort, int64(f.Packets), int64(f.Bytes), f.EndMS, s.nat.windowMS)
	if err != nil {
		return nil, fmt.Errorf("query nat mate: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var m storedMate
		fl := &m.flow
		if err := rows.Scan(&m.id, &fl.InputIf, &fl.OutputIf, &fl.StartMS, &fl.EndMS,
			&fl.SrcAddr, &fl.SrcPort, &fl.DstAddr, &fl.DstPort, &fl.Protocol,
			&fl.Bytes, &fl.Packets, &fl.SrcVLAN, &fl.DstVLAN); err != nil {
			return nil, fmt.Errorf("scan nat mate: %w", err)
		}
		// A genuine pair has exactly one WAN-sourced member. Anything else is a
		// separate flow that merely shares the counters.
		if s.nat.isWANSource(fl.SrcAddr) == incomingIsWAN {
			continue
		}
		fl.Exporter = f.Exporter
		m.flow = *fl
		return &m, nil
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("nat mate rows: %w", err)
	}
	return nil, nil
}

// dropPostNATMate reports whether f is the post-NAT view of an already stored
// record, in which case f is redundant and must not be inserted.
func (s *Store) dropPostNATMate(ctx context.Context, tx *sql.Tx, f *model.Flow) (bool, error) {
	if s.nat == nil || !s.nat.isWANSource(f.SrcAddr) {
		return false, nil
	}
	mate, err := s.findStoredMate(ctx, tx, f)
	if err != nil || mate == nil {
		return false, err
	}
	return true, nil
}

// replaceStoredPostNAT handles the reverse arrival order: f is the pre-NAT view
// and a post-NAT row for the same packet is already stored. That row is deleted,
// and its contribution — already committed by an earlier batch, or counted in
// this batch's accumulator — is subtracted so the one surviving traversal is
// counted once.
func (s *Store) replaceStoredPostNAT(ctx context.Context, tx *sql.Tx, f *model.Flow, aggs *batchAggregates) error {
	if s.nat == nil || s.nat.isWANSource(f.SrcAddr) {
		return nil // post-NAT arrivals are handled by dropPostNATMate
	}
	mate, err := s.findStoredMate(ctx, tx, f)
	if err != nil || mate == nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM flows WHERE id = ?`, mate.id); err != nil {
		return fmt.Errorf("delete superseded post-NAT flow: %w", err)
	}
	// The survivor's own row is subtracted from this batch's accumulator only if
	// the batch is the one that counted it. A row committed by an earlier batch is
	// not in the accumulator at all, and subtracting it there would produce a
	// negative contribution on top of the rebuild.
	if aggs.counted(flowDedupKey(&mate.flow)) {
		aggs.sub(mate.flow)
	}
	return s.rebuildAggregatesFor(ctx, tx, &mate.flow)
}

// rebuildAggregatesFor re-derives every aggregate bucket the removed row could
// have contributed to, from the rows that still exist, so a deletion can never
// leave a stale or negative total behind. Callers must have applied the deletion
// in the same transaction.
func (s *Store) rebuildAggregatesFor(ctx context.Context, tx *sql.Tx, removed *model.Flow) error {
	end := removed.EndMS
	if end <= 0 {
		end = removed.ReceivedAtMS
	}
	hour := (end / hourMS) * hourMS

	clears := []struct {
		table string
		query string
		args  []any
	}{
		{`agg_exporter`, `DELETE FROM agg_exporter WHERE hour_ms = ? AND exporter = ?`, []any{hour, removed.Exporter}},
		{`agg_host`, `DELETE FROM agg_host WHERE hour_ms = ? AND host IN (?, ?)`, []any{hour, removed.SrcAddr, removed.DstAddr}},
		{`agg_interface`, `DELETE FROM agg_interface WHERE hour_ms = ? AND if_index IN (?, ?)`, []any{hour, removed.InputIf, removed.OutputIf}},
		{`agg_vlan`, `DELETE FROM agg_vlan WHERE hour_ms = ? AND vlan IN (?, ?)`, []any{hour, removed.SrcVLAN, removed.DstVLAN}},
		{`agg_protocol`, `DELETE FROM agg_protocol WHERE hour_ms = ? AND protocol = ?`, []any{hour, removed.Protocol}},
	}
	for _, c := range clears {
		if _, err := tx.ExecContext(ctx, c.query, c.args...); err != nil {
			return fmt.Errorf("clear %s for rebuild: %w", c.table, err)
		}
	}

	// Rows in this hour, and nothing else, feed the re-aggregation.
	const inHour = ` FROM flows WHERE end_ms >= ? AND end_ms < ? `

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agg_exporter(hour_ms, exporter, bytes, packets, flows, sampled_flows, unknown_flows)
		SELECT ?, exporter, SUM(bytes), SUM(packets), COUNT(*), SUM(sampled), 0`+inHour+`
		GROUP BY exporter`, hour, hour, hour+hourMS); err != nil {
		return fmt.Errorf("rebuild agg_exporter: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agg_protocol(hour_ms, protocol, bytes, packets, flows, sampled_flows, unknown_flows)
		SELECT ?, protocol, SUM(bytes), SUM(packets), COUNT(*), SUM(sampled), 0`+inHour+`
		GROUP BY protocol`, hour, hour, hour+hourMS); err != nil {
		return fmt.Errorf("rebuild agg_protocol: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agg_host(hour_ms, host, direction, bytes, packets, flows, sampled_flows, unknown_flows)
		SELECT ?, src_addr, 'source', SUM(bytes), SUM(packets), COUNT(*), SUM(sampled),
		       SUM(CASE WHEN src_addr = '' THEN 1 ELSE 0 END)`+inHour+`
		GROUP BY src_addr`, hour, hour, hour+hourMS); err != nil {
		return fmt.Errorf("rebuild agg_host source: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agg_host(hour_ms, host, direction, bytes, packets, flows, sampled_flows, unknown_flows)
		SELECT ?, dst_addr, 'destination', SUM(bytes), SUM(packets), COUNT(*), SUM(sampled),
		       SUM(CASE WHEN dst_addr = '' THEN 1 ELSE 0 END)`+inHour+`
		GROUP BY dst_addr`, hour, hour, hour+hourMS); err != nil {
		return fmt.Errorf("rebuild agg_host destination: %w", err)
	}

	// agg_interface counts an ingress bucket per input_if and an egress bucket
	// per output_if.
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agg_interface(hour_ms, exporter, if_index, direction, label, bytes, packets, flows, sampled_flows, unknown_flows)
		SELECT ?, exporter, input_if, ?, MAX(input_label), SUM(bytes), SUM(packets), COUNT(*),
		       SUM(sampled), SUM(CASE WHEN input_if = 0 THEN 1 ELSE 0 END)`+inHour+`
		GROUP BY exporter, input_if`,
		hour, model.DirectionIngress, hour, hour+hourMS); err != nil {
		return fmt.Errorf("rebuild agg_interface ingress: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agg_interface(hour_ms, exporter, if_index, direction, label, bytes, packets, flows, sampled_flows, unknown_flows)
		SELECT ?, exporter, output_if, ?, MAX(output_label), SUM(bytes), SUM(packets), COUNT(*),
		       SUM(sampled), SUM(CASE WHEN output_if = 0 THEN 1 ELSE 0 END)`+inHour+`
		GROUP BY exporter, output_if`,
		hour, model.DirectionEgress, hour, hour+hourMS); err != nil {
		return fmt.Errorf("rebuild agg_interface egress: %w", err)
	}

	// agg_vlan counts a source bucket per src_vlan and a destination bucket per
	// dst_vlan.
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agg_vlan(hour_ms, vlan, direction, bytes, packets, flows, sampled_flows, unknown_flows)
		SELECT ?, src_vlan, ?, SUM(bytes), SUM(packets), COUNT(*), SUM(sampled),
		       SUM(CASE WHEN src_vlan < 0 THEN 1 ELSE 0 END)`+inHour+`
		GROUP BY src_vlan`,
		hour, model.DirectionSource, hour, hour+hourMS); err != nil {
		return fmt.Errorf("rebuild agg_vlan source: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agg_vlan(hour_ms, vlan, direction, bytes, packets, flows, sampled_flows, unknown_flows)
		SELECT ?, dst_vlan, ?, SUM(bytes), SUM(packets), COUNT(*), SUM(sampled),
		       SUM(CASE WHEN dst_vlan < 0 THEN 1 ELSE 0 END)`+inHour+`
		GROUP BY dst_vlan`,
		hour, model.DirectionDest, hour, hour+hourMS); err != nil {
		return fmt.Errorf("rebuild agg_vlan destination: %w", err)
	}
	return nil
}
