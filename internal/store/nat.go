package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/a-bali/nfvis/internal/config"
	"github.com/a-bali/nfvis/internal/model"
)

// This file holds the duplicate-suppression policy. Nothing here runs unless the
// operator has configured the topology the policy is allowed to exploit: a
// matching payload alone is never treated as proof that two records describe the
// same packet.
//
// Conservative direction: a missed collapse costs disk and an inflated interface
// traversal count; a wrong collapse silently deletes a real flow. Every rule
// therefore fails closed — when the stored state does not prove a pair, the
// record is stored.

// dedupActionKind is what the writer does with one incoming record.
type dedupActionKind int

const (
	// actionInsert stores the record as a new row.
	actionInsert dedupActionKind = iota
	// actionDrop skips the record: an equivalent row already exists.
	actionDrop
	// actionPromote rewrites an existing row's interfaces to this record's and
	// moves its aggregate contribution.
	actionPromote
	// actionReplace deletes an existing row — the redundant post-NAT view — and
	// stores this record in its place.
	actionReplace
)

// dedupAction is the decision for one record.
type dedupAction struct {
	kind dedupActionKind
	// id is the existing row for actionPromote/actionReplace.
	id int64
	// stored is the existing row as read back, used to subtract exactly what it
	// contributed to the aggregates.
	stored model.Flow
}

// storedColumns is the column list needed to subtract a stored row from every
// aggregate family and to prove that two records are the same observation.
const storedColumns = `id, exporter, start_ms, end_ms, received_ms, input_if, input_label,
	output_if, output_label, src_addr, dst_addr, src_port, dst_port, protocol, tcp_flags,
	src_tos, dst_tos, src_as, dst_as, src_vlan, dst_vlan, src_vlan_source, dst_vlan_source,
	bytes, packets, sampled, sampling_interval, sampling_algorithm, sampling_source,
	direction, direction_known`

func scanStoredRow(scanner interface{ Scan(...any) error }) (int64, model.Flow, error) {
	var (
		id int64
		f  model.Flow
	)
	err := scanner.Scan(&id, &f.Exporter, &f.StartMS, &f.EndMS, &f.ReceivedAtMS,
		&f.InputIf, &f.InputLabel, &f.OutputIf, &f.OutputLabel,
		&f.SrcAddr, &f.DstAddr, &f.SrcPort, &f.DstPort, &f.Protocol, &f.TCPFlags,
		&f.SrcTOS, &f.DstTOS, &f.SrcAS, &f.DstAS, &f.SrcVLAN, &f.DstVLAN,
		&f.SrcVLANSource, &f.DstVLANSource, &f.Bytes, &f.Packets, &f.Sampled,
		&f.SamplingInterval, &f.SamplingAlgorithm, &f.SamplingSource,
		&f.Direction, &f.DirectionKnown)
	return id, f, err
}

// sameFrame reports whether two records may stand in for one another when only
// the interface columns of the stored row are rewritten in place.
//
// Promotion issues UPDATE flows SET input_if=..., output_if=... — nothing else —
// so every other column of the stored row survives. If any of them differs
// between the two records, the surviving row would report the child's values
// while the aggregates (which are moved) describe the parent's. Requiring the
// non-interface fields to be equal is therefore not pedantry: it is what makes
// "the aggregates describe the stored rows" true.
//
// The aggregate hour is compared too, because a record with no exported window
// is bucketed by receipt time; two copies received in different hours belong to
// different buckets and cannot be merged in place.
func sameFrame(stored, incoming *model.Flow) bool {
	return stored.StartMS == incoming.StartMS &&
		stored.EndMS == incoming.EndMS &&
		stored.SrcAddr == incoming.SrcAddr &&
		stored.DstAddr == incoming.DstAddr &&
		stored.SrcPort == incoming.SrcPort &&
		stored.DstPort == incoming.DstPort &&
		stored.Protocol == incoming.Protocol &&
		stored.TCPFlags == incoming.TCPFlags &&
		stored.SrcTOS == incoming.SrcTOS &&
		stored.DstTOS == incoming.DstTOS &&
		stored.SrcAS == incoming.SrcAS &&
		stored.DstAS == incoming.DstAS &&
		stored.SrcVLAN == incoming.SrcVLAN &&
		stored.DstVLAN == incoming.DstVLAN &&
		stored.SrcVLANSource == incoming.SrcVLANSource &&
		stored.DstVLANSource == incoming.DstVLANSource &&
		stored.Bytes == incoming.Bytes &&
		stored.Packets == incoming.Packets &&
		stored.Sampled == incoming.Sampled &&
		stored.SamplingInterval == incoming.SamplingInterval &&
		stored.SamplingAlgorithm == incoming.SamplingAlgorithm &&
		stored.SamplingSource == incoming.SamplingSource &&
		stored.Direction == incoming.Direction &&
		stored.DirectionKnown == incoming.DirectionKnown &&
		stored.OutputIf == incoming.OutputIf &&
		flowHourMS(*stored) == flowHourMS(*incoming)
}

// dedupEnabled reports whether any duplicate suppression is configured. When it
// is off, records are written with a NULL dedup key so they are never candidates.
func (s *Store) dedupEnabled() bool {
	return s.pc != nil || s.nat != nil
}

// dedupAction decides what to do with f, considering rows already committed and
// rows inserted earlier in the same transaction. The two mechanisms have
// disjoint interests — NAT pairs differ in source address, parent/child copies
// do not — so NAT is evaluated first.
func (s *Store) dedupAction(ctx context.Context, tx *sql.Tx, f *model.Flow) (dedupAction, error) {
	if !s.dedupEnabled() {
		return dedupAction{}, nil
	}
	if s.nat != nil {
		action, err := s.natAction(ctx, tx, f)
		if err != nil || action.kind != actionInsert {
			return action, err
		}
	}
	if s.pc != nil {
		return s.parentChildAction(ctx, tx, f)
	}
	return dedupAction{}, nil
}

// parentChildAction collapses a child-interface copy of a frame into the parent
// copy. It acts only when all of the following hold:
//
//   - the exporter and the two input interfaces are a configured link,
//   - the records carry the same payload fingerprint (same exporter, export
//     interval, endpoints, protocol and counters),
//   - they are the same observation in every column promotion does not rewrite
//     (see sameFrame), and
//   - exactly one stored row stands in the required relation to this record.
//
// Anything else — two different interfaces that are not a configured link, the
// same interface twice, an exporter without links, several possible parents, or
// two records that differ in any field the stored row would keep — is stored as
// its own row. A record on an unconfigured interface (ifIndex 0 included) is
// never treated as a copy of anything and never blocks the pair it happens to
// share a fingerprint with: it is simply not part of the link.
func (s *Store) parentChildAction(ctx context.Context, tx *sql.Tx, f *model.Flow) (dedupAction, error) {
	key := flowDedupKey(f)
	if key == nil || !s.pc.hasExporter(f.Exporter) {
		return dedupAction{}, nil
	}
	rows, err := tx.QueryContext(ctx,
		`SELECT `+storedColumns+` FROM flows WHERE dedup_key = ? ORDER BY id`, key)
	if err != nil {
		return dedupAction{}, fmt.Errorf("query parent/child duplicate: %w", err)
	}
	defer rows.Close()
	var (
		parentCandidate *dedupAction // stored row is the parent, this record the child
		childCandidate  *dedupAction // stored row is the child, this record the parent
		parents         int
		children        int
	)
	for rows.Next() {
		id, stored, err := scanStoredRow(rows)
		if err != nil {
			return dedupAction{}, fmt.Errorf("scan parent/child duplicate: %w", err)
		}
		if !sameFrame(&stored, f) {
			// The stored row would keep its own values for a column that
			// differs, so it is a different observation, not a copy.
			continue
		}
		switch s.pc.relation(f.Exporter, stored.InputIf, f.InputIf) {
		case relationParent:
			// The stored row is the parent and this is the child: the parent stays.
			parents++
			candidate := dedupAction{kind: actionDrop, id: id, stored: stored}
			parentCandidate = &candidate
		case relationChild:
			// The stored row is the child: rewrite it to the parent's interfaces.
			children++
			candidate := dedupAction{kind: actionPromote, id: id, stored: stored}
			childCandidate = &candidate
		}
	}
	if err := rows.Err(); err != nil {
		return dedupAction{}, fmt.Errorf("parent/child duplicate rows: %w", err)
	}
	// One possible parent and no possible child, or one possible child and no
	// possible parent, is the only shape that is a proven pair. A group that
	// contains both (a three-node chain, or a duplicated export) is left alone:
	// picking one would discard a traversal on a guess.
	switch {
	case parents == 1 && children == 0:
		return *parentCandidate, nil
	case children == 1 && parents == 0:
		return *childCandidate, nil
	}
	return dedupAction{}, nil
}

// natOption is one configured firewall's NAT topology.
type natOption struct {
	preIf  uint32
	postIf uint32
}

// natState is the resolved NAT policy. It is nil unless WAN addresses and at
// least one exporter topology are configured: without both, a WAN-sourced record
// cannot be told apart from the firewall's own traffic or from an unrelated
// record that merely shares its counters.
type natState struct {
	wan       map[string]struct{}
	exporters map[string]natOption
	windowMS  int64
}

// natPairWindowMS is the default window between the two views of one packet.
// Measured pairs sat under a second apart; two seconds leaves room for export
// jitter without merging unrelated flows.
const natPairWindowMS = int64(2000)

func newNatState(wan []string, exporters []config.NATExporter, window time.Duration) *natState {
	if len(wan) == 0 || len(exporters) == 0 {
		return nil
	}
	n := &natState{
		wan:       make(map[string]struct{}, len(wan)),
		exporters: make(map[string]natOption, len(exporters)),
		windowMS:  window.Milliseconds(),
	}
	for _, a := range wan {
		n.wan[a] = struct{}{}
	}
	for _, e := range exporters {
		n.exporters[e.Address] = natOption{preIf: e.PreInterface, postIf: e.PostInterface}
	}
	if n.windowMS <= 0 {
		n.windowMS = natPairWindowMS
	}
	return n
}

func (n *natState) isWANSource(src string) bool {
	if n == nil {
		return false
	}
	_, ok := n.wan[src]
	return ok
}

// natAction decides whether f is one view of a NAT pair whose other view is
// stored. A WAN-sourced record is the post-NAT view and is dropped when its
// pre-NAT mate exists; a non-WAN record on the pre-NAT interface is the surviving
// view and replaces a stored post-NAT row.
//
// Records whose exporter or input interface is not part of the configured
// topology are never candidates, which is what keeps the firewall's own
// WAN-sourced traffic (exported on a different interface) and other exporters'
// records out of the pairing.
func (s *Store) natAction(ctx context.Context, tx *sql.Tx, f *model.Flow) (dedupAction, error) {
	opt, ok := s.nat.exporters[f.Exporter]
	if !ok {
		return dedupAction{}, nil
	}
	wan := s.nat.isWANSource(f.SrcAddr)
	switch {
	case wan && f.InputIf == opt.postIf:
		mate, err := s.findNatMate(ctx, tx, f, opt.preIf, false)
		if err != nil || mate == nil {
			return dedupAction{}, err
		}
		// The post-NAT view of a stored pre-NAT record: keep the one with the
		// real source identity.
		return dedupAction{kind: actionDrop, id: mate.id, stored: mate.stored}, nil
	case !wan && f.InputIf == opt.preIf:
		mate, err := s.findNatMate(ctx, tx, f, opt.postIf, true)
		if err != nil || mate == nil {
			return dedupAction{}, err
		}
		// The pre-NAT view of a stored post-NAT record: the stored row is the
		// redundant one and goes.
		return dedupAction{kind: actionReplace, id: mate.id, stored: mate.stored}, nil
	}
	return dedupAction{}, nil
}

// findNatMate returns the single stored row that is the other view of f, or nil
// when there is none or more than one. More than one candidate is not a pair: the
// policy refuses to guess.
func (s *Store) findNatMate(ctx context.Context, tx *sql.Tx, f *model.Flow, mateIf uint32, wantWAN bool) (*dedupAction, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT `+storedColumns+` FROM flows
		 WHERE exporter = ? AND protocol = ? AND dst_addr = ? AND dst_port = ?
		   AND packets = ? AND bytes = ? AND input_if = ?
		   AND end_ms BETWEEN ? AND ?
		 ORDER BY id`,
		f.Exporter, f.Protocol, f.DstAddr, f.DstPort, int64(f.Packets), int64(f.Bytes), mateIf,
		f.EndMS-s.nat.windowMS, f.EndMS+s.nat.windowMS)
	if err != nil {
		return nil, fmt.Errorf("query nat mate: %w", err)
	}
	defer rows.Close()
	var mate *dedupAction
	count := 0
	for rows.Next() {
		id, stored, err := scanStoredRow(rows)
		if err != nil {
			return nil, fmt.Errorf("scan nat mate: %w", err)
		}
		// The mate must be the opposite side of the NAT boundary and share the
		// output interface; anything else merely shares the counters.
		if s.nat.isWANSource(stored.SrcAddr) != wantWAN || stored.OutputIf != f.OutputIf {
			continue
		}
		count++
		mate = &dedupAction{id: id, stored: stored}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("nat mate rows: %w", err)
	}
	if count != 1 {
		return nil, nil
	}
	return mate, nil
}
