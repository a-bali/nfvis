package store

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
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
//
// The two mechanisms act on the same packet, so they must not be evaluated as an
// either/or choice. The measured OPNsense export of one forwarded packet is
//
//	R1  parent  pre-NAT   in=2  src 10.10.10.194
//	R2  post-NAT          in=7  src 192.168.0.2
//	R3  child   pre-NAT   in=7  src 10.10.10.194
//
// R1/R3 is the parent/child pair and R1/R2 (or R3/R2) is the NAT pair. A
// single-action decision has to pick one of those and then inserts the record
// anyway, which is how R3 followed by R2 followed by R1 used to leave two rows.
// dedupPlan therefore evaluates both mechanisms over one read of the candidate
// group and commits to the smallest set of rows that represents it: the surviving
// pre-NAT copy on the parent interface, with every redundant view deleted and
// every aggregate contribution moved exactly once.

// dedupPlan is the set of row operations one incoming record implies.
type dedupPlan struct {
	// insert is the canonical surviving record, or nil when the record is fully
	// represented by rows that already exist.
	insert *model.Flow
	// delete holds stored rows that become redundant (a superseded NAT view, or a
	// pre-NAT copy being collapsed into this record).
	delete []storedRow
	// promote holds stored rows that must be rewritten to this record's interface
	// columns.
	promote []promotion
	// mergeTo is the row that already represents this record, when insert is nil.
	// It is the row a queued lookup belongs to.
	mergeTo int64
}

// promotion rewrites a stored row's interface columns to a canonical record's.
type promotion struct {
	id     int64
	stored model.Flow
}

// storedColumns is the column list needed to subtract a stored row from every
// aggregate family and to prove that two records are the same observation.
const storedColumns = `id, exporter, start_ms, end_ms, received_ms, input_if, input_label,
	output_if, output_label, src_addr, dst_addr, src_port, dst_port, protocol, tcp_flags,
	src_tos, dst_tos, src_as, dst_as, src_vlan, dst_vlan, src_vlan_source, dst_vlan_source,
	bytes, packets, sampled, sampling_interval, sampling_algorithm, sampling_source,
	direction, direction_known`

// storedRow is one row read back from the table.
type storedRow struct {
	id   int64
	flow model.Flow
}

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
// the interface columns of one row are rewritten in place.
//
// Promotion issues UPDATE flows SET input_if=..., output_if=... — nothing else —
// so every other column of the stored row survives. If any of them differs
// between the two records, the surviving row would report one record's values
// while the aggregates describe the other's. Requiring the non-interface fields
// to be equal is therefore not pedantry: it is what makes "the aggregates
// describe the stored rows" true.
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

// dedupPlan decides what to do with f given the rows already committed and the
// rows inserted earlier in the same transaction.
func (s *Store) dedupPlan(ctx context.Context, tx *sql.Tx, f *model.Flow) (dedupPlan, error) {
	if !s.dedupEnabled() {
		return dedupPlan{insert: f}, nil
	}
	candidates, err := s.candidatesFor(ctx, tx, f)
	if err != nil {
		return dedupPlan{}, err
	}
	// NAT first: it decides which pre-NAT copy is authoritative and which stored
	// rows are redundant views. Parent/child then decides whether the surviving
	// copy is itself a duplicate of another row.
	if s.nat != nil {
		if plan, decided, err := s.natPlan(ctx, tx, f, candidates); err != nil {
			return dedupPlan{}, err
		} else if decided {
			return plan, nil
		}
	}
	if s.pc != nil {
		return s.parentChildPlan(ctx, tx, f, candidates)
	}
	return dedupPlan{insert: f}, nil
}

// candidatesFor reads the rows a record could be a duplicate of: every stored row
// with the same payload fingerprint, plus, for NAT, rows that share the counters
// and window but differ in source (a post-NAT mate has a different source, so the
// fingerprint cannot find it).
func (s *Store) candidatesFor(ctx context.Context, tx *sql.Tx, f *model.Flow) ([]storedRow, error) {
	key := flowDedupKey(f)
	byID := map[int64]storedRow{}
	if key != nil && s.pc != nil {
		rows, err := tx.QueryContext(ctx, `SELECT `+storedColumns+` FROM flows WHERE dedup_key = ? ORDER BY id`, key)
		if err != nil {
			return nil, fmt.Errorf("query parent/child candidates: %w", err)
		}
		if err := collectRows(rows, byID); err != nil {
			return nil, err
		}
	}
	if s.nat != nil {
		opt, ok := s.nat.exporters[f.Exporter]
		if ok {
			var mateIf uint32
			var wantWAN bool
			switch {
			case s.nat.isWANSource(f.SrcAddr) && f.InputIf == opt.postIf:
				mateIf, wantWAN = opt.preIf, false
			case !s.nat.isWANSource(f.SrcAddr) && f.InputIf == opt.preIf:
				mateIf, wantWAN = opt.postIf, true
			}
			if mateIf != 0 {
				rows, err := tx.QueryContext(ctx,
					`SELECT `+storedColumns+` FROM flows
					 WHERE exporter = ? AND protocol = ? AND dst_addr = ? AND dst_port = ?
					   AND packets = ? AND bytes = ? AND input_if = ?
					   AND end_ms BETWEEN ? AND ?
					 ORDER BY id`,
					f.Exporter, f.Protocol, f.DstAddr, f.DstPort, int64(f.Packets), int64(f.Bytes), mateIf,
					f.EndMS-s.nat.windowMS, f.EndMS+s.nat.windowMS)
				if err != nil {
					return nil, fmt.Errorf("query nat candidates: %w", err)
				}
				if err := collectRows(rows, byID); err != nil {
					return nil, err
				}
				_ = wantWAN // the role is re-derived from each row's own source
			}
		}
	}
	out := make([]storedRow, 0, len(byID))
	for _, row := range byID {
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	return out, nil
}

func collectRows(rows *sql.Rows, into map[int64]storedRow) error {
	defer rows.Close()
	for rows.Next() {
		id, flow, err := scanStoredRow(rows)
		if err != nil {
			return fmt.Errorf("scan candidate row: %w", err)
		}
		into[id] = storedRow{id: id, flow: flow}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("candidate rows: %w", err)
	}
	return nil
}

// natPlan resolves the NAT role of f. It reports decided=false when NAT has
// nothing to say about this record, in which case parent/child gets to decide.
//
// The outcome must be independent of arrival order, so the plan is defined in
// terms of the canonical row (the pre-NAT copy on the configured pre interface)
// rather than in terms of which record arrived first:
//
//   - f is post-NAT (WAN source on the post interface): drop it when a pre-NAT
//     mate exists; otherwise store it, because its mate may still arrive.
//   - f is pre-NAT (non-WAN source on the pre interface): it is canonical. Every
//     validated post-NAT view is deleted, and if an equivalent pre-NAT copy is
//     already stored on another interface (the child), f becomes a promotion of
//     that row instead of a new row.
//   - anything else: not a NAT participant.
//
// Ambiguity (more than one candidate mate) means no pair is proven: nothing is
// deleted and the record is stored.
func (s *Store) natPlan(ctx context.Context, tx *sql.Tx, f *model.Flow, candidates []storedRow) (dedupPlan, bool, error) {
	opt, ok := s.nat.exporters[f.Exporter]
	if !ok {
		return dedupPlan{}, false, nil
	}
	wan := s.nat.isWANSource(f.SrcAddr)
	switch {
	case wan && f.InputIf == opt.postIf:
		if _, ok := s.pickPostNATMate(f, candidates, opt.preIf); !ok {
			return dedupPlan{insert: f}, true, nil
		}
		// The post-NAT view of a stored pre-NAT record: keep the one carrying the
		// real source identity. The stored row does not hold this record — its
		// source is the translated address — so no row id is reported: a reverse
		// lookup for the WAN address must not be written onto the pre-NAT row.
		return dedupPlan{}, true, nil
	case !wan && f.InputIf == opt.preIf:
		// f is the canonical pre-NAT view. Remove validated post-NAT views of the
		// same packet, but only when exactly one exists: two post-NAT candidates
		// mean the pair is ambiguous, and deleting either would be a guess.
		var redundant []storedRow
		for _, row := range candidates {
			if s.isValidatedPostNAT(f, &row.flow, opt.postIf) {
				redundant = append(redundant, row)
			}
		}
		if len(redundant) > 1 {
			return dedupPlan{insert: f}, true, nil
		}
		plan := dedupPlan{insert: f, delete: redundant}
		for _, row := range candidates {
			if row.flow.InputIf == f.InputIf || !sameFrame(&row.flow, f) {
				continue
			}
			switch s.pcRelation(f.Exporter, row.flow.InputIf, f.InputIf) {
			case relationParent:
				// An equivalent stored record on the parent interface already is
				// the canonical row: f is its child copy.
				plan.insert = nil
				plan.mergeTo = row.id
				return plan, true, nil
			case relationChild:
				plan.promote = append(plan.promote, promotion{id: row.id, stored: row.flow})
				plan.insert = nil
				plan.mergeTo = row.id
			}
		}
		return plan, true, nil
	}
	return dedupPlan{}, false, nil
}

// pickPostNATMate returns the single stored pre-NAT mate for a post-NAT record.
// More than one candidate is not a pair.
func (s *Store) pickPostNATMate(f *model.Flow, candidates []storedRow, preIf uint32) (storedRow, bool) {
	var mate storedRow
	found := 0
	for _, row := range candidates {
		if row.flow.InputIf != preIf || s.nat.isWANSource(row.flow.SrcAddr) || !s.natViewMatches(&row.flow, f) {
			continue
		}
		found++
		mate = row
	}
	if found != 1 {
		return storedRow{}, false
	}
	return mate, true
}

// natViewMatches is sameNATViews with the exporter check, which sameNATViews
// omits because the candidate query already scopes by exporter.
func (s *Store) natViewMatches(stored, incoming *model.Flow) bool {
	return stored.Exporter == incoming.Exporter && sameNATViews(stored, incoming, s.nat.windowMS)
}

// sameNATViews reports whether two records can be the two views of one NAT
// translation. The two views genuinely differ in the fields NAT itself rewrites:
//
//   - source address and source port (the translation),
//   - the source VLAN and its provenance, which the classifier derives from the
//     source address,
//   - start/end, because the two views are exported at different moments (the
//     pairing is bounded by the configured window instead),
//   - the input interface, because the views come from different nodes.
//
// Everything else must be equal, including the destination VLAN, so a pair whose
// destination classification or payload metadata differs is not paired.
func sameNATViews(a, b *model.Flow, windowMS int64) bool {
	if a.DstAddr != b.DstAddr || a.DstPort != b.DstPort || a.Protocol != b.Protocol {
		return false
	}
	if a.TCPFlags != b.TCPFlags || a.SrcTOS != b.SrcTOS || a.DstTOS != b.DstTOS {
		return false
	}
	if a.SrcAS != b.SrcAS || a.DstAS != b.DstAS {
		return false
	}
	if a.DstVLAN != b.DstVLAN || a.DstVLANSource != b.DstVLANSource {
		return false
	}
	if a.Bytes != b.Bytes || a.Packets != b.Packets {
		return false
	}
	if a.Sampled != b.Sampled || a.SamplingInterval != b.SamplingInterval ||
		a.SamplingAlgorithm != b.SamplingAlgorithm || a.SamplingSource != b.SamplingSource {
		return false
	}
	if a.Direction != b.Direction || a.DirectionKnown != b.DirectionKnown {
		return false
	}
	if a.OutputIf != b.OutputIf {
		return false
	}
	// The export timestamps are not compared field by field, but the pair must
	// still be inside the configured window: the candidate query applies it to
	// the stored row, and this bounds it from the other side.
	diff := a.EndMS - b.EndMS
	if diff < 0 {
		diff = -diff
	}
	return diff <= windowMS
}

// isValidatedPostNAT reports whether stored is the post-NAT view of canonical:
// on the configured post interface, WAN-sourced, and identical in every field
// NAT does not rewrite.
func (s *Store) isValidatedPostNAT(canonical, stored *model.Flow, postIf uint32) bool {
	return stored.InputIf == postIf &&
		s.nat.isWANSource(stored.SrcAddr) &&
		!s.nat.isWANSource(canonical.SrcAddr) &&
		sameNATViews(canonical, stored, s.nat.windowMS)
}

// parentChildPlan collapses a child-interface copy of a frame into the parent
// copy. It acts only when the exporter and the two input interfaces are a
// configured link and the records are the same observation in every column
// promotion does not rewrite (see sameFrame).
//
// Anything else — two different interfaces that are not a configured link, the
// same interface twice, an exporter without links, several possible parents, or
// two records that differ in any field the stored row would keep — is stored as
// its own row.
func (s *Store) parentChildPlan(ctx context.Context, tx *sql.Tx, f *model.Flow, candidates []storedRow) (dedupPlan, error) {
	if !s.pc.hasExporter(f.Exporter) {
		return dedupPlan{insert: f}, nil
	}
	var (
		parent     *storedRow
		child      *storedRow
		parents    int
		children   int
		ambiguous  bool
		hasSibling bool
	)
	for i := range candidates {
		row := &candidates[i]
		if !sameFrame(&row.flow, f) {
			continue
		}
		// A row already on the parent interface is the canonical row for this
		// frame whatever else the group contains; finding it is enough.
		if row.flow.InputIf == f.InputIf {
			hasSibling = true
		}
		switch s.pcRelation(f.Exporter, row.flow.InputIf, f.InputIf) {
		case relationParent:
			parents++
			parent = row
		case relationChild:
			children++
			child = row
		}
	}
	if parents > 1 || children > 1 {
		ambiguous = true
	}
	switch {
	case ambiguous || hasSibling:
		// Several possible parents, or a row already on this exact interface: no
		// single pair is provable, so store the record as its own row.
		return dedupPlan{insert: f}, nil
	case parent != nil:
		// The stored row is the parent and this is the child: the parent stays.
		return dedupPlan{mergeTo: parent.id}, nil
	case child != nil:
		// The stored row is the child: rewrite it to the parent's interfaces.
		return dedupPlan{
			promote: []promotion{{id: child.id, stored: child.flow}},
			mergeTo: child.id,
		}, nil
	}
	return dedupPlan{insert: f}, nil
}

// pcRelation is the parent/child relation between two interfaces under this
// configuration, or relationNone when parent/child suppression is off.
func (s *Store) pcRelation(exporter string, a, b uint32) relation {
	if s.pc == nil {
		return relationNone
	}
	return s.pc.relation(exporter, a, b)
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
