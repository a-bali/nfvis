package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/a-bali/nfvis/internal/model"
)

const hourMS = int64(time.Hour / time.Millisecond)

// counters is a signed accumulator: rebuilding an hourly aggregate subtracts the
// rows it already counted before adding the replacement.
type counters struct {
	bytes   int64
	packets int64
	flows   int64
	sampled int64
	unknown int64
	label   string
}

func (c *counters) add(f model.Flow, unknown bool) { c.accumulate(f, unknown, 1) }

func (c *counters) sub(f model.Flow, unknown bool) { c.accumulate(f, unknown, -1) }

func (c *counters) accumulate(f model.Flow, unknown bool, sign int64) {
	c.bytes += sign * int64(f.Bytes)
	c.packets += sign * int64(f.Packets)
	c.flows += sign
	if f.Sampled {
		c.sampled += sign
	}
	if unknown {
		c.unknown += sign
	}
}

type exporterKey struct {
	hour     int64
	exporter string
}
type ifaceKey struct {
	hour      int64
	exporter  string
	ifIndex   uint32
	direction string
}
type hostKey struct {
	hour      int64
	host      string
	direction string
}
type vlanKey struct {
	hour      int64
	vlan      int32
	direction string
}
type protocolKey struct {
	hour     int64
	protocol uint8
}

type trafficKey struct {
	res    int64
	bucket int64
}

type batchAggregates struct {
	exporter map[exporterKey]*counters
	iface    map[ifaceKey]*counters
	host     map[hostKey]*counters
	vlan     map[vlanKey]*counters
	protocol map[protocolKey]*counters
	traffic  map[trafficKey]*counters
	tiers    []trafficTier
	// keys records which dedup identities this batch has already counted, so a
	// row deleted later in the same transaction can be subtracted from the
	// accumulator (and not confused with a row committed by an earlier batch).
	keys map[string]struct{}
}

func newBatchAggregates(n int, tiers []trafficTier) *batchAggregates {
	return &batchAggregates{
		exporter: make(map[exporterKey]*counters, n),
		iface:    make(map[ifaceKey]*counters, n),
		host:     make(map[hostKey]*counters, n),
		vlan:     make(map[vlanKey]*counters, n),
		protocol: make(map[protocolKey]*counters, n),
		traffic:  make(map[trafficKey]*counters, len(tiers)),
		tiers:    tiers,
		keys:     make(map[string]struct{}, n),
	}
}

// add counts a stored flow; sub removes one (used when a row is rebuilt as a
// different flow, e.g. a post-NAT record replaced by its pre-NAT mate).
func (a *batchAggregates) add(f model.Flow) { a.apply(f, 1) }

func (a *batchAggregates) sub(f model.Flow) { a.apply(f, -1) }

// register records that this batch counted the flow under key.
func (a *batchAggregates) register(key []byte) {
	if len(key) > 0 {
		a.keys[string(key)] = struct{}{}
	}
}

// counted reports whether this batch already counted the flow under key.
func (a *batchAggregates) counted(key []byte) bool {
	_, ok := a.keys[string(key)]
	return ok
}

func (a *batchAggregates) apply(f model.Flow, sign int64) {
	end := f.EndMS
	if end <= 0 {
		end = f.ReceivedAtMS
	}
	hour := (end / hourMS) * hourMS

	c := &counters{}
	c.accumulate(f, false, sign)
	ek := exporterKey{hour: hour, exporter: f.Exporter}
	if dst, ok := a.exporter[ek]; ok {
		dst.accumulate(f, false, sign)
	} else {
		a.exporter[ek] = c
	}

	addInterface := func(index uint32, label, direction string) {
		unknown := index == 0
		k := ifaceKey{hour: hour, exporter: f.Exporter, ifIndex: index, direction: direction}
		if dst, ok := a.iface[k]; ok {
			dst.accumulate(f, unknown, sign)
			if dst.label == "" && label != "" {
				dst.label = label
			}
		} else {
			cc := &counters{label: label}
			cc.accumulate(f, unknown, sign)
			a.iface[k] = cc
		}
	}
	addInterface(f.InputIf, f.InputLabel, model.DirectionIngress)
	addInterface(f.OutputIf, f.OutputLabel, model.DirectionEgress)

	addHost := func(host, direction string) {
		k := hostKey{hour: hour, host: host, direction: direction}
		if dst, ok := a.host[k]; ok {
			dst.accumulate(f, host == "", sign)
		} else {
			cc := &counters{}
			cc.accumulate(f, host == "", sign)
			a.host[k] = cc
		}
	}
	addHost(f.SrcAddr, model.DirectionSource)
	addHost(f.DstAddr, model.DirectionDest)

	addVLAN := func(vlan int32, direction string) {
		k := vlanKey{hour: hour, vlan: vlan, direction: direction}
		if dst, ok := a.vlan[k]; ok {
			dst.accumulate(f, vlan < 0, sign)
		} else {
			cc := &counters{}
			cc.accumulate(f, vlan < 0, sign)
			a.vlan[k] = cc
		}
	}
	addVLAN(f.SrcVLAN, model.DirectionSource)
	addVLAN(f.DstVLAN, model.DirectionDest)

	for _, tier := range a.tiers {
		k := trafficKey{res: tier.resMS, bucket: (end / tier.resMS) * tier.resMS}
		if dst, ok := a.traffic[k]; ok {
			dst.accumulate(f, false, sign)
		} else {
			cc := &counters{}
			cc.accumulate(f, false, sign)
			a.traffic[k] = cc
		}
	}

	pk := protocolKey{hour: hour, protocol: f.Protocol}
	if dst, ok := a.protocol[pk]; ok {
		dst.accumulate(f, false, sign)
	} else {
		cc := &counters{}
		cc.accumulate(f, false, sign)
		a.protocol[pk] = cc
	}
}

const insertFlowSQL = `INSERT INTO flows (
	received_ms, start_ms, end_ms, time_source, version, exporter, exporter_ip,
	exporter_id, sequence, engine_type, engine_id, input_if, input_label,
	output_if, output_label, direction, direction_known, src_addr, dst_addr,
	src_port, dst_port, protocol, tcp_flags, src_tos, dst_tos, bytes, packets,
	src_as, dst_as, src_vlan, dst_vlan, src_vlan_source, dst_vlan_source,
	sampled, sampling_interval, sampling_algorithm, sampling_source, dedup_key
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(dedup_key) WHERE dedup_key IS NOT NULL DO NOTHING
RETURNING id`

// updateFlowSQL rewrites the mutable identity columns of an existing row when a
// more specific duplicate arrives (see prefersInput). The stored row keeps its
// id, so the DNS lookups already queued against it stay valid.
const updateFlowSQL = `UPDATE flows SET input_if=?, input_label=?, output_if=?, output_label=? WHERE id=?`

// InsertBatch writes raw flows and all hourly aggregates in one transaction.
func (s *Store) InsertBatch(ctx context.Context, flows []model.Flow) error {
	_, err := s.InsertBatchIDs(ctx, flows)
	return err
}

// InsertBatchIDs atomically inserts a batch and returns the row IDs of the rows
// that still exist afterwards. Duplicates of a flow already stored — including
// one stored by an earlier batch — are dropped instead of inserted.
func (s *Store) InsertBatchIDs(ctx context.Context, flows []model.Flow) ([]int64, error) {
	if len(flows) == 0 {
		return nil, nil
	}
	ids := make([]int64, 0, len(flows))
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin insert: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.PrepareContext(ctx, insertFlowSQL)
	if err != nil {
		return nil, fmt.Errorf("prepare flow insert: %w", err)
	}
	update, err := tx.PrepareContext(ctx, updateFlowSQL)
	if err != nil {
		_ = stmt.Close()
		return nil, fmt.Errorf("prepare flow update: %w", err)
	}
	aggs := newBatchAggregates(len(flows), s.tiers)
	for i := range flows {
		f := &flows[i]
		// M2: a post-NAT copy whose pre-NAT mate is already stored is redundant.
		// Checked before the insert so the row is never written or counted.
		if s.nat != nil {
			drop, err := s.dropPostNATMate(ctx, tx, f)
			if err != nil {
				_ = stmt.Close()
				_ = update.Close()
				return nil, err
			}
			if drop {
				continue
			}
			// f may be the pre-NAT view of a post-NAT row stored earlier: remove
			// that row and subtract what it contributed.
			if err := s.replaceStoredPostNAT(ctx, tx, f, aggs); err != nil {
				_ = stmt.Close()
				_ = update.Close()
				return nil, err
			}
		}
		var key []byte
		if s.parentChildDedup {
			key = flowDedupKey(f)
		}
		var id int64
		err := stmt.QueryRowContext(ctx,
			f.ReceivedAtMS, f.StartMS, f.EndMS, f.TimeSource, f.Version,
			f.Exporter, f.ExporterIP, f.ExporterID, f.Sequence, f.EngineType, f.EngineID,
			f.InputIf, f.InputLabel, f.OutputIf, f.OutputLabel, f.Direction, boolToInt(f.DirectionKnown),
			f.SrcAddr, f.DstAddr, f.SrcPort, f.DstPort, f.Protocol, f.TCPFlags,
			f.SrcTOS, f.DstTOS, int64(f.Bytes), int64(f.Packets), f.SrcAS, f.DstAS,
			f.SrcVLAN, f.DstVLAN, f.SrcVLANSource, f.DstVLANSource,
			boolToInt(f.Sampled), int64(f.SamplingInterval), f.SamplingAlgorithm, f.SamplingSource,
			key,
		).Scan(&id)
		switch {
		case err == nil:
			ids = append(ids, id)
			aggs.add(*f)
			aggs.register(key)
		case errors.Is(err, sql.ErrNoRows):
			// A duplicate that lost the conflict may still be the better record:
			// the parent and VLAN-child copies of one frame differ only in their
			// interfaces, and the parent's must survive.
			id, err = s.resolveDuplicate(ctx, tx, update, f, key)
			if err != nil {
				_ = stmt.Close()
				_ = update.Close()
				return nil, err
			}
			if id == 0 {
				continue // the stored copy is preferred; nothing to aggregate
			}
			// The row was rewritten to this record. Its earlier contribution is
			// already in a previous batch's aggregates, so this record must not be
			// added again: doing so would count one frame twice.
			ids = append(ids, id)
		default:
			_ = stmt.Close()
			_ = update.Close()
			return nil, fmt.Errorf("insert flow: %w", err)
		}
	}
	if err := stmt.Close(); err != nil {
		return nil, fmt.Errorf("close flow insert: %w", err)
	}
	if err := update.Close(); err != nil {
		return nil, fmt.Errorf("close flow update: %w", err)
	}
	if err := upsertAggregates(ctx, tx, aggs); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit insert: %w", err)
	}
	s.invalidateStats()
	return ids, nil
}

// resolveDuplicate decides what to do with a record whose dedup key is already
// stored. It returns the id of the existing row when that row has been rewritten
// to this record, and 0 when the stored row is the preferred copy.
func (s *Store) resolveDuplicate(ctx context.Context, tx *sql.Tx, update *sql.Stmt, f *model.Flow, key []byte) (int64, error) {
	var (
		id          int64
		inputIf     uint32
		outputIf    uint32
		inputLabel  string
		outputLabel string
	)
	err := tx.QueryRowContext(ctx,
		`SELECT id, input_if, output_if, input_label, output_label FROM flows WHERE dedup_key = ?`, key,
	).Scan(&id, &inputIf, &outputIf, &inputLabel, &outputLabel)
	if errors.Is(err, sql.ErrNoRows) {
		// The conflicting row vanished between the insert and this read; there is
		// nothing to update, and re-inserting would race the same way.
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read duplicate flow: %w", err)
	}
	if !prefersInput(f.InputIf, inputIf) {
		return 0, nil
	}
	if _, err := update.ExecContext(ctx, f.InputIf, f.InputLabel, f.OutputIf, f.OutputLabel, id); err != nil {
		return 0, fmt.Errorf("promote duplicate flow: %w", err)
	}
	// The replaced copy was written by an earlier batch, whose aggregates already
	// count its interfaces and VLANs. Move that contribution to the interfaces
	// this record carries, or the dashboard keeps reporting the interface this
	// row no longer has.
	old := model.Flow{InputIf: inputIf, InputLabel: inputLabel, OutputIf: outputIf, OutputLabel: outputLabel}
	if err := moveInterfaceAggregate(ctx, tx, &old, f); err != nil {
		return 0, err
	}
	return id, nil
}

// moveInterfaceAggregate subtracts a replaced record's interface contribution
// from agg_interface and adds this record's. Only that table depends on
// input_if/output_if; every other aggregate is keyed by fields the duplicate
// shares.
func moveInterfaceAggregate(ctx context.Context, tx *sql.Tx, old, current *model.Flow) error {
	end := current.EndMS
	if end <= 0 {
		end = current.ReceivedAtMS
	}
	h := (end / hourMS) * hourMS
	type move struct {
		ifIndex   uint32
		label     string
		direction string
	}
	moves := []move{
		{old.InputIf, old.InputLabel, model.DirectionIngress},
		{old.OutputIf, old.OutputLabel, model.DirectionEgress},
		{current.InputIf, current.InputLabel, model.DirectionIngress},
		{current.OutputIf, current.OutputLabel, model.DirectionEgress},
	}
	// Net effect per key: -1 for the replaced interfaces, +1 for this record's.
	delta := make(map[move]int, len(moves))
	for i, m := range moves {
		if m.ifIndex == 0 {
			continue
		}
		if i < 2 {
			delta[m]--
		} else {
			delta[m]++
		}
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO agg_interface(hour_ms, exporter, if_index, direction, label, bytes, packets, flows, sampled_flows, unknown_flows)
		 VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(hour_ms, exporter, if_index, direction) DO UPDATE SET
		 label=CASE WHEN excluded.label <> '' THEN excluded.label ELSE agg_interface.label END,
		 bytes=agg_interface.bytes+excluded.bytes, packets=agg_interface.packets+excluded.packets,
		 flows=agg_interface.flows+excluded.flows, sampled_flows=agg_interface.sampled_flows+excluded.sampled_flows,
		 unknown_flows=agg_interface.unknown_flows+excluded.unknown_flows`)
	if err != nil {
		return fmt.Errorf("prepare interface aggregate move: %w", err)
	}
	defer stmt.Close()
	for m, d := range delta {
		if d == 0 {
			continue
		}
		var bytes, packets, flows, sampled int64
		if d > 0 {
			bytes, packets, flows = int64(current.Bytes), int64(current.Packets), 1
			if current.Sampled {
				sampled = 1
			}
		} else {
			bytes, packets, flows = -int64(current.Bytes), -int64(current.Packets), -1
			if current.Sampled {
				sampled = -1
			}
		}
		if _, err := stmt.ExecContext(ctx, h, current.Exporter, m.ifIndex, m.direction, m.label,
			bytes, packets, flows, sampled, 0); err != nil {
			return fmt.Errorf("move interface aggregate: %w", err)
		}
	}
	return nil
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func upsertAggregates(ctx context.Context, tx *sql.Tx, a *batchAggregates) error {
	statements := []struct {
		query string
		exec  func(*sql.Stmt) error
	}{
		{
			`INSERT INTO agg_exporter(hour_ms, exporter, bytes, packets, flows, sampled_flows, unknown_flows)
			 VALUES(?,?,?,?,?,?,?) ON CONFLICT(hour_ms, exporter) DO UPDATE SET
			 bytes=bytes+excluded.bytes, packets=packets+excluded.packets,
			 flows=flows+excluded.flows, sampled_flows=sampled_flows+excluded.sampled_flows,
			 unknown_flows=unknown_flows+excluded.unknown_flows`,
			func(stmt *sql.Stmt) error {
				for k, c := range a.exporter {
					if _, err := stmt.ExecContext(ctx, k.hour, k.exporter, c.bytes, c.packets, c.flows, c.sampled, c.unknown); err != nil {
						return err
					}
				}
				return nil
			},
		},
		{
			`INSERT INTO agg_interface(hour_ms, exporter, if_index, direction, label, bytes, packets, flows, sampled_flows, unknown_flows)
			 VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(hour_ms, exporter, if_index, direction) DO UPDATE SET
			 label=CASE WHEN excluded.label <> '' THEN excluded.label ELSE agg_interface.label END,
			 bytes=bytes+excluded.bytes, packets=packets+excluded.packets,
			 flows=flows+excluded.flows, sampled_flows=sampled_flows+excluded.sampled_flows,
			 unknown_flows=unknown_flows+excluded.unknown_flows`,
			func(stmt *sql.Stmt) error {
				for k, c := range a.iface {
					if _, err := stmt.ExecContext(ctx, k.hour, k.exporter, k.ifIndex, k.direction, c.label, c.bytes, c.packets, c.flows, c.sampled, c.unknown); err != nil {
						return err
					}
				}
				return nil
			},
		},
		{
			`INSERT INTO agg_host(hour_ms, host, direction, bytes, packets, flows, sampled_flows, unknown_flows)
			 VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(hour_ms, host, direction) DO UPDATE SET
			 bytes=bytes+excluded.bytes, packets=packets+excluded.packets,
			 flows=flows+excluded.flows, sampled_flows=sampled_flows+excluded.sampled_flows,
			 unknown_flows=unknown_flows+excluded.unknown_flows`,
			func(stmt *sql.Stmt) error {
				for k, c := range a.host {
					if _, err := stmt.ExecContext(ctx, k.hour, k.host, k.direction, c.bytes, c.packets, c.flows, c.sampled, c.unknown); err != nil {
						return err
					}
				}
				return nil
			},
		},
		{
			`INSERT INTO agg_vlan(hour_ms, vlan, direction, bytes, packets, flows, sampled_flows, unknown_flows)
			 VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(hour_ms, vlan, direction) DO UPDATE SET
			 bytes=bytes+excluded.bytes, packets=packets+excluded.packets,
			 flows=flows+excluded.flows, sampled_flows=sampled_flows+excluded.sampled_flows,
			 unknown_flows=unknown_flows+excluded.unknown_flows`,
			func(stmt *sql.Stmt) error {
				for k, c := range a.vlan {
					if _, err := stmt.ExecContext(ctx, k.hour, k.vlan, k.direction, c.bytes, c.packets, c.flows, c.sampled, c.unknown); err != nil {
						return err
					}
				}
				return nil
			},
		},
		{
			`INSERT INTO agg_protocol(hour_ms, protocol, bytes, packets, flows, sampled_flows, unknown_flows)
			 VALUES(?,?,?,?,?,?,?) ON CONFLICT(hour_ms, protocol) DO UPDATE SET
			 bytes=bytes+excluded.bytes, packets=packets+excluded.packets,
			 flows=flows+excluded.flows, sampled_flows=sampled_flows+excluded.sampled_flows,
			 unknown_flows=unknown_flows+excluded.unknown_flows`,
			func(stmt *sql.Stmt) error {
				for k, c := range a.protocol {
					if _, err := stmt.ExecContext(ctx, k.hour, k.protocol, c.bytes, c.packets, c.flows, c.sampled, c.unknown); err != nil {
						return err
					}
				}
				return nil
			},
		},
		{
			`INSERT INTO agg_traffic(res_ms, bucket_ms, bytes, packets, flows, sampled_flows, unknown_flows)
			 VALUES(?,?,?,?,?,?,?) ON CONFLICT(res_ms, bucket_ms) DO UPDATE SET
			 bytes=bytes+excluded.bytes, packets=packets+excluded.packets,
			 flows=flows+excluded.flows, sampled_flows=sampled_flows+excluded.sampled_flows,
			 unknown_flows=unknown_flows+excluded.unknown_flows`,
			func(stmt *sql.Stmt) error {
				for k, c := range a.traffic {
					if _, err := stmt.ExecContext(ctx, k.res, k.bucket, c.bytes, c.packets, c.flows, c.sampled, c.unknown); err != nil {
						return err
					}
				}
				return nil
			},
		},
	}
	for _, s := range statements {
		stmt, err := tx.PrepareContext(ctx, s.query)
		if err != nil {
			return fmt.Errorf("prepare aggregate upsert: %w", err)
		}
		if err := s.exec(stmt); err != nil {
			_ = stmt.Close()
			return fmt.Errorf("upsert aggregate: %w", err)
		}
		if err := stmt.Close(); err != nil {
			return fmt.Errorf("close aggregate upsert: %w", err)
		}
	}
	return nil
}
