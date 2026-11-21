package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/a-bali/nfvis/internal/model"
)

const hourMS = int64(time.Hour / time.Millisecond)

// counters is a signed accumulator. Every aggregate write is a delta applied with
// `column = column + excluded.column`, so a removed or rewritten row is
// subtracted from the same buckets it was added to. Nothing is ever re-derived
// from the raw table: aggregate history outlives raw retention, so a re-derivation
// would silently delete totals for hours whose rows have been pruned.
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

// zero reports whether the accumulator carries no contribution at all, in which
// case the upsert can skip the row entirely.
func (c *counters) zero() bool {
	return c.bytes == 0 && c.packets == 0 && c.flows == 0 && c.sampled == 0 && c.unknown == 0
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
	}
}

// add counts a stored flow.
func (a *batchAggregates) add(f model.Flow) { a.apply(f, 1) }

// sub removes a flow the transaction deleted or rewrote away. It subtracts from
// every family, so a deletion cannot leave a stale total in any of them, and the
// arithmetic is exact even when the affected hour's raw rows are partly pruned.
func (a *batchAggregates) sub(f model.Flow) { a.apply(f, -1) }

// replace applies the net effect of rewriting one stored row into another: the
// non-interface fields are equal by construction, so their buckets cancel and
// only the interface buckets move.
func (a *batchAggregates) replace(old, current model.Flow) {
	a.sub(old)
	a.add(current)
}

func (a *batchAggregates) apply(f model.Flow, sign int64) {
	end := f.EndMS
	if end <= 0 {
		end = f.ReceivedAtMS
	}
	hour := (end / hourMS) * hourMS

	bucket(a.exporter, exporterKey{hour: hour, exporter: f.Exporter}).accumulate(f, false, sign)

	addInterface := func(index uint32, label, direction string) {
		unknown := index == 0
		cc := bucket(a.iface, ifaceKey{hour: hour, exporter: f.Exporter, ifIndex: index, direction: direction})
		if sign > 0 && cc.label == "" && label != "" {
			cc.label = label
		}
		cc.accumulate(f, unknown, sign)
	}
	addInterface(f.InputIf, f.InputLabel, model.DirectionIngress)
	addInterface(f.OutputIf, f.OutputLabel, model.DirectionEgress)

	addHost := func(host, direction string) {
		bucket(a.host, hostKey{hour: hour, host: host, direction: direction}).accumulate(f, host == "", sign)
	}
	addHost(f.SrcAddr, model.DirectionSource)
	addHost(f.DstAddr, model.DirectionDest)

	addVLAN := func(vlan int32, direction string) {
		bucket(a.vlan, vlanKey{hour: hour, vlan: vlan, direction: direction}).accumulate(f, vlan < 0, sign)
	}
	addVLAN(f.SrcVLAN, model.DirectionSource)
	addVLAN(f.DstVLAN, model.DirectionDest)

	for _, tier := range a.tiers {
		bucket(a.traffic, trafficKey{res: tier.resMS, bucket: (end / tier.resMS) * tier.resMS}).accumulate(f, false, sign)
	}

	bucket(a.protocol, protocolKey{hour: hour, protocol: f.Protocol}).accumulate(f, false, sign)
}

// bucket returns the accumulator for k, creating it on first use.
func bucket[K comparable](m map[K]*counters, k K) *counters {
	if dst, ok := m[k]; ok {
		return dst
	}
	cc := &counters{}
	m[k] = cc
	return cc
}

const insertFlowSQL = `INSERT INTO flows (
	received_ms, start_ms, end_ms, time_source, version, exporter, exporter_ip,
	exporter_id, sequence, engine_type, engine_id, input_if, input_label,
	output_if, output_label, direction, direction_known, src_addr, dst_addr,
	src_port, dst_port, protocol, tcp_flags, src_tos, dst_tos, bytes, packets,
	src_as, dst_as, src_vlan, dst_vlan, src_vlan_source, dst_vlan_source,
	sampled, sampling_interval, sampling_algorithm, sampling_source, dedup_key
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
RETURNING id`

// updateFlowSQL rewrites the interface columns of an existing row when the
// parent copy of a duplicated frame arrives after its child. Only the interfaces
// differ between the copies, so nothing else is touched and the row keeps its id:
// DNS lookups already queued against it stay valid.
const updateFlowSQL = `UPDATE flows SET input_if=?, input_label=?, output_if=?, output_label=? WHERE id=?`

// InsertResult reports what a batch did with each record.
type InsertResult struct {
	// IDs is parallel to the input records. IDs[i] is the row that now holds
	// record i: the new row's id, or the id of the existing row a duplicate was
	// merged into. It is 0 when the record produced no row and no row holds the
	// same endpoints (a dropped post-NAT view, whose addresses differ from the
	// surviving pre-NAT row's).
	IDs []int64
	// NewRows counts rows created by this batch.
	NewRows int
	// Merged counts records that did not create a row because an equivalent row
	// already existed. NewRows+Merged == len(flows).
	Merged int
}

// InsertBatch writes raw flows and all hourly aggregates in one transaction.
func (s *Store) InsertBatch(ctx context.Context, flows []model.Flow) error {
	_, err := s.InsertBatchResult(ctx, flows)
	return err
}

// InsertBatchIDs is InsertBatch returning the positional row ids. A zero entry
// means the record at that position was collapsed into another row.
func (s *Store) InsertBatchIDs(ctx context.Context, flows []model.Flow) ([]int64, error) {
	res, err := s.InsertBatchResult(ctx, flows)
	if err != nil {
		return nil, err
	}
	return res.IDs, nil
}

// InsertBatchResult atomically inserts a batch and reports per-record row ids.
//
// Duplicate handling is delegated to dedupAction, which only collapses records a
// configured topology authorizes: an identical payload on another interface is
// not by itself a duplicate. Aggregates are maintained as signed deltas in the
// same transaction, so a dropped, promoted or replaced row is subtracted from
// exactly the buckets it was added to.
func (s *Store) InsertBatchResult(ctx context.Context, flows []model.Flow) (InsertResult, error) {
	res := InsertResult{IDs: make([]int64, len(flows))}
	if len(flows) == 0 {
		return res, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return res, fmt.Errorf("begin insert: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.PrepareContext(ctx, insertFlowSQL)
	if err != nil {
		return res, fmt.Errorf("prepare flow insert: %w", err)
	}
	defer stmt.Close()
	update, err := tx.PrepareContext(ctx, updateFlowSQL)
	if err != nil {
		return res, fmt.Errorf("prepare flow update: %w", err)
	}
	defer update.Close()
	deleteStmt, err := tx.PrepareContext(ctx, `DELETE FROM flows WHERE id = ?`)
	if err != nil {
		return res, fmt.Errorf("prepare flow delete: %w", err)
	}
	defer deleteStmt.Close()

	aggs := newBatchAggregates(len(flows), s.tiers)
	for i := range flows {
		f := &flows[i]
		action, err := s.dedupAction(ctx, tx, f)
		if err != nil {
			return res, err
		}
		switch action.kind {
		case actionDrop:
			// The stored row already holds this record. Attach the lookup only when
			// the row really is this record — the parent/child case, where the
			// addresses are identical by construction. A dropped post-NAT view has a
			// different source address, and a lookup for it must not be written onto
			// the pre-NAT row.
			if action.stored.SrcAddr == f.SrcAddr && action.stored.DstAddr == f.DstAddr {
				res.IDs[i] = action.id
			}
			res.Merged++
			continue
		case actionPromote:
			if _, err := update.ExecContext(ctx, f.InputIf, f.InputLabel, f.OutputIf, f.OutputLabel, action.id); err != nil {
				return res, fmt.Errorf("promote duplicate flow: %w", err)
			}
			aggs.replace(action.stored, *f)
			res.IDs[i] = action.id
			res.Merged++
			continue
		case actionReplace:
			// The stored row was the redundant view; it goes and this record
			// takes its place as a new row.
			if _, err := deleteStmt.ExecContext(ctx, action.id); err != nil {
				return res, fmt.Errorf("delete superseded flow: %w", err)
			}
			aggs.sub(action.stored)
		}
		key := []byte(nil)
		if s.dedupEnabled() {
			key = flowDedupKey(f)
		}
		id, err := insertFlow(ctx, stmt, f, key)
		if err != nil {
			return res, err
		}
		aggs.add(*f)
		res.IDs[i] = id
		res.NewRows++
	}
	if err := upsertAggregates(ctx, tx, aggs); err != nil {
		return res, err
	}
	if err := tx.Commit(); err != nil {
		return res, fmt.Errorf("commit insert: %w", err)
	}
	s.invalidateStats()
	return res, nil
}

func insertFlow(ctx context.Context, stmt *sql.Stmt, f *model.Flow, key []byte) (int64, error) {
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
	if err != nil {
		return 0, fmt.Errorf("insert flow: %w", err)
	}
	return id, nil
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
					if c.zero() {
						continue
					}
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
					if c.zero() {
						continue
					}
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
					if c.zero() {
						continue
					}
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
					if c.zero() {
						continue
					}
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
					if c.zero() {
						continue
					}
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
					if c.zero() {
						continue
					}
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
