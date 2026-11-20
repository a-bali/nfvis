package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/a-bali/nfvis/internal/model"
)

const hourMS = int64(time.Hour / time.Millisecond)

type counters struct {
	bytes   uint64
	packets uint64
	flows   uint64
	sampled uint64
	unknown uint64
	label   string
}

func (c *counters) add(f model.Flow, unknown bool) {
	c.bytes += f.Bytes
	c.packets += f.Packets
	c.flows++
	if f.Sampled {
		c.sampled++
	}
	if unknown {
		c.unknown++
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

func (a *batchAggregates) add(f model.Flow) {
	end := f.EndMS
	if end <= 0 {
		end = f.ReceivedAtMS
	}
	hour := (end / hourMS) * hourMS

	c := &counters{}
	c.add(f, false)
	ek := exporterKey{hour: hour, exporter: f.Exporter}
	if dst, ok := a.exporter[ek]; ok {
		dst.add(f, false)
	} else {
		a.exporter[ek] = c
	}

	addInterface := func(index uint32, label, direction string) {
		unknown := index == 0
		k := ifaceKey{hour: hour, exporter: f.Exporter, ifIndex: index, direction: direction}
		if dst, ok := a.iface[k]; ok {
			dst.add(f, unknown)
			if dst.label == "" && label != "" {
				dst.label = label
			}
		} else {
			cc := &counters{label: label}
			cc.add(f, unknown)
			a.iface[k] = cc
		}
	}
	addInterface(f.InputIf, f.InputLabel, model.DirectionIngress)
	addInterface(f.OutputIf, f.OutputLabel, model.DirectionEgress)

	addHost := func(host, direction string) {
		k := hostKey{hour: hour, host: host, direction: direction}
		if dst, ok := a.host[k]; ok {
			dst.add(f, host == "")
		} else {
			cc := &counters{}
			cc.add(f, host == "")
			a.host[k] = cc
		}
	}
	addHost(f.SrcAddr, model.DirectionSource)
	addHost(f.DstAddr, model.DirectionDest)

	addVLAN := func(vlan int32, direction string) {
		k := vlanKey{hour: hour, vlan: vlan, direction: direction}
		if dst, ok := a.vlan[k]; ok {
			dst.add(f, vlan < 0)
		} else {
			cc := &counters{}
			cc.add(f, vlan < 0)
			a.vlan[k] = cc
		}
	}
	addVLAN(f.SrcVLAN, model.DirectionSource)
	addVLAN(f.DstVLAN, model.DirectionDest)

	for _, tier := range a.tiers {
		k := trafficKey{res: tier.resMS, bucket: (end / tier.resMS) * tier.resMS}
		if dst, ok := a.traffic[k]; ok {
			dst.add(f, false)
		} else {
			cc := &counters{}
			cc.add(f, false)
			a.traffic[k] = cc
		}
	}

	pk := protocolKey{hour: hour, protocol: f.Protocol}
	if dst, ok := a.protocol[pk]; ok {
		dst.add(f, false)
	} else {
		cc := &counters{}
		cc.add(f, false)
		a.protocol[pk] = cc
	}
}

const insertFlowSQL = `INSERT INTO flows (
	received_ms, start_ms, end_ms, time_source, version, exporter, exporter_ip,
	exporter_id, sequence, engine_type, engine_id, input_if, input_label,
	output_if, output_label, direction, direction_known, src_addr, dst_addr,
	src_port, dst_port, protocol, tcp_flags, src_tos, dst_tos, bytes, packets,
	src_as, dst_as, src_vlan, dst_vlan, src_vlan_source, dst_vlan_source,
	sampled, sampling_interval, sampling_algorithm, sampling_source
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`

// InsertBatch writes raw flows and all hourly aggregates in one transaction.
func (s *Store) InsertBatch(ctx context.Context, flows []model.Flow) error {
	_, err := s.InsertBatchIDs(ctx, flows)
	return err
}

// InsertBatchIDs atomically inserts a batch and returns its row IDs.
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
	aggs := newBatchAggregates(len(flows), s.tiers)
	for i := range flows {
		f := &flows[i]
		result, err := stmt.ExecContext(ctx,
			f.ReceivedAtMS, f.StartMS, f.EndMS, f.TimeSource, f.Version,
			f.Exporter, f.ExporterIP, f.ExporterID, f.Sequence, f.EngineType, f.EngineID,
			f.InputIf, f.InputLabel, f.OutputIf, f.OutputLabel, f.Direction, boolToInt(f.DirectionKnown),
			f.SrcAddr, f.DstAddr, f.SrcPort, f.DstPort, f.Protocol, f.TCPFlags,
			f.SrcTOS, f.DstTOS, int64(f.Bytes), int64(f.Packets), f.SrcAS, f.DstAS,
			f.SrcVLAN, f.DstVLAN, f.SrcVLANSource, f.DstVLANSource,
			boolToInt(f.Sampled), int64(f.SamplingInterval), f.SamplingAlgorithm, f.SamplingSource,
		)
		if err != nil {
			_ = stmt.Close()
			return nil, fmt.Errorf("insert flow: %w", err)
		}
		id, err := result.LastInsertId()
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
		aggs.add(*f)
	}
	if err := stmt.Close(); err != nil {
		return nil, fmt.Errorf("close flow insert: %w", err)
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
