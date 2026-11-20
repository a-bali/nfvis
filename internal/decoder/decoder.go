// Package decoder converts NetFlow datagrams into normalized flow records.
//
// Packet parsing is delegated to the maintained goflow2 decoders (BSD-3);
// this package owns template lifecycle, timestamp normalization, VLAN
// classification, and exporter labels behind a small internal interface.
package decoder

import (
	"net/netip"
	"time"

	"github.com/a-bali/nfvis/internal/config"
	"github.com/a-bali/nfvis/internal/model"
)

// Stats describes per-datagram decoder outcomes. Counters are accumulated by
// the ingestion pipeline and exposed through the health API.
type Stats struct {
	PacketsReceived     uint64
	PacketsDecoded      uint64
	PacketsMalformed    uint64
	PacketsUnsupported  uint64
	PacketsOversized    uint64
	FlowsDecoded        uint64
	FlowsSkipped        uint64
	UnknownTemplateSets uint64
	TemplateRefreshes   uint64
	TemplateExpirations uint64
	ExporterReboots     uint64
	TimestampWraps      uint64
	TimestampFallbacks  uint64
	SamplingOptions     uint64
	UnknownFields       uint64
}

// Add accumulates another Stats value.
func (s *Stats) Add(other Stats) {
	s.PacketsReceived += other.PacketsReceived
	s.PacketsDecoded += other.PacketsDecoded
	s.PacketsMalformed += other.PacketsMalformed
	s.PacketsUnsupported += other.PacketsUnsupported
	s.PacketsOversized += other.PacketsOversized
	s.FlowsDecoded += other.FlowsDecoded
	s.FlowsSkipped += other.FlowsSkipped
	s.UnknownTemplateSets += other.UnknownTemplateSets
	s.TemplateRefreshes += other.TemplateRefreshes
	s.TemplateExpirations += other.TemplateExpirations
	s.ExporterReboots += other.ExporterReboots
	s.TimestampWraps += other.TimestampWraps
	s.TimestampFallbacks += other.TimestampFallbacks
	s.SamplingOptions += other.SamplingOptions
	s.UnknownFields += other.UnknownFields
}

// Decoder is the internal boundary between UDP ingestion and NetFlow parsing.
type Decoder interface {
	Decode(exporter netip.Addr, payload []byte, receivedAt time.Time) ([]model.Flow, Stats, error)
	ExpireTemplates(now time.Time) int
	TemplateCount() int
}

type decoder struct {
	cfg        config.Config
	exporters  map[netip.Addr]config.Exporter
	classifier *Classifier
	v9         *v9Decoder
}

// New creates a decoder supporting NetFlow v5 and v9.
func New(cfg config.Config) Decoder {
	exporters := make(map[netip.Addr]config.Exporter, len(cfg.Exporters))
	for _, e := range cfg.Exporters {
		addr, err := netip.ParseAddr(e.Address)
		if err != nil {
			continue
		}
		exporters[addr.Unmap()] = e
	}
	d := &decoder{
		cfg:        cfg,
		exporters:  exporters,
		classifier: NewClassifier(cfg),
	}
	d.v9 = newV9Decoder(d)
	return d
}

// Decode reads the version and dispatches to the appropriate parser.
func (d *decoder) Decode(exporter netip.Addr, payload []byte, receivedAt time.Time) ([]model.Flow, Stats, error) {
	stats := Stats{PacketsReceived: 1}
	if len(payload) < 2 {
		stats.PacketsMalformed++
		return nil, stats, nil
	}
	if len(payload) > d.cfg.Decoder.MaxPacketBytes {
		stats.PacketsOversized++
		return nil, stats, nil
	}
	version := uint16(payload[0])<<8 | uint16(payload[1])
	exporter = exporter.Unmap()
	switch version {
	case 5:
		flows, local, err := decodeV5(d, exporter, payload, receivedAt)
		if err != nil {
			local.PacketsMalformed++
		} else {
			local.PacketsDecoded++
		}
		stats.Add(local)
		return flows, stats, err
	case 9:
		flows, local, err := d.v9.decode(exporter, payload, receivedAt)
		if err != nil {
			local.PacketsMalformed++
		} else {
			local.PacketsDecoded++
		}
		stats.Add(local)
		return flows, stats, err
	default:
		stats.PacketsUnsupported++
		return nil, stats, nil
	}
}

// ExpireTemplates removes stale v9 templates and returns the number removed.
func (d *decoder) ExpireTemplates(now time.Time) int {
	return d.v9.expire(now)
}

// TemplateCount returns the number of currently cached v9 templates.
func (d *decoder) TemplateCount() int { return d.v9.templateCount() }

func (d *decoder) labels(exporter netip.Addr) (string, map[uint32]string) {
	cfg, ok := d.exporters[exporter]
	if !ok {
		return "", nil
	}
	return cfg.Label, cfg.Interfaces
}

func interfaceLabel(labels map[uint32]string, index uint32) string {
	if index == 0 {
		return ""
	}
	if label, ok := labels[index]; ok {
		return label
	}
	return ""
}
