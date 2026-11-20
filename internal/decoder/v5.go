package decoder

import (
	"bytes"
	"encoding/binary"
	"net/netip"
	"time"

	"github.com/a-bali/nfvis/internal/model"
	"github.com/netsampler/goflow2/v3/decoders/netflowlegacy"
)

func decodeV5(d *decoder, exporter netip.Addr, payload []byte, receivedAt time.Time) ([]model.Flow, Stats, error) {
	var stats Stats
	var packet netflowlegacy.PacketNetFlowV5
	if err := netflowlegacy.DecodeMessageVersion(bytes.NewBuffer(payload), &packet); err != nil {
		return nil, stats, err
	}
	if int(packet.Count) > d.cfg.Decoder.MaxFlowsPerPacket {
		return nil, stats, nil
	}

	receivedMS := receivedAt.UTC().UnixMilli()
	anchor := makeUptimeAnchor(packet.UnixSecs, packet.SysUptime)
	label, ifLabels := d.labels(exporter)
	exporterStr := exporter.String()

	flows := make([]model.Flow, 0, len(packet.Records))
	for _, rec := range packet.Records {
		start := switchedTime(anchor, rec.First)
		end := switchedTime(anchor, rec.Last)
		start, end, source, wrapped := normalizeWindow(start, end, receivedMS)
		if wrapped {
			stats.TimestampWraps++
		}
		if source == "received" {
			stats.TimestampFallbacks++
		}

		var srcBuf, dstBuf [4]byte
		binary.BigEndian.PutUint32(srcBuf[:], uint32(rec.SrcAddr))
		binary.BigEndian.PutUint32(dstBuf[:], uint32(rec.DstAddr))
		src := netip.AddrFrom4(srcBuf).String()
		dst := netip.AddrFrom4(dstBuf).String()
		srcVLAN, srcVLANSource, dstVLAN, dstVLANSource := d.classifier.Classify(src, dst, 0, 0)

		sampled := packet.SamplingInterval > 1
		samplingSource := "unknown"
		if sampled {
			samplingSource = "header"
		}
		flows = append(flows, model.Flow{
			ReceivedAtMS:     receivedMS,
			StartMS:          start,
			EndMS:            end,
			TimeSource:       source,
			Version:          5,
			Exporter:         exporterStr,
			ExporterIP:       exporterStr,
			ExporterID:       uint32(packet.EngineType)<<8 | uint32(packet.EngineId),
			Sequence:         packet.FlowSequence,
			EngineType:       packet.EngineType,
			EngineID:         packet.EngineId,
			InputIf:          uint32(rec.Input),
			InputLabel:       interfaceLabel(ifLabels, uint32(rec.Input)),
			OutputIf:         uint32(rec.Output),
			OutputLabel:      interfaceLabel(ifLabels, uint32(rec.Output)),
			SrcAddr:          src,
			DstAddr:          dst,
			SrcPort:          rec.SrcPort,
			DstPort:          rec.DstPort,
			Protocol:         rec.Proto,
			TCPFlags:         rec.TCPFlags,
			SrcTOS:           rec.Tos,
			Bytes:            uint64(rec.DOctets),
			Packets:          uint64(rec.DPkts),
			SrcAS:            uint32(rec.SrcAS),
			DstAS:            uint32(rec.DstAS),
			SrcVLAN:          srcVLAN,
			SrcVLANSource:    srcVLANSource,
			DstVLAN:          dstVLAN,
			DstVLANSource:    dstVLANSource,
			Sampled:          sampled,
			SamplingInterval: uint64(packet.SamplingInterval),
			SamplingSource:   samplingSource,
		})
		_ = label // Exporter labels are resolved by the API from configuration.
	}
	stats.FlowsDecoded = uint64(len(flows))
	return flows, stats, nil
}
