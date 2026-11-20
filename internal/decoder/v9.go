package decoder

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"
	"sync"
	"time"

	"github.com/balia/nfvis/internal/model"
	"github.com/netsampler/goflow2/v3/decoders/netflow"
)

const (
	v9FieldInBytes       = 1
	v9FieldInPackets     = 2
	v9FieldProtocol      = 4
	v9FieldTCPFlags      = 6
	v9FieldSrcPort       = 7
	v9FieldIPv4Src       = 8
	v9FieldInputSNMP     = 10
	v9FieldDstPort       = 11
	v9FieldIPv4Dst       = 12
	v9FieldOutputSNMP    = 14
	v9FieldSrcAS         = 16
	v9FieldDstAS         = 17
	v9FieldFirstSwitched = 22
	v9FieldLastSwitched  = 21
	v9FieldOutBytes      = 23
	v9FieldOutPackets    = 24
	v9FieldIPv6Src       = 27
	v9FieldIPv6Dst       = 28
	v9FieldSamplingInt   = 34
	v9FieldSamplingAlg   = 35
	v9FieldSamplerID     = 48
	v9FieldSamplerMode   = 49
	v9FieldRandomInt     = 50
	v9FieldSrcVLAN       = 58
	v9FieldDstVLAN       = 59
	v9FieldDirection     = 61
	v9FieldSrcTOS        = 5
	v9FieldDstTOS        = 55
)

type samplingInfo struct {
	interval  uint64
	algorithm uint32
	known     bool
}

type v9ExporterState struct {
	lastAnchor int64
	hasAnchor  bool
	sampling   samplingInfo
}

type v9Decoder struct {
	parent *decoder
	store  *templateStore
	mu     sync.Mutex
	states map[string]*v9ExporterState
}

func newV9Decoder(parent *decoder) *v9Decoder {
	return &v9Decoder{
		parent: parent,
		store:  newTemplateStore(parent.cfg.Decoder.TemplateTTL.Duration()),
		states: make(map[string]*v9ExporterState),
	}
}

func (v *v9Decoder) expire(now time.Time) int { return v.store.expire(now) }
func (v *v9Decoder) templateCount() int       { return v.store.count() }

func routerKey(exporter netip.Addr, sourceID uint32) string {
	return fmt.Sprintf("%s|%d", exporter.String(), sourceID)
}

func (v *v9Decoder) decode(exporter netip.Addr, payload []byte, receivedAt time.Time) ([]model.Flow, Stats, error) {
	var stats Stats
	if len(payload) < 20 {
		return nil, stats, fmt.Errorf("v9 header is truncated")
	}
	_ = binary.BigEndian.Uint16(payload[2:4]) // flowset count; packet length bounds loops
	uptime := binary.BigEndian.Uint32(payload[4:8])
	unixSecs := binary.BigEndian.Uint32(payload[8:12])
	sequence := binary.BigEndian.Uint32(payload[12:16])
	sourceID := binary.BigEndian.Uint32(payload[16:20])
	key := routerKey(exporter, sourceID)
	anchor := makeUptimeAnchor(unixSecs, uptime)
	bootTime := int64(unixSecs)*1000 - int64(uptime)

	v.mu.Lock()
	state, ok := v.states[key]
	if !ok {
		state = &v9ExporterState{}
		v.states[key] = state
	}
	reboot := false
	if state.hasAnchor {
		delta := bootTime - state.lastAnchor
		// An uptime counter wrap shifts the apparent boot time by 2^32 ms.
		if delta > two32/2 {
			delta -= two32
		} else if delta < -two32/2 {
			delta += two32
		}
		if delta < 0 {
			delta = -delta
		}
		// Sequence numbers can regress due to reordering, wrap, or exporters
		// using independent counters for streams sharing a source ID. A
		// sequence drop alone is not proof of reboot and must not invalidate
		// templates needed by subsequent data packets.
		if delta > 5*60*1000 {
			reboot = true
		}
	}
	if reboot {
		removed := v.store.clearRouter(key)
		stats.TemplateExpirations += uint64(removed)
		stats.ExporterReboots++
		state.sampling = samplingInfo{}
	}
	state.lastAnchor = bootTime
	state.hasAnchor = true
	stateCopy := *state
	v.mu.Unlock()

	ctx := netflow.FlowContext{RouterKey: key}
	packet := netflow.NFv9Packet{}
	// goflow2's v9 decoder expects the version field to have been consumed.
	err := netflow.DecodeMessageNetFlow(bytes.NewBuffer(payload[2:]), v.store, ctx, &packet)
	missingTemplate := errors.Is(err, netflow.ErrorTemplateNotFound)
	if err != nil && !missingTemplate {
		return nil, stats, err
	}
	if missingTemplate {
		// A joined error can contain other causes; only tolerate template gaps.
		var flowErr *netflow.FlowError
		if errors.As(err, &flowErr) && !errors.Is(flowErr.Err, netflow.ErrorTemplateNotFound) {
			return nil, stats, err
		}
		stats.UnknownTemplateSets++
	}
	receivedMS := receivedAt.UTC().UnixMilli()
	exporterStr := exporter.String()
	_, ifLabels := v.parent.labels(exporter)
	flows := make([]model.Flow, 0)

	for _, set := range packet.FlowSets {
		switch typed := set.(type) {
		case netflow.RawFlowSet:
			stats.UnknownTemplateSets++
		case netflow.OptionsDataFlowSet:
			parsed, found := v.parseSamplingOptions(ctx, sourceID, typed)
			if found {
				stats.SamplingOptions++
				v.mu.Lock()
				state.sampling = parsed
				v.mu.Unlock()
				stateCopy.sampling = parsed
			}
		case netflow.DataFlowSet:
			for _, rec := range typed.Records {
				fields, unknown := fieldsFromRecord(rec)
				stats.UnknownFields += unknown
				flow, local, ok := v.flowFromFields(fields, anchor, receivedMS, exporterStr, sourceID, sequence, stateCopy.sampling, ifLabels)
				if !ok {
					stats.FlowsSkipped++
					continue
				}
				stats.Add(local)
				flows = append(flows, flow)
			}
		}
	}
	stats.FlowsDecoded += uint64(len(flows))
	return flows, stats, nil
}

func (v *v9Decoder) parseSamplingOptions(ctx netflow.FlowContext, sourceID uint32, set netflow.OptionsDataFlowSet) (samplingInfo, bool) {
	var result samplingInfo
	found := false
	for _, rec := range set.Records {
		fields := make(map[uint16][]byte)
		for _, f := range rec.OptionsValues {
			if raw, ok := f.Value.([]byte); ok {
				fields[f.Type] = raw
			}
		}
		interval, ok1 := uintFromFields(fields, v9FieldSamplingInt, v9FieldRandomInt)
		algorithm, ok2 := uintFromFields(fields, v9FieldSamplingAlg, v9FieldSamplerMode)
		if !ok1 && !ok2 {
			continue
		}
		result = samplingInfo{interval: interval, algorithm: uint32(algorithm), known: true}
		found = true
	}
	_ = ctx
	_ = sourceID
	return result, found
}

func fieldsFromRecord(rec netflow.DataRecord) (map[uint16][]byte, uint64) {
	fields := make(map[uint16][]byte, len(rec.Values))
	var unknown uint64
	for _, f := range rec.Values {
		raw, ok := f.Value.([]byte)
		if !ok {
			unknown++
			continue
		}
		if _, exists := fields[f.Type]; exists {
			unknown++ // duplicate field in one template; keep the first
			continue
		}
		fields[f.Type] = raw
	}
	return fields, unknown
}

func (v *v9Decoder) flowFromFields(fields map[uint16][]byte, anchor uptimeAnchor, receivedMS int64, exporter string, sourceID, sequence uint32, sampling samplingInfo, ifLabels map[uint32]string) (model.Flow, Stats, bool) {
	var stats Stats
	startUptime, hasStart := uintFromFields(fields, v9FieldFirstSwitched)
	endUptime, hasEnd := uintFromFields(fields, v9FieldLastSwitched)
	startMS, endMS := receivedMS, receivedMS
	timeSource := "received"
	if hasStart && hasEnd {
		startMS = switchedTime(anchor, uint32(startUptime))
		endMS = switchedTime(anchor, uint32(endUptime))
		var wrapped bool
		startMS, endMS, timeSource, wrapped = normalizeWindow(startMS, endMS, receivedMS)
		if wrapped {
			stats.TimestampWraps++
		}
	} else if hasEnd {
		endMS = switchedTime(anchor, uint32(endUptime))
		if endMS < 946684800000 || endMS > receivedMS+24*time.Hour.Milliseconds() {
			endMS = receivedMS
		} else {
			timeSource = "exported"
		}
		startMS = endMS
	}
	if timeSource == "received" {
		stats.TimestampFallbacks++
	}

	src := ipFromFields(fields, v9FieldIPv4Src, v9FieldIPv6Src)
	dst := ipFromFields(fields, v9FieldIPv4Dst, v9FieldIPv6Dst)
	srcVLANVal, _ := uintFromFields(fields, v9FieldSrcVLAN)
	dstVLANVal, _ := uintFromFields(fields, v9FieldDstVLAN)
	srcVLAN, srcVLANSource, dstVLAN, dstVLANSource := v.parent.classifier.Classify(src, dst, uint32(srcVLANVal), uint32(dstVLANVal))

	bytesValue, _ := uintFromFields(fields, v9FieldInBytes, v9FieldOutBytes)
	packetsValue, _ := uintFromFields(fields, v9FieldInPackets, v9FieldOutPackets)
	inputIf, _ := uintFromFields(fields, v9FieldInputSNMP)
	outputIf, _ := uintFromFields(fields, v9FieldOutputSNMP)
	srcPort, _ := uintFromFields(fields, v9FieldSrcPort)
	dstPort, _ := uintFromFields(fields, v9FieldDstPort)
	protocol, _ := uintFromFields(fields, v9FieldProtocol)
	flags, _ := uintFromFields(fields, v9FieldTCPFlags)
	srcTOS, _ := uintFromFields(fields, v9FieldSrcTOS)
	dstTOS, _ := uintFromFields(fields, v9FieldDstTOS)
	srcAS, _ := uintFromFields(fields, v9FieldSrcAS)
	dstAS, _ := uintFromFields(fields, v9FieldDstAS)
	direction, directionKnown := uintFromFields(fields, v9FieldDirection)

	sampledFlag := false
	samplingInterval := sampling.interval
	samplingAlgorithm := sampling.algorithm
	samplingSource := "unknown"
	if sampling.known && (sampling.interval > 1 || sampling.algorithm > 0) {
		sampledFlag = true
		samplingSource = "options"
	}
	if recordInterval, ok := uintFromFields(fields, v9FieldSamplingInt, v9FieldRandomInt); ok {
		samplingInterval = recordInterval
		sampledFlag = recordInterval > 1
		samplingSource = "record"
	}
	if recordAlg, ok := uintFromFields(fields, v9FieldSamplingAlg, v9FieldSamplerMode); ok {
		samplingAlgorithm = uint32(recordAlg)
		if recordAlg > 0 {
			sampledFlag = true
		}
		if samplingSource == "unknown" {
			samplingSource = "record"
		}
	}

	flow := model.Flow{
		ReceivedAtMS:      receivedMS,
		StartMS:           startMS,
		EndMS:             endMS,
		TimeSource:        timeSource,
		Version:           9,
		Exporter:          exporter,
		ExporterIP:        exporter,
		ExporterID:        sourceID,
		Sequence:          sequence,
		InputIf:           uint32(inputIf),
		InputLabel:        interfaceLabel(ifLabels, uint32(inputIf)),
		OutputIf:          uint32(outputIf),
		OutputLabel:       interfaceLabel(ifLabels, uint32(outputIf)),
		Direction:         uint8(direction),
		DirectionKnown:    directionKnown,
		SrcAddr:           src,
		DstAddr:           dst,
		SrcPort:           uint16(srcPort),
		DstPort:           uint16(dstPort),
		Protocol:          uint8(protocol),
		TCPFlags:          uint8(flags),
		SrcTOS:            uint8(srcTOS),
		DstTOS:            uint8(dstTOS),
		Bytes:             bytesValue,
		Packets:           packetsValue,
		SrcAS:             uint32(srcAS),
		DstAS:             uint32(dstAS),
		SrcVLAN:           srcVLAN,
		SrcVLANSource:     srcVLANSource,
		DstVLAN:           dstVLAN,
		DstVLANSource:     dstVLANSource,
		Sampled:           sampledFlag,
		SamplingInterval:  samplingInterval,
		SamplingAlgorithm: samplingAlgorithm,
		SamplingSource:    samplingSource,
	}
	return flow, stats, true
}

func uintFromFields(fields map[uint16][]byte, types ...uint16) (uint64, bool) {
	for _, t := range types {
		raw, ok := fields[t]
		if !ok {
			continue
		}
		if v, ok := rawToUint(raw); ok {
			return v, true
		}
	}
	return 0, false
}

func rawToUint(raw []byte) (uint64, bool) {
	switch len(raw) {
	case 1:
		return uint64(raw[0]), true
	case 2:
		return uint64(binary.BigEndian.Uint16(raw)), true
	case 3:
		return uint64(raw[0])<<16 | uint64(raw[1])<<8 | uint64(raw[2]), true
	case 4:
		return uint64(binary.BigEndian.Uint32(raw)), true
	case 8:
		return binary.BigEndian.Uint64(raw), true
	default:
		return 0, false
	}
}

func ipFromFields(fields map[uint16][]byte, types ...uint16) string {
	for _, t := range types {
		raw, ok := fields[t]
		if !ok {
			continue
		}
		switch len(raw) {
		case 4:
			return netip.AddrFrom4([4]byte{raw[0], raw[1], raw[2], raw[3]}).String()
		case 16:
			return netip.AddrFrom16([16]byte(raw)).Unmap().String()
		}
	}
	return ""
}
