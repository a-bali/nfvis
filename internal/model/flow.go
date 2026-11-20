// Package model contains the normalized flow record shared by the decoder,
// storage layer, and API.
package model

// Flow is one normalized NetFlow record. Unknown values are represented by
// zero for numeric fields and an empty string for addresses, rather than by
// synthetic values.
type Flow struct {
	ReceivedAtMS int64  `json:"received_at_ms"`
	StartMS      int64  `json:"start_ms,omitempty"`
	EndMS        int64  `json:"end_ms,omitempty"`
	TimeSource   string `json:"time_source"` // exported | received

	Version    uint16 `json:"version"`
	Exporter   string `json:"exporter"`
	ExporterIP string `json:"exporter_ip"`
	ExporterID uint32 `json:"exporter_id,omitempty"`
	Sequence   uint32 `json:"sequence,omitempty"`
	EngineType uint8  `json:"engine_type,omitempty"`
	EngineID   uint8  `json:"engine_id,omitempty"`

	InputIf        uint32 `json:"input_if,omitempty"`
	InputLabel     string `json:"input_label,omitempty"`
	OutputIf       uint32 `json:"output_if,omitempty"`
	OutputLabel    string `json:"output_label,omitempty"`
	Direction      uint8  `json:"direction,omitempty"`
	DirectionKnown bool   `json:"direction_known,omitempty"`

	SrcAddr  string `json:"src_addr,omitempty"`
	DstAddr  string `json:"dst_addr,omitempty"`
	SrcPort  uint16 `json:"src_port,omitempty"`
	DstPort  uint16 `json:"dst_port,omitempty"`
	Protocol uint8  `json:"protocol,omitempty"`
	TCPFlags uint8  `json:"tcp_flags,omitempty"`
	SrcTOS   uint8  `json:"src_tos,omitempty"`
	DstTOS   uint8  `json:"dst_tos,omitempty"`
	Bytes    uint64 `json:"bytes"`
	Packets  uint64 `json:"packets"`
	SrcAS    uint32 `json:"src_as,omitempty"`
	DstAS    uint32 `json:"dst_as,omitempty"`

	SrcVLAN       int32  `json:"src_vlan"`
	DstVLAN       int32  `json:"dst_vlan"`
	SrcVLANSource string `json:"src_vlan_source"` // exported | rule | unknown
	DstVLANSource string `json:"dst_vlan_source"`

	Sampled           bool   `json:"sampled"`
	SamplingInterval  uint64 `json:"sampling_interval,omitempty"`
	SamplingAlgorithm uint32 `json:"sampling_algorithm,omitempty"`
	SamplingSource    string `json:"sampling_source,omitempty"` // header | options | record | unknown
}

// Direction constants used by the aggregate families.
const (
	DirectionIngress = "ingress"
	DirectionEgress  = "egress"
	DirectionSource  = "source"
	DirectionDest    = "destination"
	DirectionNone    = "none"
)
