// Package testutil builds synthetic NetFlow datagrams for tests and benchmarks.
package testutil

import (
	"encoding/binary"
	"time"
)

// V5Field describes one v5 record value.
type V5Field struct {
	SrcAddr, DstAddr [4]byte
	Input, Output    uint16
	Packets, Bytes   uint32
	First, Last      uint32
	SrcPort, DstPort uint16
	Flags, Proto     uint8
}

func ip4(a, b, c, d byte) [4]byte { return [4]byte{a, b, c, d} }

// DefaultV4Src returns a stable test source address.
func DefaultV4Src() [4]byte { return ip4(10, 1, 2, 3) }

// DefaultV4Dst returns a stable test destination address.
func DefaultV4Dst() [4]byte { return ip4(203, 0, 113, 5) }

// V5Packet builds a v5 packet with the supplied records.
func V5Packet(now time.Time, uptime uint32, sampling uint16, records ...V5Field) []byte {
	if len(records) == 0 {
		records = []V5Field{{}}
	}
	buf := make([]byte, 24+48*len(records))
	put16(buf[0:], 5)
	put16(buf[2:], uint16(len(records)))
	put32(buf[4:], uptime)
	put32(buf[8:], uint32(now.Unix()))
	put32(buf[12:], 0)
	put32(buf[16:], 7)
	buf[20], buf[21] = 1, 0
	put16(buf[22:], sampling)
	for i, r := range records {
		o := 24 + i*48
		copy(buf[o:o+4], r.SrcAddr[:])
		copy(buf[o+4:o+8], r.DstAddr[:])
		nextHop := ip4(10, 0, 0, 1)
		copy(buf[o+8:o+12], nextHop[:])
		put16(buf[o+12:], r.Input)
		put16(buf[o+14:], r.Output)
		put32(buf[o+16:], r.Packets)
		put32(buf[o+20:], r.Bytes)
		put32(buf[o+24:], r.First)
		put32(buf[o+28:], r.Last)
		put16(buf[o+32:], r.SrcPort)
		put16(buf[o+34:], r.DstPort)
		buf[o+37] = r.Flags
		buf[o+38] = r.Proto
		put16(buf[o+40:], 65001)
		put16(buf[o+42:], 65002)
	}
	return buf
}

// Field is one NetFlow v9 information element.
type Field struct {
	Type  uint16
	Value []byte
}

// Template returns a v9 template record with fields sized from Value.
func Template(id uint16, fields ...Field) []byte {
	buf := make([]byte, 4+4*len(fields))
	put16(buf[0:], id)
	put16(buf[2:], uint16(len(fields)))
	for i, f := range fields {
		put16(buf[4+i*4:], f.Type)
		put16(buf[6+i*4:], uint16(len(f.Value)))
	}
	return buf
}

// DataRecord serializes field values in template order.
func DataRecord(fields ...Field) []byte {
	var buf []byte
	for _, f := range fields {
		buf = append(buf, f.Value...)
	}
	return buf
}

// FlowSet wraps records in a flow set with the given ID.
func FlowSet(id uint16, records ...[]byte) []byte {
	length := 4
	for _, r := range records {
		length += len(r)
	}
	buf := make([]byte, length)
	put16(buf[0:], id)
	put16(buf[2:], uint16(length))
	pos := 4
	for _, r := range records {
		copy(buf[pos:], r)
		pos += len(r)
	}
	return buf
}

// OptionsTemplate builds a v9 options template record.
func OptionsTemplate(id, scopeType uint16, scopeLen int, options ...Field) []byte {
	optionLen := 0
	for _, f := range options {
		optionLen += len(f.Value)
	}
	buf := make([]byte, 6+4*(1+len(options)))
	put16(buf[0:], id)
	put16(buf[2:], uint16(scopeLen))
	put16(buf[4:], uint16(optionLen))
	put16(buf[6:], scopeType)
	put16(buf[8:], uint16(scopeLen))
	for i, f := range options {
		put16(buf[10+i*4:], f.Type)
		put16(buf[12+i*4:], uint16(len(f.Value)))
	}
	return buf
}

// OptionsData serializes one options data record.
func OptionsData(scope []byte, options ...Field) []byte {
	buf := append([]byte{}, scope...)
	for _, f := range options {
		buf = append(buf, f.Value...)
	}
	return buf
}

// V9Packet builds a v9 packet from already encoded flow sets.
func V9Packet(now time.Time, uptime uint32, sourceID, sequence uint32, sets ...[]byte) []byte {
	length := 20
	for _, s := range sets {
		length += len(s)
	}
	buf := make([]byte, length)
	put16(buf[0:], 9)
	put16(buf[2:], uint16(len(sets)))
	put32(buf[4:], uptime)
	put32(buf[8:], uint32(now.Unix()))
	put32(buf[12:], sequence)
	put32(buf[16:], sourceID)
	pos := 20
	for _, s := range sets {
		copy(buf[pos:], s)
		pos += len(s)
	}
	return buf
}

// U32 returns a big-endian uint32 field value.
func U32(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

// U16 returns a big-endian uint16 field value.
func U16(v uint16) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, v)
	return b
}

// U8 returns a one-byte field value.
func U8(v uint8) []byte { return []byte{v} }

// IPv4 returns a four-byte address field value.
func IPv4(a, b, c, d byte) []byte { return []byte{a, b, c, d} }

func put16(b []byte, v uint16) { binary.BigEndian.PutUint16(b, v) }
func put32(b []byte, v uint32) { binary.BigEndian.PutUint32(b, v) }
