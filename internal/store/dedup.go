package store

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/a-bali/nfvis/internal/model"
)

// dedupKeyBytes is the width of the stored payload fingerprint. 16 bytes (a
// truncated SHA-256) collides with probability ~1e-19 across the ~10^7 rows a
// day of homelab traffic holds, and costs half what storing the nine-field text
// key would.
const dedupKeyBytes = 16

// flowDedupKey returns the identity of a flow's payload, or nil when the record
// carries no usable identity (all-zero timestamps), in which case it is stored
// unconditionally.
//
// The key deliberately covers the payload and the export interval but not the
// interfaces: OPNsense's ng_netflow emits one record per interface node, so the
// frame seen on re1 and the same frame seen on its VLAN child differ only in
// input_if/output_if.
//
// bytes and packets are part of the key on purpose. OPNsense re-exports a live
// connection with cumulative counters at each 60 s active timeout, so the same
// 5-tuple appears repeatedly with packets 1, 2, 3… Including the counters keeps
// those records distinct; dropping them would silently merge a live connection
// into its first packet.
func flowDedupKey(f *model.Flow) []byte {
	if f.StartMS <= 0 && f.EndMS <= 0 && f.ReceivedAtMS <= 0 {
		return nil
	}
	h := sha256.New()
	var num [8]byte
	writeInt := func(v int64) {
		binary.BigEndian.PutUint64(num[:], uint64(v))
		_, _ = h.Write(num[:])
	}
	writeStr := func(s string) {
		binary.BigEndian.PutUint64(num[:], uint64(len(s)))
		_, _ = h.Write(num[:])
		_, _ = h.Write([]byte(s))
	}

	writeStr(f.Exporter)
	writeInt(f.StartMS)
	writeInt(f.EndMS)
	writeStr(f.SrcAddr)
	writeInt(int64(f.SrcPort))
	writeStr(f.DstAddr)
	writeInt(int64(f.DstPort))
	writeInt(int64(f.Protocol))
	writeInt(int64(f.Bytes))
	writeInt(int64(f.Packets))

	sum := h.Sum(nil)
	return sum[:dedupKeyBytes]
}

// prefersInput reports whether a record on ifIndex should replace a stored
// duplicate on storedIndex.
//
// The surviving record is the one on the smallest non-zero input interface.
// OPNsense numbers interfaces in creation order, so a parent precedes its
// sub-interfaces (measured pairs: 2+7, 2+13, 2+10, 2+11, 2+12), and keeping the
// parent preserves the pre-NAT source identity that the ping fixture shows
// surviving. It also makes the outcome independent of arrival order.
//
// ifIndex 0 means "not recorded", not "the most parent interface": a plain
// minimum would pick it and attribute the traffic to an unknown interface, which
// happens in 1,208 duplicate groups an hour on the measured exporter.
//
// When neither side has an input interface there is nothing to prefer, so the
// first stored copy is kept: no tie-break among equal interfaces is needed (no
// group in the measured hour had two records on the same input_if).
func prefersInput(ifIndex, storedIndex uint32) bool {
	switch {
	case ifIndex == 0 && storedIndex == 0:
		return false
	case ifIndex == 0:
		return false
	case storedIndex == 0:
		return true
	default:
		return ifIndex < storedIndex
	}
}
