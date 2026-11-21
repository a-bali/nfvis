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
// The parent interface carries the lower ifIndex in every OPNsense topology
// pair observed in production — (2,1)/(7,1), (2,1)/(13,1), (2,2)/(10,2),
// (2,3)/(13,3) — and the surviving record measured for a ping is the parent's
// (in=2). Surviving on the lower ifIndex therefore keeps the record that holds
// the pre-NAT source identity, and it makes the outcome independent of which
// copy the exporter happened to send first.
func prefersInput(ifIndex, storedIndex uint32) bool {
	return ifIndex < storedIndex
}
