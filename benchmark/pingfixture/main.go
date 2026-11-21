// Command pingfixture sends the three NetFlow v5 records OPNsense exports for
// one ICMP echo: the parent interface copy, the VLAN child copy, and the
// post-NAT copy. Used to exercise duplicate suppression against a live binary.
package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/a-bali/nfvis/internal/testutil"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:21055", "collector UDP address")
	wan := flag.String("wan", "192.168.0.2", "post-NAT source address")
	flag.Parse()

	conn, err := net.Dial("udp", *addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer conn.Close()
	parsed := net.ParseIP(*wan)
	if parsed == nil || parsed.To4() == nil {
		fmt.Fprintln(os.Stderr, "invalid -wan")
		os.Exit(2)
	}
	var wanAddr [4]byte
	copy(wanAddr[:], parsed.To4())

	inner := [4]byte{10, 10, 10, 194}
	dst := testutil.DefaultV4Dst()
	parent := testutil.V5Field{SrcAddr: inner, DstAddr: dst, Input: 2, Output: 1,
		Packets: 1, Bytes: 98, Proto: 1, First: 1000, Last: 2000}
	child := parent
	child.Input = 7
	postNAT := parent
	postNAT.SrcAddr = wanAddr
	postNAT.SrcPort = 41234

	// One datagram carrying all three records, as a single exporter would emit.
	packet := testutil.V5Packet(time.Now(), 2000, 0, parent, child, postNAT)
	if _, err := conn.Write(packet); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("sent 3 records (in=2 pre-NAT, in=7 pre-NAT child, in=2 post-NAT %s) to %s\n", *wan, *addr)
}
