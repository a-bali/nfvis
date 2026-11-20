// Package testutil builds synthetic NetFlow datagrams for tests and benchmarks.
package testutil

import (
	"encoding/binary"
	"net"
	"strings"
	"sync"
)

// DNSServer is a minimal UDP DNS server that answers every PTR query with a
// name derived from the queried address. It exists so tests can prove that a
// configured server was actually used instead of the host resolver.
type DNSServer struct {
	// Addr is the "127.0.0.1:port" the server listens on.
	Addr string
	// Name is the PTR name returned for every query. A trailing dot is added
	// when missing.
	Name string

	conn net.PacketConn

	mu      sync.Mutex
	queries []string
	done    chan struct{}
}

// StartDNSServer starts a DNS server on a loopback port. Call Stop when done.
func StartDNSServer(name string) (*DNSServer, error) {
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	if !strings.HasSuffix(name, ".") {
		name += "."
	}
	s := &DNSServer{Addr: conn.LocalAddr().String(), Name: name, conn: conn, done: make(chan struct{})}
	go s.serve()
	return s, nil
}

// Stop closes the listener and waits for the serving goroutine.
func (s *DNSServer) Stop() {
	select {
	case <-s.done:
	default:
		close(s.done)
	}
	_ = s.conn.Close()
}

// Queries returns every question name received so far.
func (s *DNSServer) Queries() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.queries...)
}

func (s *DNSServer) serve() {
	buf := make([]byte, 512)
	for {
		n, from, err := s.conn.ReadFrom(buf)
		if err != nil {
			return
		}
		query := buf[:n]
		if len(query) < 12 {
			continue
		}
		name, questionEnd, ok := readQuestionName(query, 12)
		if !ok || questionEnd+4 > len(query) {
			continue
		}
		s.mu.Lock()
		s.queries = append(s.queries, name)
		s.mu.Unlock()
		if resp := buildPTRResponse(query, questionEnd+4, s.Name); resp != nil {
			_, _ = s.conn.WriteTo(resp, from)
		}
	}
}

// readQuestionName decodes the (uncompressed) question name at off and returns
// it with the offset just past the terminating zero byte.
func readQuestionName(msg []byte, off int) (string, int, bool) {
	var labels []string
	for {
		if off >= len(msg) {
			return "", 0, false
		}
		length := int(msg[off])
		off++
		if length == 0 {
			break
		}
		if length > 63 || off+length > len(msg) {
			return "", 0, false
		}
		labels = append(labels, string(msg[off:off+length]))
		off += length
	}
	return strings.Join(labels, ".") + ".", off, true
}

// buildPTRResponse answers the single PTR question that ends at questionEnd
// (just past qtype and qclass) with one PTR record pointing back at the
// question name via a compression pointer.
//
// The answer is inserted after the question and before any additional records,
// because Go's resolver sends an EDNS0 OPT record there and a response that
// drops it is rejected by the resolver as a mismatch.
func buildPTRResponse(query []byte, questionEnd int, name string) []byte {
	if questionEnd > len(query) {
		return nil
	}
	header := make([]byte, questionEnd, questionEnd+64+len(query)-questionEnd)
	copy(header, query[:questionEnd])
	qs := binary.BigEndian.Uint16(query[4:6])
	an := binary.BigEndian.Uint16(query[6:8])
	ns := binary.BigEndian.Uint16(query[8:10])
	ar := binary.BigEndian.Uint16(query[10:12])
	header[2] = 0x81 | (query[2] & 0x01) // QR=1, AA=1, keep opcode and RD
	header[3] = 0x80                     // RA=1, rcode=NOERROR
	binary.BigEndian.PutUint16(header[4:6], qs)
	binary.BigEndian.PutUint16(header[6:8], an+1)
	binary.BigEndian.PutUint16(header[8:10], ns)
	binary.BigEndian.PutUint16(header[10:12], ar)

	rdata := encodeName(name)
	answer := make([]byte, 0, 12+len(rdata))
	answer = append(answer, 0xC0, 0x0C) // pointer to the question name
	answer = append(answer, 0x00, 0x0C) // TYPE PTR
	answer = append(answer, 0x00, 0x01) // CLASS IN
	answer = append(answer, 0x00, 0x00, 0x00, 0x3C)
	answer = binary.BigEndian.AppendUint16(answer, uint16(len(rdata)))
	answer = append(answer, rdata...)

	out := append(header, answer...)
	// Preserve the query's additional section (EDNS0 OPT), if any.
	return append(out, query[questionEnd:]...)
}

func encodeName(name string) []byte {
	var out []byte
	for _, label := range strings.Split(strings.TrimSuffix(name, "."), ".") {
		if label == "" {
			continue
		}
		out = append(out, byte(len(label)))
		out = append(out, label...)
	}
	return append(out, 0)
}
