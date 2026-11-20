// Command loadgen sends synthetic NetFlow v5/v9 UDP packets at a target rate.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/balia/nfvis/internal/testutil"
)

type healthCounters struct {
	Health map[string]json.RawMessage `json:"counters"`
}

func main() {
	addr := flag.String("addr", "127.0.0.1:2055", "collector UDP address")
	target := flag.Int("target", 1000, "target flows per second")
	duration := flag.Duration("duration", 30*time.Second, "run duration")
	perPacket := flag.Int("flows-per-packet", 30, "flows per UDP datagram")
	sourceIP := flag.String("source-ip", "", "optional UDP source IP (exporter identity)")
	v9 := flag.Bool("v9", false, "send v9 instead of v5")
	httpURL := flag.String("health", "http://127.0.0.1:8080/api/v1/health", "health endpoint for counter deltas")
	flag.Parse()

	if *target < 1 || *perPacket < 1 || *perPacket > 200 {
		fmt.Fprintln(os.Stderr, "target must be positive and flows-per-packet must be 1..200")
		os.Exit(2)
	}

	before := readCounters(*httpURL)
	dialer := net.Dialer{}
	if *sourceIP != "" {
		ip := net.ParseIP(*sourceIP)
		if ip == nil {
			fmt.Fprintln(os.Stderr, "invalid -source-ip")
			os.Exit(2)
		}
		dialer.LocalAddr = &net.UDPAddr{IP: ip}
	}
	conn, err := dialer.Dial("udp", *addr)
	if err != nil {
		fatal(err)
	}
	defer conn.Close()

	up := uint32(time.Now().Unix() % 100000)
	now := time.Now()
	records := make([]testutil.V5Field, *perPacket)
	for i := range records {
		records[i] = testutil.V5Field{
			SrcAddr: testutil.DefaultV4Src(), DstAddr: testutil.DefaultV4Dst(),
			Packets: 3, Bytes: 300, Proto: 6, Input: 1, Output: 2,
		}
	}
	fields := []testutil.Field{
		{Type: 8, Value: testutil.IPv4(10, 1, 2, 3)},
		{Type: 12, Value: testutil.IPv4(203, 0, 113, 5)},
		{Type: 1, Value: testutil.U32(300)}, {Type: 2, Value: testutil.U32(3)},
		{Type: 4, Value: testutil.U8(6)}, {Type: 10, Value: testutil.U16(1)},
		{Type: 14, Value: testutil.U16(2)}, {Type: 7, Value: testutil.U16(1234)},
		{Type: 11, Value: testutil.U16(443)}, {Type: 21, Value: testutil.U32(1000)},
		{Type: 22, Value: testutil.U32(900)},
	}
	tmpl := testutil.FlowSet(0, testutil.Template(256, fields...))
	dataRecords := make([][]byte, *perPacket)
	for i := range dataRecords {
		dataRecords[i] = testutil.DataRecord(fields...)
	}
	templatePacket := testutil.V9Packet(now, up, 1, 1, tmpl, testutil.FlowSet(256, dataRecords...))

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.Now().Add(*duration)
	start := time.Now()
	var packets, flows, errors int
	credit := 0.0
	rate := float64(*target) / float64(*perPacket)
	for {
		if time.Now().After(deadline) {
			break
		}
		<-ticker.C
		credit += rate * 0.01
		for credit >= 1 {
			var packet []byte
			if *v9 {
				packet = templatePacket
			} else {
				up += 1
				packet = testutil.V5Packet(time.Now(), up, 100, records...)
			}
			if _, err := conn.Write(packet); err != nil {
				errors++
			} else {
				packets++
				flows += *perPacket
			}
			credit--
		}
	}
	elapsed := time.Since(start)
	after := readCounters(*httpURL)
	rateActual := float64(flows) / elapsed.Seconds()
	fmt.Printf("sent_packets=%d sent_flows=%d errors=%d elapsed=%s achieved_flows_per_s=%.1f target=%d\n",
		packets, flows, errors, elapsed.Round(time.Millisecond), rateActual, *target)
	for _, key := range []string{"flows_stored", "flows_dropped", "udp_queue_drops", "db_errors", "disk_full_events", "flows_decoded"} {
		b, a := counter(before, key), counter(after, key)
		fmt.Printf("%s_before=%d %s_after=%d %s_delta=%d\n", key, b, key, a, key, a-b)
	}
	if rateActual < float64(*target)*0.95 {
		fmt.Fprintln(os.Stderr, "warning: achieved rate is below 95% of target")
	}
}

func readCounters(url string) map[string]json.RawMessage {
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "health read failed: %v\n", err)
		return map[string]json.RawMessage{}
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var parsed healthCounters
	if err := json.Unmarshal(body, &parsed); err != nil {
		fmt.Fprintf(os.Stderr, "health parse failed: %v\n", err)
		return map[string]json.RawMessage{}
	}
	return parsed.Health
}

func counter(c map[string]json.RawMessage, key string) int64 {
	var n int64
	_ = json.Unmarshal(c[key], &n)
	return n
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
