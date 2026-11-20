package decoder

import "testing"

func TestSwitchedTimeUptimeBoundaries(t *testing.T) {
	for _, tc := range []struct{ header, switched uint32 }{
		{0x80000010, 0x7ffffff0},
		{0x00000010, 0xfffffff0},
	} {
		a := makeUptimeAnchor(1700000000, tc.header)
		if got, want := switchedTime(a, tc.switched), int64(1700000000000-32); got != want {
			t.Fatalf("header %x switched %x: got %d want %d", tc.header, tc.switched, got, want)
		}
	}
}
