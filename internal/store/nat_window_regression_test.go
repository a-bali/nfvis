package store

import (
	"context"
	"testing"
	"time"

	"github.com/a-bali/nfvis/internal/model"
)

func TestNATConfiguredWindowIsHonored(t *testing.T) {
	for _, tc := range []struct {
		name        string
		window, gap time.Duration
		want        int
	}{
		{"wide-within", 10 * time.Second, 6 * time.Second, 1},
		{"wide-outside", 10 * time.Second, 11 * time.Second, 2},
		{"narrow-within", 200 * time.Millisecond, 100 * time.Millisecond, 1},
		{"narrow-outside", 200 * time.Millisecond, 300 * time.Millisecond, 2},
	} {
		for _, reverse := range []bool{false, true} {
			t.Run(tc.name+map[bool]string{false: "/pre-first", true: "/post-first"}[reverse], func(t *testing.T) {
				s := natStore(t)
				s.nat.windowMS = tc.window.Milliseconds()
				now := time.Now().UTC().Truncate(time.Second)
				pre := preNATRecord(now)
				post := postNATRecord(now)
				post.EndMS = pre.EndMS + tc.gap.Milliseconds()
				pair := []model.Flow{pre, post}
				if reverse {
					pair[0], pair[1] = pair[1], pair[0]
				}
				if err := s.InsertBatch(context.Background(), pair); err != nil {
					t.Fatal(err)
				}
				if got := countRows(t, s, "flows"); got != int64(tc.want) {
					t.Fatalf("window %v, gap %v, reverse %v: rows=%d want=%d", tc.window, tc.gap, reverse, got, tc.want)
				}
			})
		}
	}
}
