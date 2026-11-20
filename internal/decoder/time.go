package decoder

import "time"

const two32 = int64(1) << 32

// signedDelta interprets a uint32 millisecond difference as a signed 32-bit
// value, which is how NetFlow uptime counters wrap every ~49.7 days.
func signedDelta(a, b uint32) int64 {
	return int64(int32(a - b))
}

// uptimeAnchor carries the header time and uptime together for wrap-safe conversion.
type uptimeAnchor struct {
	unixMS int64
	uptime uint32
}

func makeUptimeAnchor(unixSec, uptimeMS uint32) uptimeAnchor {
	return uptimeAnchor{unixMS: int64(unixSec) * 1000, uptime: uptimeMS}
}

// switchedTime calculates the signed modular offset relative to header uptime.
func switchedTime(anchor uptimeAnchor, switched uint32) int64 {
	return anchor.unixMS + signedDelta(switched, anchor.uptime)
}

// normalizeWindow checks exported timestamps and falls back to receipt time
// for bucketing when they are missing or implausible. The second return value
// reports an uptime wrap between first and last switched.
func normalizeWindow(start, end, receivedMS int64) (int64, int64, string, bool) {
	wrapped := false
	if end < start && uint32(end)-uint32(start) < uint32(1<<31) {
		end += two32
		wrapped = true
	}
	const year2000MS = 946684800000
	if start <= 0 || end <= 0 || end < start || end > receivedMS+24*time.Hour.Milliseconds() || start < year2000MS {
		return receivedMS, receivedMS, "received", wrapped
	}
	if end > receivedMS+5*time.Minute.Milliseconds() {
		// Keep exported time but the UI flags long flows via time_source=exported;
		// moderate clock skew is common and must not be silently discarded.
	}
	return start, end, "exported", wrapped
}
