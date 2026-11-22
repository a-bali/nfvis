//go:build race

package store

// raceEnabled reports whether the binary was built with the race detector, so a
// timing-sensitive test can distinguish real contention from the detector's own
// slowdown.
const raceEnabled = true
