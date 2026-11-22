package ingest

import (
	"errors"
	"testing"
)

// TestSetSuccessClearsLastError covers half of the production report that the UI
// showed "insert flow: database is locked (5) (SQLITE_BUSY)" indefinitely.
//
// setError stored the message and nothing ever cleared it: Snapshot reported it
// forever and the health handler appends it to warnings unconditionally, so a
// single transient lock kept the dashboard warning up for the life of the
// process. The cumulative DBErrors counter is what must preserve the history;
// the message must not.
func TestSetSuccessClearsLastError(t *testing.T) {
	var c Counters
	c.Healthy.Store(true)

	c.setError(errors.New("insert flow: database is locked (5) (SQLITE_BUSY)"))

	s := c.Snapshot()
	if s.LastError == "" {
		t.Fatal("error not recorded")
	}
	if s.Healthy {
		t.Error("Healthy should be false after an error")
	}
	if s.DBErrors != 1 {
		t.Errorf("DBErrors = %d, want 1", s.DBErrors)
	}

	c.setSuccess()

	s = c.Snapshot()
	if s.LastError != "" {
		t.Errorf("LastError = %q after a success; a recovered instance must stop reporting the "+
			"old failure, or the dashboard shows it for the life of the process", s.LastError)
	}
	if !s.Healthy {
		t.Error("Healthy should be true after a success")
	}
	// The cumulative counter must survive: clearing the message must not erase
	// the evidence that an error happened.
	if s.DBErrors != 1 {
		t.Errorf("DBErrors = %d after success, want 1 (cumulative, must not reset)", s.DBErrors)
	}
	if s.LastSuccessMS == 0 {
		t.Error("LastSuccessMS not recorded")
	}
}

// TestSetErrorIgnoresNil guards the retry loop, which calls setError with the
// last error state even when it recovered.
func TestSetErrorIgnoresNil(t *testing.T) {
	var c Counters
	c.Healthy.Store(true)
	c.setError(nil)
	if s := c.Snapshot(); s.DBErrors != 0 || !s.Healthy || s.LastError != "" {
		t.Errorf("setError(nil) changed state: %+v", s)
	}
}
