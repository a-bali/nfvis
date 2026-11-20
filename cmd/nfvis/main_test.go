package main

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

func TestBackupIncludesWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.db")
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)&_pragma=wal_autocheckpoint(0)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE test (v INTEGER); INSERT INTO test VALUES (42)"); err != nil {
		t.Fatal(err)
	}
	if err := backupDatabase(path); err != nil {
		t.Fatal(err)
	}
	matches, err := filepath.Glob(path + ".bak.*")
	if err != nil || len(matches) != 1 {
		t.Fatalf("backup paths: %v %v", matches, err)
	}
	backup, err := sql.Open("sqlite", "file:"+matches[0]+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer backup.Close()
	var value int
	if err := backup.QueryRow("SELECT v FROM test").Scan(&value); err != nil || value != 42 {
		t.Fatalf("backup value %d: %v", value, err)
	}
	info, err := os.Stat(matches[0])
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("backup permissions: %v %v", info, err)
	}
}
