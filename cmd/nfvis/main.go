// Command nfvis collects NetFlow v5/v9 and serves a local dashboard.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/balia/nfvis/internal/api"
	"github.com/balia/nfvis/internal/config"
	"github.com/balia/nfvis/internal/decoder"
	"github.com/balia/nfvis/internal/ingest"
	"github.com/balia/nfvis/internal/store"
	"modernc.org/sqlite"
)

// version is set by release builds with -ldflags; source builds identify as development.
var version = "dev"

func main() {
	configPath := flag.String("config", "config.yaml", "YAML configuration file (missing file uses defaults)")
	backup := flag.Bool("backup", false, "copy the SQLite database before applying migrations")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [options]\n  --config PATH  YAML configuration file (default: config.yaml; missing file uses defaults)\n  --backup       snapshot the database before migrations\n  --version      print version and exit\n  --help         show this help\n", os.Args[0])
	}
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "unexpected arguments:", strings.Join(flag.Args(), " "))
		flag.Usage()
		os.Exit(2)
	}
	if *showVersion {
		fmt.Println("nfvis", version)
		return
	}

	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	// Install the JSON handler as the default too, so packages that log through
	// slog (store migrations, backfill) emit the same structured format.
	slog.SetDefault(log)
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Error("configuration error", "error", err)
		os.Exit(2)
	}
	if err := cfg.Validate(); err != nil {
		log.Error("configuration validation failed", "error", err)
		os.Exit(2)
	}
	if *backup {
		if err := backupDatabase(cfg.Database.Path); err != nil {
			log.Error("database backup failed", "error", err)
			os.Exit(1)
		}
	}

	st, err := store.Open(cfg)
	if err != nil {
		log.Error("database open failed", "error", err)
		os.Exit(1)
	}
	defer func() { _ = st.Close() }()

	dec := decoder.New(cfg)
	counters := &ingest.Counters{}
	collector := ingest.New(cfg, dec, st, counters, log)
	if err := collector.Start(); err != nil {
		log.Error("collector start failed", "error", err)
		os.Exit(1)
	}

	server := api.New(cfg, st, collector, log, version)
	serverErr := make(chan error, 1)
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	failed := false
	select {
	case sig := <-signals:
		log.Info("shutting down", "signal", sig.String())
	case err := <-serverErr:
		log.Error("HTTP server failed", "error", err)
		failed = true
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
	if err := collector.Stop(); err != nil {
		log.Error("collector shutdown error", "error", err)
	}
	log.Info("nfvis stopped")
	if failed {
		_ = st.Close()
		os.Exit(1)
	}
}

func backupDatabase(path string) (result error) {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	dstPath := fmt.Sprintf("%s.bak.%s", abs, time.Now().UTC().Format("20060102T150405Z"))
	dst, err := os.OpenFile(dstPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if err := dst.Close(); err != nil {
		_ = os.Remove(dstPath)
		return err
	}
	defer func() {
		if result != nil {
			_ = os.Remove(dstPath)
		}
	}()
	u := url.URL{Scheme: "file", Path: abs, RawQuery: "mode=ro&_pragma=busy_timeout%285000%29"}
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return err
	}
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()
	return conn.Raw(func(driverConn any) error {
		src, ok := driverConn.(interface {
			NewBackup(string) (*sqlite.Backup, error)
		})
		if !ok {
			return fmt.Errorf("sqlite driver does not support backup")
		}
		backup, err := src.NewBackup(dstPath)
		if err != nil {
			return err
		}
		_, stepErr := backup.Step(-1)
		finishErr := backup.Finish()
		if stepErr != nil {
			return stepErr
		}
		return finishErr
	})
}
