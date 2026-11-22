package store

import (
	"sync/atomic"

	"modernc.org/sqlite"
)

// txCommits counts COMMIT events across the pool by registering a connection
// authorizer. An authorizer is the only supported way to observe transaction
// boundaries: SELECT total_changes() counts rows modified, not transactions, so
// it cannot distinguish "one transaction wrote four rows" from "four
// transactions wrote one row each".
var (
	txCommits     atomic.Int64
	txHookReady   atomic.Bool
	txHookInstall atomic.Bool
)

// txCountAvailable reports whether the authorizer was installed, so a test can
// skip rather than silently assert on a counter that never moves.
func txCountAvailable() bool { return txHookReady.Load() }

// txCommitCount returns the number of commits observed since the hook was
// installed.
func txCommitCount() int64 { return txCommits.Load() }

// installTxCounter registers a connection hook once. Every new connection gets an
// authorizer that counts AuthTransaction events carrying SQLITE_TRANSACTION on
// COMMIT; other actions return SQLITE_OK so the policy never denies anything.
//
// Called from an init so it is in place before any pool opens.
func installTxCounter() {
	if !txHookInstall.CompareAndSwap(false, true) {
		return
	}
	sqlite.RegisterConnectionHook(func(driverConn sqlite.ExecQuerierContext, _ string) error {
		registerer, ok := driverConn.(sqlite.AuthorizerRegisterer)
		if !ok {
			return nil // leave txHookReady false; tests that need it will skip
		}
		err := registerer.RegisterAuthorizer(func(action sqlite.AuthorizerActionCode, arg1, _, _, _ string) sqlite.AuthorizerReturnCode {
			// AuthTransaction fires for BEGIN/COMMIT/ROLLBACK with the statement
			// name in arg1.
			if action == sqlite.AuthTransaction && arg1 == "COMMIT" {
				txCommits.Add(1)
			}
			return sqlite.AuthorizerOK
		})
		if err == nil {
			txHookReady.Store(true)
		}
		return err
	})
}

func init() { installTxCounter() }
