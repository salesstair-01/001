package sqlengine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"

	"github.com/salesstair-01/001/internal/kv"
)

// Session is a MySQL session whose transactions are KV transactions.
type Session struct {
	*sql.BaseSession
	store *Store
	stmt  atomic.Uint64 // statement id; one per client command
}

var (
	_ sql.TransactionSession    = (*Session)(nil)
	_ sql.LifecycleAwareSession = (*Session)(nil)
	_ sql.Transaction           = (*Txn)(nil)
)

// NewSession wraps a base session.
func NewSession(base *sql.BaseSession, store *Store) *Session {
	s := &Session{BaseSession: base, store: store}
	s.stmt.Store(1)
	return s
}

// Stmt returns the current statement id.
func (s *Session) Stmt() uint64 { return s.stmt.Load() }

// CommandBegin starts a new statement: its writes form one statement layer.
func (s *Session) CommandBegin() error { s.stmt.Add(1); return nil }
func (s *Session) CommandEnd()         {}

// SessionEnd rolls back an open transaction when the client goes away.
func (s *Session) SessionEnd() {
	if tx, ok := s.GetTransaction().(*Txn); ok && tx != nil {
		_ = tx.Rollback(context.Background())
	}
}

func (s *Session) isolation(ctx *sql.Context) (kv.Isolation, error) {
	v, err := s.GetSessionVariable(ctx, "transaction_isolation")
	if err != nil {
		return kv.RepeatableRead, nil
	}
	switch strings.ToUpper(strings.ReplaceAll(fmt.Sprint(v), " ", "-")) {
	case "READ-COMMITTED":
		return kv.ReadCommitted, nil
	case "READ-UNCOMMITTED":
		ctx.Warn(1105, "READ UNCOMMITTED runs as READ COMMITTED")
		return kv.ReadCommitted, nil
	case "SERIALIZABLE":
		return 0, mysql.NewSQLError(mysql.ERNotSupportedYet, mysql.SSUnknownSQLState,
			"SERIALIZABLE is not supported: without gap locks it would be a false promise; use REPEATABLE READ")
	}
	return kv.RepeatableRead, nil
}

// StartTransaction creates a transaction; the KV side begins on first use.
func (s *Session) StartTransaction(ctx *sql.Context, tc sql.TransactionCharacteristic) (sql.Transaction, error) {
	iso, err := s.isolation(ctx)
	if err != nil {
		return nil, err
	}
	return s.store.NewTxn(iso, tc == sql.ReadOnly), nil
}

func (s *Session) CommitTransaction(ctx *sql.Context, tx sql.Transaction) error {
	t, ok := tx.(*Txn)
	if !ok {
		return nil
	}
	return mysqlError(t.Commit(ctx))
}

func (s *Session) Rollback(ctx *sql.Context, tx sql.Transaction) error {
	t, ok := tx.(*Txn)
	if !ok {
		return nil
	}
	return mysqlError(t.Rollback(ctx))
}

var errSavepoints = mysql.NewSQLError(mysql.ERNotSupportedYet, mysql.SSUnknownSQLState, "savepoints are not supported in v1")

func (s *Session) CreateSavepoint(*sql.Context, sql.Transaction, string) error { return errSavepoints }
func (s *Session) RollbackToSavepoint(*sql.Context, sql.Transaction, string) error {
	return errSavepoints
}
func (s *Session) ReleaseSavepoint(*sql.Context, sql.Transaction, string) error { return errSavepoints }

// mysqlError maps KV errors to MySQL errors (D7).
func mysqlError(err error) error {
	if err == nil {
		return nil
	}
	var ke *kv.Error
	if !errors.As(err, &ke) {
		return err
	}
	switch ke.Kind {
	case kv.KindLockTimeout:
		return mysql.NewSQLError(mysql.ERLockWaitTimeout, "HY000", "Lock wait timeout exceeded; try restarting transaction")
	case kv.KindDuplicateKey:
		return mysql.NewSQLError(mysql.ERDupEntry, "23000", "Duplicate entry for key 'PRIMARY'")
	case kv.KindTxnTooLarge:
		return mysql.NewSQLError(1197, "HY000", "Multi-statement transaction required more than 64 MiB of storage; batch the work into smaller transactions")
	case kv.KindDeadlock, kv.KindConflict, kv.KindEpochChanged, kv.KindTxnNotFound, kv.KindUnavailable:
		return mysql.NewSQLError(mysql.ERLockDeadlock, mysql.SSLockDeadlock,
			"Deadlock found when trying to get lock; try restarting transaction (%s)", ke.Msg)
	}
	return err
}

// sessionTxn returns the session's transaction, or a temporary one (and a
// function that ends it) for reads outside any transaction.
func sessionTxn(ctx *sql.Context, store *Store) (*Txn, uint64, func()) {
	s, _ := ctx.Session.(*Session)
	var stmt uint64 = 1
	if s != nil {
		stmt = s.Stmt()
	}
	if t, ok := ctx.GetTransaction().(*Txn); ok && t != nil {
		return t, stmt, func() {}
	}
	t := store.NewTxn(kv.ReadCommitted, true)
	return t, stmt, func() { _ = t.Rollback(context.Background()) }
}
