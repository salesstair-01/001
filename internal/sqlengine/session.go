package sqlengine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/salesstair-01/001/internal/kv"
	"github.com/salesstair-01/001/internal/kvrpc"
)

// Session is a MySQL session whose transactions are KV transactions.
type Session struct {
	*sql.BaseSession
	store *Store
	stmt  atomic.Uint64 // statement id; one per client command

	locksMu sync.Mutex
	locks   stmtLocks // lock modes of the current statement

	autoMu sync.Mutex
	auto   map[uint32]*autoBatch // AUTO_INCREMENT reservations of the current statement

	txnMu   sync.Mutex
	current *Txn // the open transaction, until it commits or rolls back
}

// autoBatch is a reservation of consecutive AUTO_INCREMENT values. It doubles
// with each refill within a statement, so a single-row INSERT reserves one
// value and a bulk INSERT needs few round trips.
type autoBatch struct {
	stmt, next, end, size uint64
}

func (s *Session) nextAutoValue(table uint32, reserve func(count uint64) (uint64, error)) (uint64, error) {
	s.autoMu.Lock()
	defer s.autoMu.Unlock()
	if s.auto == nil {
		s.auto = map[uint32]*autoBatch{}
	}
	stmt := s.Stmt()
	b := s.auto[table]
	if b == nil || b.stmt != stmt {
		b = &autoBatch{stmt: stmt}
		s.auto[table] = b
	}
	if b.next >= b.end {
		b.size *= 2
		if b.size == 0 {
			b.size = 1
		}
		if b.size > 1024 {
			b.size = 1024
		}
		first, err := reserve(b.size)
		if err != nil {
			return 0, err
		}
		b.next, b.end = first, first+b.size
	}
	v := b.next
	b.next++
	return v, nil
}

// dropAutoBatch forgets the reservation after an explicit value raised the counter.
func (s *Session) dropAutoBatch(table uint32) {
	s.autoMu.Lock()
	delete(s.auto, table)
	s.autoMu.Unlock()
}

// stmtLocks says which tables a statement reads with locks (D5, D6).
type stmtLocks struct {
	stmt   uint64
	all    kv.LockMode            // every table (SELECT ... FOR UPDATE / LOCK IN SHARE MODE)
	tables map[string]kv.LockMode // target tables of UPDATE and DELETE
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

// CommandEnd ends transactions go-mysql-server leaves behind. It clears a
// failed autocommit statement's transaction without calling Rollback, and a
// statement that fails while its rows stream keeps its transaction set, so
// the next autocommit statement would join it. Either way the KV transaction
// would hold its locks. MySQL rolls a failed autocommit statement back.
func (s *Session) CommandEnd() {
	if cur, ok := s.GetTransaction().(*Txn); ok && cur != nil && !s.GetIgnoreAutoCommit() && s.autocommit() {
		s.SetTransaction(nil)
		s.finished(cur)
		_ = cur.Rollback(context.Background())
	}
	s.endOrphan()
}

func (s *Session) autocommit() bool {
	v, err := s.GetSessionVariable(sql.NewEmptyContext(), "autocommit")
	if err != nil {
		return true
	}
	switch x := v.(type) {
	case int8:
		return x != 0
	case int64:
		return x != 0
	case bool:
		return x
	}
	return fmt.Sprint(v) != "0"
}

// SessionEnd rolls back an open transaction when the client goes away.
func (s *Session) SessionEnd() {
	s.txnMu.Lock()
	t := s.current
	s.current = nil
	s.txnMu.Unlock()
	if t != nil {
		_ = t.Rollback(context.Background())
	}
}

// endOrphan rolls back a transaction the session no longer refers to.
func (s *Session) endOrphan() {
	s.txnMu.Lock()
	t := s.current
	if t == nil || s.GetTransaction() == sql.Transaction(t) {
		s.txnMu.Unlock()
		return
	}
	s.current = nil
	s.txnMu.Unlock()
	_ = t.Rollback(context.Background())
}

func (s *Session) finished(t *Txn) {
	s.txnMu.Lock()
	if s.current == t {
		s.current = nil
	}
	s.txnMu.Unlock()
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
	s.endOrphan()
	iso, err := s.isolation(ctx)
	if err != nil {
		return nil, err
	}
	t := s.store.NewTxn(iso, tc == sql.ReadOnly)
	s.txnMu.Lock()
	if old := s.current; old != nil && old != t {
		defer func() { _ = old.Rollback(context.Background()) }()
	}
	s.current = t
	s.txnMu.Unlock()
	return t, nil
}

func (s *Session) CommitTransaction(ctx *sql.Context, tx sql.Transaction) error {
	t, ok := tx.(*Txn)
	if !ok {
		return nil
	}
	defer s.finished(t)
	return mysqlError(t.Commit(ctx))
}

func (s *Session) Rollback(ctx *sql.Context, tx sql.Transaction) error {
	t, ok := tx.(*Txn)
	if !ok {
		return nil
	}
	defer s.finished(t)
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
	case kv.KindDeadlock, kv.KindEpochChanged, kv.KindTxnNotFound, kv.KindUnavailable, kv.KindSchemaChanged:
		return mysql.NewSQLError(mysql.ERLockDeadlock, mysql.SSLockDeadlock,
			"Deadlock found when trying to get lock; try restarting transaction (%s)", ke.Msg)
	}
	return err
}

// lockMode returns how the current statement reads table: UPDATE and DELETE
// targets and SELECT ... FOR UPDATE take exclusive locks and read the latest
// version; LOCK IN SHARE MODE takes shared locks; everything else is a
// consistent (snapshot) read. go-mysql-server drops the lock clause while
// planning, so the statement text is parsed once per statement.
func (s *Session) lockMode(ctx *sql.Context, table string) kv.LockMode {
	s.locksMu.Lock()
	defer s.locksMu.Unlock()
	if stmt := s.Stmt(); s.locks.stmt != stmt {
		s.locks = parseLocks(ctx.Query())
		s.locks.stmt = stmt
	}
	if s.locks.all != kv.LockNone {
		return s.locks.all
	}
	return s.locks.tables[strings.ToLower(table)]
}

func parseLocks(query string) stmtLocks {
	out := stmtLocks{tables: map[string]kv.LockMode{}}
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return out
	}
	switch st := stmt.(type) {
	case *sqlparser.Select:
		switch st.Lock {
		case sqlparser.ForUpdateStr, sqlparser.ForUpdateSkipLockedStr:
			out.all = kv.LockExclusive
		case sqlparser.ShareModeStr:
			out.all = kv.LockShared
		}
	case *sqlparser.Update:
		addTables(out.tables, st.TableExprs)
	case *sqlparser.Delete:
		if len(st.Targets) > 0 {
			for _, t := range st.Targets {
				out.tables[strings.ToLower(t.Name.String())] = kv.LockExclusive
			}
		} else {
			addTables(out.tables, st.TableExprs)
		}
	}
	return out
}

func addTables(m map[string]kv.LockMode, exprs sqlparser.TableExprs) {
	for _, e := range exprs {
		switch te := e.(type) {
		case *sqlparser.AliasedTableExpr:
			if tn, ok := te.Expr.(sqlparser.TableName); ok {
				m[strings.ToLower(tn.Name.String())] = kv.LockExclusive
			}
		case *sqlparser.JoinTableExpr:
			addTables(m, sqlparser.TableExprs{te.LeftExpr, te.RightExpr})
		case *sqlparser.ParenTableExpr:
			addTables(m, te.Exprs)
		}
	}
}

// lockWait returns the session's innodb_lock_wait_timeout (default 50 s).
func lockWait(ctx *sql.Context) time.Duration {
	v, err := ctx.GetSessionVariable(ctx, "innodb_lock_wait_timeout")
	if err != nil {
		return kv.DefaultLockWait
	}
	switch n := v.(type) {
	case int64:
		return time.Duration(n) * time.Second
	case int32:
		return time.Duration(n) * time.Second
	case int:
		return time.Duration(n) * time.Second
	case uint64:
		return time.Duration(n) * time.Second
	}
	return kv.DefaultLockWait
}

// readOpts returns the read options for table in the current statement.
func readOpts(ctx *sql.Context, table string) kvrpc.ReadOpts {
	o := kvrpc.ReadOpts{Stmt: 1}
	if s, ok := ctx.Session.(*Session); ok {
		o.Stmt = s.Stmt()
		o.Lock = s.lockMode(ctx, table)
	}
	if o.Lock != kv.LockNone {
		o.LockWaitMs = lockWait(ctx).Milliseconds()
	}
	return o
}

// sessionTxn returns the session's transaction, or a temporary one (and a
// function that ends it) for reads outside any transaction.
func sessionTxn(ctx *sql.Context, store *Store) (*Txn, func()) {
	if t, ok := ctx.GetTransaction().(*Txn); ok && t != nil {
		return t, func() {}
	}
	t := store.NewTxn(kv.ReadCommitted, true)
	return t, func() { _ = t.Rollback(context.Background()) }
}
