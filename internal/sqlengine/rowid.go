package sqlengine

import (
	"errors"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/salesstair-01/001/internal/encoding"
	"github.com/salesstair-01/001/internal/kv"
)

// Tables without a primary key are clustered by a hidden _rowid, as in
// InnoDB: the D2 key is prefix + AppendUint(rowid), and rowids come from a
// per-table counter next to the AUTO_INCREMENT one.
//
// go-mysql-server names the row an UPDATE or DELETE changes by its values,
// not its key. Reads of such a table that take locks (the target of UPDATE
// and DELETE, or SELECT ... FOR UPDATE) note each row's key under its
// encoded content for the statement, and the editor takes keys from there.

// rowidCounter is the counter id of a table's hidden rowid.
func rowidCounter(table uint32) uint32 { return table | 1<<31 }

func (t *Table) rowidKey(id uint64) []byte {
	return encoding.AppendUint(t.prefix(), id, false)
}

// rowKeys maps the content of rows read by one statement to their keys.
type rowKeys struct {
	stmt    uint64
	scanned bool                // the whole table was read into byContent
	seen    map[string]bool     // keys noted
	byRow   map[string][][]byte // encoded row -> keys not yet used
}

type rowKeyTables struct {
	mu sync.Mutex
	m  map[uint32]*rowKeys
}

func (s *Session) rowKeys(table uint32) *rowKeys {
	stmt := s.Stmt()
	if s.keyless.m == nil {
		s.keyless.m = map[uint32]*rowKeys{}
	}
	rk := s.keyless.m[table]
	if rk == nil || rk.stmt != stmt {
		rk = &rowKeys{stmt: stmt, seen: map[string]bool{}, byRow: map[string][][]byte{}}
		s.keyless.m[table] = rk
	}
	return rk
}

func (rk *rowKeys) note(content string, key []byte) {
	if rk.seen[string(key)] {
		return
	}
	rk.seen[string(key)] = true
	rk.byRow[content] = append(rk.byRow[content], append([]byte(nil), key...))
}

func (rk *rowKeys) take(content string) []byte {
	ks := rk.byRow[content]
	if len(ks) == 0 {
		return nil
	}
	rk.byRow[content] = ks[1:]
	return ks[0]
}

// noteRowKey records the key of a row a locking read returned.
func (t *Table) noteRowKey(ctx *sql.Context, row sql.Row, key []byte) error {
	s, ok := ctx.Session.(*Session)
	if !ok {
		return nil
	}
	content, err := t.encodeRow(ctx, row)
	if err != nil {
		return err
	}
	s.keyless.mu.Lock()
	s.rowKeys(t.info.ID).note(string(content), key)
	s.keyless.mu.Unlock()
	return nil
}

// keyOf returns the key of a stored row with these values that the statement
// has not changed yet. Rows not seen by a locking read are found by one
// locking scan of the table.
func (e *editor) keyOf(ctx *sql.Context, row sql.Row) ([]byte, error) {
	s, ok := ctx.Session.(*Session)
	if !ok {
		return nil, errKeylessRow
	}
	content, err := e.t.encodeRow(ctx, row)
	if err != nil {
		return nil, err
	}
	s.keyless.mu.Lock()
	rk := s.rowKeys(e.t.info.ID)
	k := rk.take(string(content))
	scanned := rk.scanned
	s.keyless.mu.Unlock()
	if k != nil || scanned {
		if k == nil {
			return nil, errKeylessRow
		}
		return k, nil
	}
	if err := e.scanKeys(ctx); err != nil {
		return nil, err
	}
	s.keyless.mu.Lock()
	k = rk.take(string(content))
	s.keyless.mu.Unlock()
	if k == nil {
		return nil, errKeylessRow
	}
	return k, nil
}

var errKeylessRow = errors.New("row of a table without a primary key not found")

// scanKeys notes the keys of every row of the table, read with locks.
func (e *editor) scanKeys(ctx *sql.Context) error {
	txn, stmt, err := e.txn(ctx)
	if err != nil {
		return err
	}
	opts := readOpts(ctx, e.t.info.Name)
	opts.Stmt = stmt
	if opts.Lock == kv.LockNone {
		opts.Lock = kv.LockExclusive
		opts.LockWaitMs = lockWait(ctx).Milliseconds()
	}
	start := e.t.prefix()
	end := encoding.PrefixEnd(start)
	for {
		resp, err := txn.Scan(ctx, start, end, scanPage, opts)
		if err != nil {
			return mysqlError(err)
		}
		for i, v := range resp.Values {
			row, err := e.t.decodeRow(ctx, v)
			if err != nil {
				return err
			}
			if err := e.t.noteRowKey(ctx, row, resp.Keys[i]); err != nil {
				return err
			}
		}
		if len(resp.Resume) == 0 {
			break
		}
		start = resp.Resume
	}
	s := ctx.Session.(*Session)
	s.keyless.mu.Lock()
	s.rowKeys(e.t.info.ID).scanned = true
	s.keyless.mu.Unlock()
	return nil
}

// newRowid reserves a hidden rowid for an inserted row.
func (e *editor) newRowid(ctx *sql.Context) ([]byte, error) {
	txn, _, err := e.txn(ctx)
	if err != nil {
		return nil, err
	}
	id := rowidCounter(e.t.info.ID)
	reserve := func(count uint64) (uint64, error) {
		n, err := txn.AutoIncrement(ctx, id, count, 0)
		return n, mysqlError(err)
	}
	var n uint64
	if s, ok := ctx.Session.(*Session); ok {
		n, err = s.nextAutoValue(id, reserve)
	} else {
		n, err = reserve(1)
	}
	if err != nil {
		return nil, err
	}
	return e.t.rowidKey(n), nil
}

// A multi-table UPDATE hands its updater each distinct old row once:
// go-mysql-server's join dedupes a table's rows by content, and identical
// rows of a keyless table join the same partners, so the update applies to
// all of them (issue #29).

var _ sql.KeylessJoinUpdatable = (*Table)(nil)

func (t *Table) KeylessJoinUpdater(ctx *sql.Context) sql.RowUpdater {
	return &joinUpdater{editor: t.editor(ctx)}
}

type joinUpdater struct {
	*editor
	last int // rows the last Update changed
}

var _ sql.KeylessJoinRowCounter = (*joinUpdater)(nil)

func (u *joinUpdater) Update(ctx *sql.Context, old, new sql.Row) error {
	n, err := u.rowsLike(ctx, old)
	if err != nil {
		return err
	}
	if n == 0 {
		return errKeylessRow
	}
	for i := 0; i < n; i++ {
		if err := u.editor.Update(ctx, old, new); err != nil {
			return err
		}
	}
	u.last = n
	return nil
}

func (u *joinUpdater) RowsLike(ctx *sql.Context, row sql.Row) (int, error) {
	return u.rowsLike(ctx, row)
}

func (u *joinUpdater) LastUpdateRows() int { return u.last }

// rowsLike is the number of stored rows with these values that the
// statement has not changed yet, after one locking scan of the table.
func (e *editor) rowsLike(ctx *sql.Context, row sql.Row) (int, error) {
	s, ok := ctx.Session.(*Session)
	if !ok {
		return 0, errKeylessRow
	}
	content, err := e.t.encodeRow(ctx, row)
	if err != nil {
		return 0, err
	}
	s.keyless.mu.Lock()
	scanned := s.rowKeys(e.t.info.ID).scanned
	s.keyless.mu.Unlock()
	if !scanned {
		if err := e.scanKeys(ctx); err != nil {
			return 0, err
		}
	}
	s.keyless.mu.Lock()
	defer s.keyless.mu.Unlock()
	return len(s.rowKeys(e.t.info.ID).byRow[string(content)]), nil
}
