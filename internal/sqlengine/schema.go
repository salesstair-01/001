package sqlengine

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/salesstair-01/001/internal/encoding"
	"github.com/salesstair-01/001/internal/kv"
	"github.com/salesstair-01/001/internal/kvrpc"
)

// Schema changes while several query engines serve one tenant (slice 8).
//
// Writers guard their commits on the table's schema version, so a write
// made with a stale schema never commits. Index builds go through the
// write-only state: writers maintain the new index, a backfill fills in the
// existing rows in locking batches, and only then is it made public. The
// backfill's progress is a row in the catalog, so the build survives a KV
// suspend or the query engine running it: any engine that loads the
// catalog resumes it. Dropping an index first makes it write-only and waits
// out the catalog lease, so no engine still reads it when it is removed.

// jobState is the progress of an index backfill.
type jobState struct {
	Resume    []byte `json:"resume,omitempty"` // next primary key to backfill; nil = from the start
	Rows      int64  `json:"rows"`
	Owner     string `json:"owner,omitempty"` // engine running it
	Heartbeat int64  `json:"heartbeat"`       // unix nanos; a stale owner is taken over
	Done      bool   `json:"done,omitempty"`
	Error     string `json:"error,omitempty"`
}

const (
	jobBatch     = 256
	jobStaleness = 10 * time.Second
)

// jobRunner tracks the index jobs a store's engine is running.
type jobRunner struct {
	mu      sync.Mutex
	running map[[2]uint32]chan error
	owner   string
}

func (s *Store) jobs() *jobRunner {
	s.jobsOnce.Do(func() {
		var b [8]byte
		_, _ = rand.Read(b[:])
		s.jobRun = &jobRunner{running: map[[2]uint32]chan error{}, owner: hex.EncodeToString(b[:])}
	})
	return s.jobRun
}

// startIndexJob runs the job for an index unless this engine already is, and
// returns a channel that yields its outcome.
func (s *Store) startIndexJob(table, index uint32) chan error {
	jr := s.jobs()
	jr.mu.Lock()
	defer jr.mu.Unlock()
	k := [2]uint32{table, index}
	if ch, ok := jr.running[k]; ok {
		return ch
	}
	ch := make(chan error, 1)
	jr.running[k] = ch
	go func() {
		err := s.runIndexJob(table, index)
		jr.mu.Lock()
		delete(jr.running, k)
		jr.mu.Unlock()
		ch <- err
		close(ch)
	}()
	return ch
}

// resumeJobs restarts unfinished index builds and drops found in the catalog.
func (s *Store) resumeJobs(tables map[uint32]*TableInfo) {
	for _, t := range tables {
		for _, ix := range t.Indexes {
			if ix.State == IndexWriteOnly {
				s.startIndexJob(t.ID, ix.ID)
			}
		}
	}
}

func (s *Store) tableByID(ctx context.Context, id uint32) (*TableInfo, error) {
	if err := s.catalog.ensure(ctx); err != nil {
		return nil, err
	}
	s.catalog.mu.RLock()
	defer s.catalog.mu.RUnlock()
	return s.catalog.tables[id], nil
}

func findIndex(t *TableInfo, id uint32) *IndexInfo {
	for i := range t.Indexes {
		if t.Indexes[i].ID == id {
			return &t.Indexes[i]
		}
	}
	return nil
}

var errJobBusy = errors.New("another query engine is running this index job")

// runIndexJob drives a write-only index to completion: backfill then public,
// or for a drop, wait out the lease then remove. Transient failures (owner
// changes, KV wake-ups, lock conflicts) retry from the last committed batch.
func (s *Store) runIndexJob(tableID, indexID uint32) error {
	ctx := sql.NewEmptyContext()
	backoff := 50 * time.Millisecond
	for attempt := 0; ; attempt++ {
		done, err := s.indexJobStep(ctx, tableID, indexID)
		if done {
			return err
		}
		if err != nil && !retryableJobErr(err) {
			return err
		}
		if err != nil {
			if errIsKind(err, kv.KindSchemaChanged) {
				s.catalog.Invalidate()
			}
			time.Sleep(backoff)
			if backoff < 2*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = 50 * time.Millisecond
	}
}

func retryableJobErr(err error) bool {
	if errors.Is(err, errJobBusy) {
		return true
	}
	switch kv.KindOf(err) {
	case kv.KindEpochChanged, kv.KindDeadlock, kv.KindLockTimeout, kv.KindTxnNotFound, kv.KindUnavailable, kv.KindSchemaChanged:
		return true
	}
	return false
}

// indexJobStep does one batch. done reports that the job ended (with err).
func (s *Store) indexJobStep(ctx *sql.Context, tableID, indexID uint32) (done bool, err error) {
	ti, err := s.tableByID(ctx, tableID)
	if err != nil {
		return false, err
	}
	var ix *IndexInfo
	if ti != nil {
		ix = findIndex(ti, indexID)
	}
	if ix == nil || ix.State != IndexWriteOnly {
		return true, nil // finished, dropped, or the table is gone
	}
	if ix.Dropping {
		return s.finishDrop(ctx, ti, ix)
	}
	t, err := newTable(ctx, s.catalog, ti)
	if err != nil {
		return true, err
	}
	var sec *secIndex
	for _, x := range t.indexes {
		if x.info.ID == indexID {
			sec = x
		}
	}

	txn := s.NewTxn(kv.RepeatableRead, false)
	defer func() {
		if err != nil {
			_ = txn.Rollback(ctx)
		}
	}()
	jr := s.jobs()
	jk := s.jobKey(tableID, indexID)
	vs, err := txn.Get(ctx, [][]byte{jk}, kvrpc.ReadOpts{Stmt: 1, IncludeStmt: true, Lock: kv.LockExclusive})
	if err != nil {
		return false, err
	}
	var job jobState
	if vs[0] != nil {
		_ = json.Unmarshal(vs[0], &job)
	}
	if job.Owner != "" && job.Owner != jr.owner && time.Since(time.Unix(0, job.Heartbeat)) < jobStaleness {
		_ = txn.Rollback(ctx)
		time.Sleep(time.Second)
		return false, errJobBusy
	}
	job.Owner, job.Heartbeat = jr.owner, time.Now().UnixNano()

	start, end := t.prefix(), encoding.PrefixEnd(t.prefix())
	if job.Resume != nil {
		start = job.Resume
	}
	// Lock each row while its entry is written: a concurrent writer of the
	// same row waits, so the entry always matches the row's latest version.
	resp, err := txn.Scan(ctx, start, end, jobBatch, kvrpc.ReadOpts{Stmt: 1, Lock: kv.LockExclusive})
	if err != nil {
		return false, err
	}
	type ent struct{ key, value, pk []byte }
	var ents []ent
	var uniqueKeys [][]byte
	for i, v := range resp.Values {
		row, err := t.decodeRow(ctx, v)
		if err != nil {
			return true, err
		}
		k, val, uniq, err := sec.entry(ctx, row, resp.Keys[i])
		if err != nil {
			return true, err
		}
		ents = append(ents, ent{k, val, resp.Keys[i]})
		if uniq {
			uniqueKeys = append(uniqueKeys, k)
		}
	}
	existing := map[string][]byte{}
	if len(uniqueKeys) > 0 {
		got, err := txn.Get(ctx, uniqueKeys, kvrpc.ReadOpts{Stmt: 2, IncludeStmt: true, Lock: kv.LockExclusive})
		if err != nil {
			return false, err
		}
		for i, g := range got {
			if g != nil {
				existing[string(uniqueKeys[i])] = g
			}
		}
	}
	var muts []kv.Mutation
	for _, e := range ents {
		if len(e.value) > 0 { // unique, no NULLs
			cur, ok := existing[string(e.key)]
			if !ok {
				existing[string(e.key)] = e.value // a later row of this batch must not reuse it
			} else {
				if bytes.Equal(cur, e.value) {
					continue // a writer already indexed this row
				}
				_ = txn.Rollback(ctx)
				return true, s.failIndexJob(ctx, ti, ix, fmt.Sprintf("Duplicate entry for key '%s'", ix.Name))
			}
		}
		muts = append(muts, kv.Mutation{Op: kv.OpPut, Key: e.key, Value: e.value})
	}
	if err := txn.Mutate(ctx, 3, muts, time.Duration(0)); err != nil {
		return false, err
	}
	job.Rows += int64(len(resp.Keys))
	job.Resume = resp.Resume
	finished := len(resp.Resume) == 0
	b, _ := json.Marshal(job)
	if err := txn.Mutate(ctx, 4, []kv.Mutation{{Op: kv.OpPut, Key: jk, Value: b}}, 0); err != nil {
		return false, err
	}
	t.guard(txn)
	if err := txn.Commit(ctx); err != nil {
		return false, err
	}
	if !finished {
		return false, nil
	}
	// Every row is indexed: make it public and drop the job row.
	err = s.catalog.AlterTable(ctx, ti, func(txn *Txn, nt *TableInfo) error {
		if x := findIndex(nt, indexID); x != nil {
			x.State = ""
		}
		return txn.Mutate(ctx, 5, []kv.Mutation{{Op: kv.OpDelete, Key: jk}}, 0)
	})
	return err == nil, err
}

// failIndexJob removes an index whose backfill found duplicates.
func (s *Store) failIndexJob(ctx *sql.Context, ti *TableInfo, ix *IndexInfo, msg string) error {
	id := ix.ID
	err := s.catalog.AlterTable(ctx, ti, func(txn *Txn, nt *TableInfo) error {
		removeIndex(nt, id)
		return txn.Mutate(ctx, 5, []kv.Mutation{{Op: kv.OpDelete, Key: s.jobKey(ti.ID, id)}}, 0)
	})
	if err != nil {
		return err
	}
	return sql.ErrDuplicateEntry.New(msg)
}

func removeIndex(t *TableInfo, id uint32) {
	for i := range t.Indexes {
		if t.Indexes[i].ID == id {
			t.Indexes = append(t.Indexes[:i], t.Indexes[i+1:]...)
			return
		}
	}
}

// finishDrop removes a write-only index once every engine's lease has seen
// it stop being readable.
func (s *Store) finishDrop(ctx *sql.Context, ti *TableInfo, ix *IndexInfo) (bool, error) {
	wait := time.Until(time.Unix(0, ix.Since).Add(2 * CatalogLease))
	if wait > 0 {
		time.Sleep(wait)
	}
	id := ix.ID
	err := s.catalog.AlterTable(ctx, ti, func(_ *Txn, nt *TableInfo) error {
		removeIndex(nt, id)
		return nil
	})
	return err == nil, err
}

// ---- CREATE / DROP / RENAME INDEX --------------------------------------------------

func (t *Table) findIndex(name string) int {
	for i := range t.info.Indexes {
		if strings.EqualFold(t.info.Indexes[i].Name, name) {
			return i
		}
	}
	return -1
}

// CreateIndex adds the index write-only, backfills it, and makes it public.
func (t *Table) CreateIndex(ctx *sql.Context, def sql.IndexDef) error {
	switch def.Constraint {
	case sql.IndexConstraint_Fulltext, sql.IndexConstraint_Spatial, sql.IndexConstraint_Vector:
		return fmt.Errorf("FULLTEXT, SPATIAL and VECTOR indexes are not supported in v1")
	case sql.IndexConstraint_Primary:
		return fmt.Errorf("changing the primary key is not supported in v1")
	}
	info := IndexInfo{Name: def.Name, Unique: def.IsUnique(), Comment: def.Comment, State: IndexWriteOnly}
	for _, c := range def.Columns {
		ord := t.ordinal(c.Name)
		if ord < 0 {
			return sql.ErrKeyColumnDoesNotExist.New(c.Name)
		}
		col := t.schema.Schema[ord]
		if !keyable(col.Type) {
			return fmt.Errorf("column %s: type %s cannot be indexed in v1", col.Name, col.Type)
		}
		if c.Length > 0 && !(types.IsText(col.Type) || types.IsBinaryType(col.Type)) {
			return sql.ErrInvalidIndexPrefix.New(col.Name)
		}
		if c.Length == 0 && types.IsTextBlob(col.Type) {
			return sql.ErrInvalidBlobTextKey.New(col.Name)
		}
		info.Columns = append(info.Columns, IndexColumn{ColID: t.codecs[ord].info.ID, Prefix: int(c.Length)})
	}
	if info.Name == "" {
		info.Name = t.indexName(def.Columns[0].Name)
	}
	if strings.EqualFold(info.Name, "PRIMARY") || t.findIndex(info.Name) >= 0 {
		return sql.ErrDuplicateKey.New(info.Name)
	}
	err := t.cat.AlterTable(ctx, t.info, func(txn *Txn, ti *TableInfo) error {
		if ti.NextIndexID == 0 {
			ti.NextIndexID = 1
		}
		info.ID = ti.NextIndexID
		ti.NextIndexID++
		ti.Indexes = append(ti.Indexes, info)
		b, _ := json.Marshal(jobState{})
		return txn.Mutate(ctx, 1, []kv.Mutation{{Op: kv.OpPut, Key: t.store.jobKey(ti.ID, info.ID), Value: b}}, 0)
	})
	if err != nil {
		return err
	}
	select {
	case err := <-t.store.startIndexJob(t.info.ID, info.ID):
		return err
	case <-ctx.Done():
		return ctx.Err() // the job carries on in the background
	}
}

func (t *Table) ordinal(name string) int {
	for i, col := range t.schema.Schema {
		if strings.EqualFold(col.Name, name) {
			return i
		}
	}
	return -1
}

// indexName picks MySQL's default name for an index: its first column, then col_2, col_3, ...
func (t *Table) indexName(col string) string {
	name := col
	for n := 2; t.findIndex(name) >= 0 || strings.EqualFold(name, "PRIMARY"); n++ {
		name = fmt.Sprintf("%s_%d", col, n)
	}
	return name
}

// DropIndex makes the index write-only, waits until no engine reads it, and
// removes it. Its entries are left behind; index ids are never reused.
func (t *Table) DropIndex(ctx *sql.Context, name string) error {
	i := t.findIndex(name)
	if i < 0 {
		return sql.ErrIndexNotFound.New(name)
	}
	id := t.info.Indexes[i].ID
	err := t.cat.AlterTable(ctx, t.info, func(_ *Txn, ti *TableInfo) error {
		if x := findIndex(ti, id); x != nil {
			x.State, x.Dropping, x.Since = IndexWriteOnly, true, time.Now().UnixNano()
		}
		return nil
	})
	if err != nil {
		return err
	}
	return <-t.store.startIndexJob(t.info.ID, id)
}

func (t *Table) RenameIndex(ctx *sql.Context, from, to string) error {
	if t.findIndex(from) < 0 {
		return sql.ErrIndexNotFound.New(from)
	}
	if t.findIndex(to) >= 0 && !strings.EqualFold(from, to) {
		return sql.ErrDuplicateKey.New(to)
	}
	return t.cat.AlterTable(ctx, t.info, func(_ *Txn, ti *TableInfo) error {
		ti.Indexes[(&Table{info: ti}).findIndex(from)].Name = to
		return nil
	})
}

// ---- ADD / DROP / MODIFY COLUMN ------------------------------------------------------

var _ sql.AlterableTable = (*Table)(nil)

func insertColumn(cols []ColumnInfo, ci ColumnInfo, order *sql.ColumnOrder) []ColumnInfo {
	at := len(cols)
	if order != nil {
		if order.First {
			at = 0
		} else if order.AfterColumn != "" {
			for i := range cols {
				if strings.EqualFold(cols[i].Name, order.AfterColumn) {
					at = i + 1
				}
			}
		}
	}
	cols = append(cols, ColumnInfo{})
	copy(cols[at+1:], cols[at:])
	cols[at] = ci
	return cols
}

// AddColumn is instant (D3): existing rows lack the new column id, which
// reads as the column's original default — its default at this moment.
func (t *Table) AddColumn(ctx *sql.Context, col *sql.Column, order *sql.ColumnOrder) error {
	if col.PrimaryKey {
		return fmt.Errorf("adding a primary-key column is not supported in v1")
	}
	if t.ordinal(col.Name) >= 0 {
		return sql.ErrColumnExists.New(col.Name)
	}
	var orig any
	if col.Default != nil {
		v, err := col.Default.Eval(ctx, nil)
		if err != nil {
			return err
		}
		orig = v
	} else if !col.Nullable {
		orig = col.Type.Zero() // MySQL's implicit default for NOT NULL
	}
	var updated *TableInfo
	err := t.cat.AlterTable(ctx, t.info, func(_ *Txn, ti *TableInfo) error {
		updated = ti
		ci, err := columnInfo(col, ti.NextColumnID)
		if err != nil {
			return err
		}
		ti.NextColumnID++
		if orig != nil {
			cv, _, err := col.Type.Convert(ctx, orig)
			if err != nil {
				return err
			}
			b, err := (&colCodec{info: &ci, typ: col.Type}).encodeValue(ctx, cv)
			if err != nil {
				return err
			}
			ci.OriginalDefault = b
		}
		ti.Columns = insertColumn(ti.Columns, ci, order)
		return nil
	})
	if err != nil {
		return err
	}
	// go-mysql-server follows AddColumn with a pass that rewrites every row
	// with the default, through this same table object. Rows already read the
	// original default, so give this object the new schema and no rows: the
	// change stays instant.
	nt, err := newTable(ctx, t.cat, updated.clone())
	if err != nil {
		return err
	}
	*t = *nt
	t.instantAdd = true
	return nil
}

// DropColumn is instant: rows keep the value, which is no longer read.
func (t *Table) DropColumn(ctx *sql.Context, name string) error {
	ord := t.ordinal(name)
	if ord < 0 {
		return sql.ErrTableColumnNotFound.New(t.info.Name, name)
	}
	id := t.codecs[ord].info.ID
	for _, pk := range t.info.PK {
		if pk == id {
			return fmt.Errorf("dropping a primary-key column is not supported in v1")
		}
	}
	for _, ix := range t.info.Indexes {
		for _, c := range ix.Columns {
			if c.ColID == id {
				return fmt.Errorf("column %s is used by index %s; drop the index first", name, ix.Name)
			}
		}
	}
	if len(t.info.Columns) == 1 {
		return sql.ErrCantDropFieldOrKey.New(name)
	}
	return t.cat.AlterTable(ctx, t.info, func(_ *Txn, ti *TableInfo) error {
		for i := range ti.Columns {
			if ti.Columns[i].ID == id {
				ti.Columns = append(ti.Columns[:i], ti.Columns[i+1:]...)
				break
			}
		}
		return nil
	})
}

// ModifyColumn supports changes that keep every stored row and key valid:
// renames, moves, defaults, comments, dropping NOT NULL, and widening
// integers or string lengths. Anything that would need a rewrite is refused.
func (t *Table) ModifyColumn(ctx *sql.Context, name string, col *sql.Column, order *sql.ColumnOrder) error {
	ord := t.ordinal(name)
	if ord < 0 {
		return sql.ErrTableColumnNotFound.New(t.info.Name, name)
	}
	old := t.schema.Schema[ord]
	if !compatibleType(old.Type, col.Type) {
		return fmt.Errorf("changing column %s from %s to %s needs a table rewrite, which v1 does not support", name, old.Type, col.Type)
	}
	if old.Nullable && !col.Nullable {
		return fmt.Errorf("making column %s NOT NULL needs a scan of existing rows, which v1 does not support", name)
	}
	if !strings.EqualFold(old.Name, col.Name) && t.ordinal(col.Name) >= 0 {
		return sql.ErrColumnExists.New(col.Name)
	}
	id := t.codecs[ord].info.ID
	return t.cat.AlterTable(ctx, t.info, func(_ *Txn, ti *TableInfo) error {
		for i := range ti.Columns {
			if ti.Columns[i].ID != id {
				continue
			}
			ci, err := columnInfo(col, id)
			if err != nil {
				return err
			}
			ci.OriginalDefault = ti.Columns[i].OriginalDefault
			ti.Columns = append(ti.Columns[:i], ti.Columns[i+1:]...)
			if order == nil {
				order = &sql.ColumnOrder{}
				if i == 0 {
					order.First = true
				} else {
					order.AfterColumn = ti.Columns[i-1].Name
				}
			}
			ti.Columns = insertColumn(ti.Columns, ci, order)
			return nil
		}
		return sql.ErrTableColumnNotFound.New(ti.Name, name)
	})
}

// compatibleType reports whether values and keys stored for from read the
// same under to: integers keep 8-byte encodings, strings keep raw bytes.
func compatibleType(from, to sql.Type) bool {
	if from.String() == to.String() {
		return true
	}
	switch {
	case types.IsSigned(from) && types.IsSigned(to):
		return integerRank(to) >= integerRank(from)
	case types.IsUnsigned(from) && types.IsUnsigned(to):
		return integerRank(to) >= integerRank(from)
	}
	fs, ok1 := from.(sql.StringType)
	ts, ok2 := to.(sql.StringType)
	if ok1 && ok2 && fs.Collation().Name() == ts.Collation().Name() && types.IsBinaryType(from) == types.IsBinaryType(to) {
		return ts.MaxCharacterLength() >= fs.MaxCharacterLength() && fs.Type() == ts.Type()
	}
	return false
}

func integerRank(t sql.Type) int {
	switch t {
	case types.Boolean, types.Int8, types.Uint8:
		return 1
	case types.Int16, types.Uint16:
		return 2
	case types.Int24, types.Uint24:
		return 3
	case types.Int32, types.Uint32:
		return 4
	}
	return 5
}

// ---- statistics (system table 2) --------------------------------------------------

var _ sql.StatisticsTable = (*Table)(nil)

// RowCount is the last analyzed row count (an estimate).
func (t *Table) RowCount(*sql.Context) (uint64, bool, error) {
	st, ok := t.cat.Stats(t.info.ID)
	return st.Rows, ok && false, nil
}

func (t *Table) DataLength(*sql.Context) (uint64, error) {
	st, _ := t.cat.Stats(t.info.ID)
	return st.Bytes, nil
}

// analyze counts a table's rows and bytes and stores them in system table 2.
// It runs as background work: its requests don't count as activity, and it
// gives up instead of waking a suspended tenant.
func (s *Store) analyze(ctx context.Context, t *TableInfo) error {
	txn := s.NewTxn(kv.ReadCommitted, false)
	txn.background = true
	defer txn.Rollback(ctx)
	prefix := encoding.Prefix(s.partition, t.ID, encoding.PrimaryIndexID)
	start, end := prefix, encoding.PrefixEnd(prefix)
	var st TableStats
	for {
		resp, err := txn.Scan(ctx, start, end, 1000, kvrpc.ReadOpts{Stmt: 1})
		if err != nil {
			return err
		}
		st.Rows += uint64(len(resp.Keys))
		for i := range resp.Keys {
			st.Bytes += uint64(len(resp.Keys[i]) + len(resp.Values[i]))
		}
		if len(resp.Resume) == 0 {
			break
		}
		start = resp.Resume
	}
	st.Analyzed = time.Now().Unix()
	b, _ := json.Marshal(st)
	if err := txn.Mutate(ctx, 2, []kv.Mutation{{Op: kv.OpPut, Key: s.statsKey(t.ID), Value: b}}, 0); err != nil {
		return err
	}
	if err := txn.Commit(ctx); err != nil {
		return err
	}
	s.catalog.mu.Lock()
	if s.catalog.stats == nil {
		s.catalog.stats = map[uint32]TableStats{}
	}
	s.catalog.stats[t.ID] = st
	s.catalog.mu.Unlock()
	return nil
}

// noteWrites counts row writes per table and starts a background ANALYZE
// once a table has changed by 20% (and at least 1000 rows) since the last one.
func (s *Store) noteWrites(t *TableInfo, n int) {
	s.statsMu.Lock()
	if s.changed == nil {
		s.changed = map[uint32]int{}
		s.analyzing = map[uint32]bool{}
	}
	s.changed[t.ID] += n
	st, _ := s.catalog.Stats(t.ID)
	due := s.changed[t.ID] >= 1000 && uint64(s.changed[t.ID])*5 >= st.Rows && !s.analyzing[t.ID]
	if due {
		s.changed[t.ID] = 0
		s.analyzing[t.ID] = true
	}
	s.statsMu.Unlock()
	if due {
		go func() {
			_ = s.analyze(context.Background(), t)
			s.statsMu.Lock()
			s.analyzing[t.ID] = false
			s.statsMu.Unlock()
		}()
	}
}

// statsProvider wraps go-mysql-server's in-memory statistics so that ANALYZE
// TABLE also stores the row count in system table 2.
type statsProvider struct {
	sql.StatsProvider
}

func (p *statsProvider) AnalyzeTable(ctx *sql.Context, table sql.Table, db string) error {
	if err := p.histograms(ctx, table, db); err != nil {
		return err
	}
	if t, ok := sql.GetUnderlyingTable(table).(*Table); ok {
		return t.store.analyze(ctx, t.info)
	}
	return nil
}

// histograms runs go-mysql-server's sampling, which divides by zero on an
// empty table and panics comparing []byte keys (BLOB, VARBINARY); found by
// MySQL's test suite (issue #17). Such a table gets no histogram, as one
// that was never analyzed, and its row count is still stored.
func (p *statsProvider) histograms(ctx *sql.Context, table sql.Table, db string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = nil
		}
	}()
	return p.StatsProvider.AnalyzeTable(ctx, table, db)
}
