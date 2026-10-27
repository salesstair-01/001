package sqlengine

import (
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/vitess/go/mysql"
)

// Temporary tables (CREATE TEMPORARY TABLE, issue #22) live in the query
// engine's memory, in the session that created them: they are never written
// to the KV store, and other sessions don't see them. A temporary table
// shadows a permanent table of the same name and goes away with the session
// (a reconnect to another query engine loses it, as a MySQL reconnect does).
// As with InnoDB temporary tables, a failed statement and ROLLBACK undo their
// rows; CREATE and DROP are not transactional. Primary and unique keys are
// enforced under the columns' collations, with the KV tables' key encoding.
// Secondary indexes are recorded (for uniqueness and duplicate checks) but
// reads scan the rows.

type tempKey struct{ db, name string }

func tempKeyOf(db, name string) tempKey {
	return tempKey{strings.ToLower(db), strings.ToLower(name)}
}

// tempTables is a session's temporary tables.
type tempTables struct {
	mu     sync.Mutex
	tables map[tempKey]*tempTable
}

func sessionTemps(ctx *sql.Context) *tempTables {
	if s, ok := ctx.Session.(*Session); ok {
		return &s.temps
	}
	return nil
}

func (ts *tempTables) get(db, name string) *tempTable {
	if ts == nil {
		return nil
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.tables[tempKeyOf(db, name)]
}

func (ts *tempTables) list(db string) []*tempTable {
	if ts == nil {
		return nil
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	var out []*tempTable
	for k, t := range ts.tables {
		if k.db == strings.ToLower(db) {
			out = append(out, t)
		}
	}
	return out
}

// endTransaction keeps (commit) or undoes (rollback) every temporary
// table's changes since the transaction began.
func (ts *tempTables) endTransaction(commit bool) {
	ts.mu.Lock()
	tables := make([]*tempTable, 0, len(ts.tables))
	for _, t := range ts.tables {
		tables = append(tables, t)
	}
	ts.mu.Unlock()
	for _, t := range tables {
		t.mu.Lock()
		if !commit {
			t.undoTo(0)
		}
		t.undo = t.undo[:0]
		t.mu.Unlock()
	}
}

// ---- the database side ----------------------------------------------------------

var (
	_ sql.TemporaryTableCreator  = (*Database)(nil)
	_ sql.TemporaryTableDatabase = (*Database)(nil)
)

func (d *Database) CreateTemporaryTable(ctx *sql.Context, name string, schema sql.PrimaryKeySchema, collation sql.CollationID) error {
	ts := sessionTemps(ctx)
	if ts == nil {
		return sql.ErrTemporaryTableNotSupported.New()
	}
	if !collationSupported(collation) {
		return fmt.Errorf("collation %s is not supported in v1", collation.Name())
	}
	t := &tempTable{db: d.name, name: name, sch: schema, collation: collation, autoNext: 1}
	for i, col := range schema.Schema {
		ci, err := columnInfo(col, uint32(i+1))
		if err != nil {
			return err
		}
		t.codecs = append(t.codecs, colCodec{info: &ci, typ: col.Type})
		if col.AutoIncrement {
			t.autoOrd = i
		}
	}
	t.pk = schema.PkOrdinals
	t.rebuild()
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.tables == nil {
		ts.tables = map[tempKey]*tempTable{}
	}
	k := tempKeyOf(d.name, name)
	if _, ok := ts.tables[k]; ok {
		return errTableExists(name)
	}
	ts.tables[k] = t
	return nil
}

func (d *Database) GetAllTemporaryTables(ctx *sql.Context) ([]sql.Table, error) {
	var out []sql.Table
	for _, t := range sessionTemps(ctx).list(d.name) {
		out = append(out, t)
	}
	return out, nil
}

// dropTemp drops a temporary table; ok is false if there is none.
func (d *Database) dropTemp(ctx *sql.Context, name string) bool {
	ts := sessionTemps(ctx)
	if ts == nil {
		return false
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	k := tempKeyOf(d.name, name)
	if _, ok := ts.tables[k]; !ok {
		return false
	}
	delete(ts.tables, k)
	return true
}

func (d *Database) renameTemp(ctx *sql.Context, oldName, newName string) (bool, error) {
	ts := sessionTemps(ctx)
	if ts == nil {
		return false, nil
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	t, ok := ts.tables[tempKeyOf(d.name, oldName)]
	if !ok {
		return false, nil
	}
	if _, taken := ts.tables[tempKeyOf(d.name, newName)]; taken {
		return true, errTableExists(newName)
	}
	delete(ts.tables, tempKeyOf(d.name, oldName))
	t.name = newName
	ts.tables[tempKeyOf(d.name, newName)] = t
	return true, nil
}

// ---- the table ------------------------------------------------------------------

type tempIndex struct {
	def    sql.IndexDef
	ords   []int
	prefix []int
	keys   map[string]int // unique indexes: key -> row slot
}

type tempUndo struct {
	slot int
	old  sql.Row // nil: the slot was empty
}

type tempTable struct {
	db, name  string
	sch       sql.PrimaryKeySchema
	collation sql.CollationID
	codecs    []colCodec
	pk        []int
	autoOrd   int

	mu       sync.Mutex
	rows     []sql.Row // nil: deleted
	live     int
	pkKeys   map[string]int   // key -> slot, when there is a primary key
	rowSlots map[string][]int // all columns -> slots, when there is none
	indexes  []*tempIndex
	autoNext uint64
	undo     []tempUndo // since the transaction began
	checks   []CheckInfo
}

var (
	_ sql.Table               = (*tempTable)(nil)
	_ sql.TemporaryTable      = (*tempTable)(nil)
	_ sql.PrimaryKeyTable     = (*tempTable)(nil)
	_ sql.InsertableTable     = (*tempTable)(nil)
	_ sql.UpdatableTable      = (*tempTable)(nil)
	_ sql.DeletableTable      = (*tempTable)(nil)
	_ sql.ReplaceableTable    = (*tempTable)(nil)
	_ sql.TruncateableTable   = (*tempTable)(nil)
	_ sql.IndexAlterableTable = (*tempTable)(nil)
	_ sql.AutoIncrementTable  = (*tempTable)(nil)
	_ sql.CommentedTable      = (*tempTable)(nil)
)

func (t *tempTable) Name() string                           { return t.name }
func (t *tempTable) String() string                         { return t.name }
func (t *tempTable) Schema() sql.Schema                     { return t.sch.Schema }
func (t *tempTable) PrimaryKeySchema() sql.PrimaryKeySchema { return t.sch }
func (t *tempTable) Collation() sql.CollationID             { return t.collation }
func (t *tempTable) Comment() string                        { return "" }
func (t *tempTable) IsTemporary() bool                      { return true }

func (t *tempTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return &tempPartitions{}, nil
}

func (t *tempTable) PartitionRows(*sql.Context, sql.Partition) (sql.RowIter, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	rows := make([]sql.Row, 0, t.live)
	for _, r := range t.rows {
		if r != nil {
			rows = append(rows, r.Copy())
		}
	}
	return sql.RowsToRowIter(rows...), nil
}

type tempPartitions struct{ done bool }

func (p *tempPartitions) Next(*sql.Context) (sql.Partition, error) {
	if p.done {
		return nil, io.EOF
	}
	p.done = true
	return tempPartition{}, nil
}
func (p *tempPartitions) Close(*sql.Context) error { return nil }

type tempPartition struct{}

func (tempPartition) Key() []byte { return []byte("temp") }

// key encodes the given columns of a row, under their collations.
func (t *tempTable) key(ctx *sql.Context, row sql.Row, ords, prefix []int) (string, bool, error) {
	var b []byte
	hasNull := false
	for i, ord := range ords {
		v := row[ord]
		if v == nil {
			hasNull = true
		}
		if prefix != nil {
			v = truncate(t.codecs[ord].typ, v, prefix[i])
		}
		var err error
		if b, err = t.codecs[ord].appendKey(ctx, b, v); err != nil {
			return "", false, err
		}
	}
	return string(b), hasNull, nil
}

// rowKey identifies a stored row: its primary key, or all its columns.
func (t *tempTable) rowKey(ctx *sql.Context, row sql.Row) (string, error) {
	ords := t.pk
	if len(ords) == 0 {
		ords = make([]int, len(t.codecs))
		for i := range ords {
			ords[i] = i
		}
	}
	k, _, err := t.key(ctx, row, ords, nil)
	return k, err
}

// rebuild recomputes the key maps from the rows (after an undo, or a new
// index). Keys that can't be encoded are skipped.
func (t *tempTable) rebuild() {
	ctx := sql.NewEmptyContext()
	t.pkKeys, t.rowSlots = nil, nil
	if len(t.pk) > 0 {
		t.pkKeys = map[string]int{}
	} else {
		t.rowSlots = map[string][]int{}
	}
	for _, ix := range t.indexes {
		ix.keys = nil
		if ix.def.IsUnique() {
			ix.keys = map[string]int{}
		}
	}
	t.live = 0
	for slot, r := range t.rows {
		if r != nil {
			t.addKeys(ctx, slot, r)
		}
	}
}

// addKeys and removeKeys keep the key maps up to date for a row in slot.
func (t *tempTable) addKeys(ctx *sql.Context, slot int, r sql.Row) {
	t.live++
	if t.pkKeys != nil {
		if k, _, err := t.key(ctx, r, t.pk, nil); err == nil {
			t.pkKeys[k] = slot
		}
	} else if k, err := t.rowKey(ctx, r); err == nil {
		t.rowSlots[k] = append(t.rowSlots[k], slot)
	}
	for _, ix := range t.indexes {
		if ix.keys == nil {
			continue
		}
		if k, null, err := t.key(ctx, r, ix.ords, ix.prefix); err == nil && !null {
			ix.keys[k] = slot
		}
	}
}

func (t *tempTable) removeKeys(ctx *sql.Context, slot int, r sql.Row) {
	t.live--
	if t.pkKeys != nil {
		if k, _, err := t.key(ctx, r, t.pk, nil); err == nil && t.pkKeys[k] == slot {
			delete(t.pkKeys, k)
		}
	} else if k, err := t.rowKey(ctx, r); err == nil {
		slots := t.rowSlots[k]
		for i, s := range slots {
			if s == slot {
				slots = append(slots[:i], slots[i+1:]...)
				break
			}
		}
		if len(slots) == 0 {
			delete(t.rowSlots, k)
		} else {
			t.rowSlots[k] = slots
		}
	}
	for _, ix := range t.indexes {
		if ix.keys == nil {
			continue
		}
		if k, null, err := t.key(ctx, r, ix.ords, ix.prefix); err == nil && !null && ix.keys[k] == slot {
			delete(ix.keys, k)
		}
	}
}

func (t *tempTable) undoTo(n int) {
	for i := len(t.undo) - 1; i >= n; i-- {
		u := t.undo[i]
		t.rows[u.slot] = u.old
	}
	t.undo = t.undo[:n]
	for len(t.rows) > 0 && t.rows[len(t.rows)-1] == nil {
		t.rows = t.rows[:len(t.rows)-1]
	}
	t.rebuild()
}

// check returns the error for a row that would repeat a primary or unique
// key of another row than skip.
func (t *tempTable) check(ctx *sql.Context, row sql.Row, skip int) error {
	if t.pkKeys != nil {
		k, _, err := t.key(ctx, row, t.pk, nil)
		if err != nil {
			return err
		}
		if slot, ok := t.pkKeys[k]; ok && slot != skip {
			return sql.NewUniqueKeyErr(dupEntry(row, t.pk, t.name, "PRIMARY"), true, t.rows[slot])
		}
	}
	for _, ix := range t.indexes {
		if ix.keys == nil {
			continue
		}
		k, null, err := t.key(ctx, row, ix.ords, ix.prefix)
		if err != nil {
			return err
		}
		if slot, ok := ix.keys[k]; ok && !null && slot != skip {
			return sql.NewUniqueKeyErr(dupEntry(row, ix.ords, t.name, ix.def.Name), false, t.rows[slot])
		}
	}
	return nil
}

func (t *tempTable) find(ctx *sql.Context, row sql.Row) (int, error) {
	if t.pkKeys != nil {
		k, _, err := t.key(ctx, row, t.pk, nil)
		if err != nil {
			return -1, err
		}
		if slot, ok := t.pkKeys[k]; ok {
			return slot, nil
		}
		return -1, nil
	}
	k, err := t.rowKey(ctx, row)
	if err != nil {
		return -1, err
	}
	if slots := t.rowSlots[k]; len(slots) > 0 {
		return slots[0], nil
	}
	return -1, nil
}

func (t *tempTable) set(slot int, row sql.Row) {
	t.undo = append(t.undo, tempUndo{slot: slot, old: t.rows[slot]})
	t.rows[slot] = row
}

// repertoire refuses strings a column's character set can't hold, as the
// KV tables do (checkRepertoire).
func (t *tempTable) repertoire(row sql.Row) error {
	for i, v := range row {
		var b []byte
		switch x := v.(type) {
		case string:
			b = []byte(x)
		case []byte:
			b = x
		default:
			continue
		}
		if err := checkRepertoire(t.codecs[i].typ, t.sch.Schema[i].Name, b); err != nil {
			return err
		}
	}
	return nil
}

func (t *tempTable) insert(ctx *sql.Context, row sql.Row) error {
	if err := t.repertoire(row); err != nil {
		return err
	}
	if err := t.check(ctx, row, -1); err != nil {
		return err
	}
	t.rows = append(t.rows, nil)
	slot := len(t.rows) - 1
	t.set(slot, row.Copy())
	t.addKeys(ctx, slot, t.rows[slot])
	return nil
}

func (t *tempTable) delete(ctx *sql.Context, row sql.Row) error {
	slot, err := t.find(ctx, row)
	if err != nil || slot < 0 {
		return err
	}
	t.removeKeys(ctx, slot, t.rows[slot])
	t.set(slot, nil)
	return nil
}

func (t *tempTable) update(ctx *sql.Context, old, new sql.Row) error {
	slot, err := t.find(ctx, old)
	if err != nil || slot < 0 {
		return err
	}
	if err := t.repertoire(new); err != nil {
		return err
	}
	if err := t.check(ctx, new, slot); err != nil {
		return err
	}
	t.removeKeys(ctx, slot, t.rows[slot])
	t.set(slot, new.Copy())
	t.addKeys(ctx, slot, t.rows[slot])
	return nil
}

func (t *tempTable) Inserter(*sql.Context) sql.RowInserter { return &tempEditor{t: t} }
func (t *tempTable) Updater(*sql.Context) sql.RowUpdater   { return &tempEditor{t: t} }
func (t *tempTable) Deleter(*sql.Context) sql.RowDeleter   { return &tempEditor{t: t} }
func (t *tempTable) Replacer(*sql.Context) sql.RowReplacer { return &tempEditor{t: t} }

func (t *tempTable) Truncate(*sql.Context) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := t.live
	t.rows, t.undo, t.autoNext = nil, nil, 1
	t.rebuild()
	return n, nil
}

// tempEditor is one statement's edits; a failed statement's are undone.
type tempEditor struct {
	t    *tempTable
	mark int
}

func (e *tempEditor) StatementBegin(*sql.Context) {
	e.t.mu.Lock()
	e.mark = len(e.t.undo)
	e.t.mu.Unlock()
}

func (e *tempEditor) DiscardChanges(*sql.Context, error) error {
	e.t.mu.Lock()
	e.t.undoTo(e.mark)
	e.t.mu.Unlock()
	return nil
}

func (e *tempEditor) StatementComplete(*sql.Context) error { return nil }
func (e *tempEditor) Close(*sql.Context) error             { return nil }

func (e *tempEditor) Insert(ctx *sql.Context, row sql.Row) error {
	e.t.mu.Lock()
	defer e.t.mu.Unlock()
	return e.t.insert(ctx, row)
}

func (e *tempEditor) Delete(ctx *sql.Context, row sql.Row) error {
	e.t.mu.Lock()
	defer e.t.mu.Unlock()
	return e.t.delete(ctx, row)
}

func (e *tempEditor) Update(ctx *sql.Context, old, new sql.Row) error {
	e.t.mu.Lock()
	defer e.t.mu.Unlock()
	return e.t.update(ctx, old, new)
}

// ---- indexes and AUTO_INCREMENT ------------------------------------------------------

func (t *tempTable) CreateIndex(ctx *sql.Context, def sql.IndexDef) error {
	if def.IsFullText() || def.IsSpatial() {
		return fmt.Errorf("FULLTEXT and SPATIAL indexes are not supported in v1")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	ix := &tempIndex{def: def}
	for _, c := range def.Columns {
		ord := t.sch.Schema.IndexOfColName(c.Name)
		if ord < 0 {
			return sql.ErrKeyColumnDoesNotExist.New(c.Name)
		}
		ix.ords = append(ix.ords, ord)
		ix.prefix = append(ix.prefix, int(c.Length))
	}
	for _, other := range t.indexes {
		if strings.EqualFold(other.def.Name, def.Name) {
			return sql.ErrDuplicateKey.New(def.Name)
		}
	}
	t.indexes = append(t.indexes, ix)
	t.rebuild()
	if ix.keys != nil && len(ix.keys) < t.countNonNull(ctx, ix) {
		t.indexes = t.indexes[:len(t.indexes)-1]
		t.rebuild()
		return sql.NewUniqueKeyErr(def.Name, false, nil)
	}
	return nil
}

func (t *tempTable) countNonNull(ctx *sql.Context, ix *tempIndex) int {
	n := 0
	for _, r := range t.rows {
		if r == nil {
			continue
		}
		if _, null, err := t.key(ctx, r, ix.ords, ix.prefix); err == nil && !null {
			n++
		}
	}
	return n
}

func (t *tempTable) DropIndex(_ *sql.Context, name string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	for i, ix := range t.indexes {
		if strings.EqualFold(ix.def.Name, name) {
			t.indexes = append(t.indexes[:i], t.indexes[i+1:]...)
			return nil
		}
	}
	return sql.ErrIndexNotFound.New(name)
}

func (t *tempTable) RenameIndex(_ *sql.Context, from, to string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, ix := range t.indexes {
		if strings.EqualFold(ix.def.Name, from) {
			ix.def.Name = to
			return nil
		}
	}
	return sql.ErrIndexNotFound.New(from)
}

func (t *tempTable) PeekNextAutoIncrementValue(*sql.Context) (uint64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.autoNext, nil
}

func (t *tempTable) GetNextAutoIncrementValue(ctx *sql.Context, insertVal interface{}) (uint64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if insertVal != nil {
		n, err := toUint64(ctx, t.codecs[t.autoOrd].typ, insertVal)
		if err != nil {
			return 0, err
		}
		if n >= t.autoNext {
			t.autoNext = n + 1
		}
		return n, nil
	}
	n := t.autoNext
	t.autoNext++
	return n, nil
}

func (t *tempTable) AutoIncrementSetter(*sql.Context) sql.AutoIncrementSetter {
	return tempAutoInc{t}
}

type tempAutoInc struct{ t *tempTable }

func (a tempAutoInc) SetAutoIncrementValue(_ *sql.Context, v uint64) error {
	a.t.mu.Lock()
	defer a.t.mu.Unlock()
	a.t.autoNext = v
	return nil
}

func (a tempAutoInc) AcquireAutoIncrementLock(*sql.Context) (func(), error) {
	return func() {}, nil
}

func (a tempAutoInc) Close(*sql.Context) error { return nil }

// GetIndexes describes the primary key and the indexes (for SHOW CREATE
// TABLE, SHOW INDEX, duplicate checks and the planner); a lookup on any of
// them scans the rows (IndexedAccess).
func (t *tempTable) GetIndexes(*sql.Context) ([]sql.Index, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []sql.Index
	if len(t.pk) > 0 {
		out = append(out, &tempIndexDesc{t: t, id: "PRIMARY", ords: t.pk, unique: true})
	}
	for _, ix := range t.indexes {
		out = append(out, &tempIndexDesc{t: t, id: ix.def.Name, ords: ix.ords, prefix: ix.prefix, unique: ix.def.IsUnique(), comment: ix.def.Comment})
	}
	return out, nil
}

func (t *tempTable) IndexedAccess(*sql.Context, sql.IndexLookup) sql.IndexedTable {
	return t.indexedAccess()
}
func (t *tempTable) PreciseMatch() bool { return false }

var _ sql.IndexAddressableTable = (*tempTable)(nil)

type tempIndexDesc struct {
	t       *tempTable
	id      string
	ords    []int
	prefix  []int
	unique  bool
	comment string
}

func (i *tempIndexDesc) ID() string       { return i.id }
func (i *tempIndexDesc) Database() string { return i.t.db }
func (i *tempIndexDesc) Table() string    { return i.t.name }
func (i *tempIndexDesc) Expressions() []string {
	out := make([]string, len(i.ords))
	for n, ord := range i.ords {
		out[n] = strings.ToLower(i.t.name) + "." + strings.ToLower(i.t.sch.Schema[ord].Name)
	}
	return out
}
func (i *tempIndexDesc) IsUnique() bool                             { return i.unique }
func (i *tempIndexDesc) IsSpatial() bool                            { return false }
func (i *tempIndexDesc) IsFullText() bool                           { return false }
func (i *tempIndexDesc) IsVector() bool                             { return false }
func (i *tempIndexDesc) Comment() string                            { return i.comment }
func (i *tempIndexDesc) IndexType() string                          { return "BTREE" }
func (i *tempIndexDesc) IsGenerated() bool                          { return false }
func (i *tempIndexDesc) CanSupport(*sql.Context, ...sql.Range) bool { return true }
func (i *tempIndexDesc) CanSupportOrderBy(sql.Expression) bool      { return false }

func (i *tempIndexDesc) ColumnExpressionTypes() []sql.ColumnExpressionType {
	exprs := i.Expressions()
	out := make([]sql.ColumnExpressionType, len(exprs))
	for n, ord := range i.ords {
		out[n] = sql.ColumnExpressionType{Expression: exprs[n], Type: i.t.sch.Schema[ord].Type}
	}
	return out
}

func (i *tempIndexDesc) PrefixLengths() []uint16 {
	var out []uint16
	any := false
	for _, p := range i.prefix {
		out = append(out, uint16(p))
		any = any || p > 0
	}
	if !any {
		return nil
	}
	return out
}

// go-mysql-server parses DROP TEMPORARY TABLE as DROP TABLE, so it dropped a
// permanent table of that name. A pre-analyze rule keeps only temporary
// tables: another table is MySQL's error 1051, or a note with IF EXISTS.

const dropTemporaryID analyzer.RuleId = 10008

var dropTemporaryStmt = regexp.MustCompile(`(?is)^(\s+|/\*.*?\*/)*drop\s+temporary\s+tables?\s`)

func dropTemporary(ctx *sql.Context, a *analyzer.Analyzer, n sql.Node, scope *plan.Scope, sel analyzer.RuleSelector, qf *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	d, ok := n.(*plan.DropTable)
	if !ok || !dropTemporaryStmt.MatchString(ctx.Query()) {
		return n, transform.SameTree, nil
	}
	var keep []sql.Node
	for _, t := range d.Tables {
		if isTempTableNode(t) {
			keep = append(keep, t)
			continue
		}
		name := ""
		if nt, ok := t.(sql.Nameable); ok {
			name = nt.Name()
		}
		if rt, ok := t.(*plan.ResolvedTable); ok && rt.SqlDatabase != nil {
			name = rt.SqlDatabase.Name() + "." + name
		} else if db := ctx.GetCurrentDatabase(); db != "" && !strings.Contains(name, ".") {
			name = db + "." + name
		}
		if !d.IfExists() {
			return nil, transform.SameTree, mysql.NewSQLError(1051, "42S02", "Unknown table '%s'", name)
		}
		ctx.Session.Warn(&sql.Warning{Level: "Note", Code: 1051, Message: fmt.Sprintf("Unknown table '%s'", name)})
	}
	if len(keep) == len(d.Tables) {
		return n, transform.SameTree, nil
	}
	return plan.NewDropTable(keep, d.IfExists()), transform.NewTree, nil
}

func isTempTableNode(n sql.Node) bool {
	rt, ok := n.(*plan.ResolvedTable)
	if !ok {
		return false
	}
	_, ok = sql.GetUnderlyingTable(rt.Table).(*tempTable)
	return ok
}

func errTableExists(name string) error {
	return mysql.NewSQLError(1050, "42S01", "Table '%s' already exists", name)
}

// ---- ALTER TABLE ------------------------------------------------------------------
//
// A temporary table is rewritable: for every ALTER TABLE (columns, primary
// key), go-mysql-server streams its rows, in the new schema, into
// RewriteInserter, which builds the new table and replaces the old one on
// Close. A new primary key that repeats a value fails as in MySQL (1062).
// ALTER TABLE is not transactional.

var (
	_                      sql.RewritableTable          = (*tempTable)(nil)
	_                      sql.PrimaryKeyAlterableTable = (*tempTable)(nil)
	errTempAlterViaRewrite                              = fmt.Errorf("temporary tables are altered by rewriting")
)

func (t *tempTable) AddColumn(*sql.Context, *sql.Column, *sql.ColumnOrder) error {
	return errTempAlterViaRewrite
}
func (t *tempTable) DropColumn(*sql.Context, string) error { return errTempAlterViaRewrite }
func (t *tempTable) ModifyColumn(*sql.Context, string, *sql.Column, *sql.ColumnOrder) error {
	return errTempAlterViaRewrite
}
func (t *tempTable) CreatePrimaryKey(*sql.Context, []sql.IndexColumn) error {
	return errTempAlterViaRewrite
}
func (t *tempTable) DropPrimaryKey(*sql.Context) error { return errTempAlterViaRewrite }

func (t *tempTable) ShouldRewriteTable(*sql.Context, sql.PrimaryKeySchema, sql.PrimaryKeySchema, *sql.Column, *sql.Column) bool {
	return true
}

func (t *tempTable) RewriteInserter(ctx *sql.Context, oldSchema, newSchema sql.PrimaryKeySchema, oldColumn, newColumn *sql.Column, idxCols []sql.IndexColumn) (sql.RowInserter, error) {
	n := &tempTable{db: t.db, name: t.name, sch: newSchema, collation: t.collation, pk: newSchema.PkOrdinals, autoNext: t.autoNext}
	for i, col := range newSchema.Schema {
		ci, err := columnInfo(col, uint32(i+1))
		if err != nil {
			return nil, err
		}
		n.codecs = append(n.codecs, colCodec{info: &ci, typ: col.Type})
		if col.AutoIncrement {
			n.autoOrd = i
		}
	}
	t.mu.Lock()
	indexes := t.indexes
	t.mu.Unlock()
	// Keep the indexes whose columns are all still there (renamed as the
	// modified column is).
	for _, ix := range indexes {
		nix := &tempIndex{def: ix.def}
		nix.def.Columns = nil
		ok := true
		for i, c := range ix.def.Columns {
			name := c.Name
			if oldColumn != nil && newColumn != nil && strings.EqualFold(name, oldColumn.Name) {
				name = newColumn.Name
			}
			ord := newSchema.Schema.IndexOfColName(name)
			if ord < 0 {
				ok = false
				break
			}
			nix.def.Columns = append(nix.def.Columns, sql.IndexColumn{Name: name, Length: c.Length})
			nix.ords = append(nix.ords, ord)
			nix.prefix = append(nix.prefix, ix.prefix[i])
		}
		if ok {
			n.indexes = append(n.indexes, nix)
		}
	}
	n.rebuild()
	return &tempRewriter{t: t, n: n}, nil
}

type tempRewriter struct {
	t, n   *tempTable
	failed bool
}

func (r *tempRewriter) StatementBegin(*sql.Context)              {}
func (r *tempRewriter) DiscardChanges(*sql.Context, error) error { r.failed = true; return nil }
func (r *tempRewriter) StatementComplete(*sql.Context) error     { return nil }

func (r *tempRewriter) Insert(ctx *sql.Context, row sql.Row) error {
	if err := r.n.insert(ctx, row); err != nil {
		r.failed = true
		return err
	}
	return nil
}

func (r *tempRewriter) Close(*sql.Context) error {
	if r.failed {
		return nil
	}
	t, n := r.t, r.n
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sch, t.codecs, t.pk, t.autoOrd, t.indexes = n.sch, n.codecs, n.pk, n.autoOrd, n.indexes
	t.rows, t.undo = n.rows, nil
	t.rebuild()
	return nil
}

// IndexedAccess serves a lookup by scanning the rows and keeping those in
// its ranges (a lookup join doesn't check its condition again).
func (t *tempTable) indexedAccess() sql.IndexedTable { return &tempIndexed{t} }

type tempIndexed struct{ *tempTable }

type tempLookup struct{ lookup sql.IndexLookup }

func (tempLookup) Key() []byte { return []byte("temp-lookup") }

type tempLookupIter struct {
	p    *tempLookup
	done bool
}

func (it *tempLookupIter) Next(*sql.Context) (sql.Partition, error) {
	if it.done {
		return nil, io.EOF
	}
	it.done = true
	return it.p, nil
}
func (it *tempLookupIter) Close(*sql.Context) error { return nil }

func (t *tempIndexed) LookupPartitions(_ *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	return &tempLookupIter{p: &tempLookup{lookup}}, nil
}

func (t *tempIndexed) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	p, ok := part.(*tempLookup)
	desc, okIdx := p.lookup.Index.(*tempIndexDesc)
	ranges, okRanges := p.lookup.Ranges.(sql.MySQLRangeCollection)
	if !ok || !okIdx || !okRanges {
		return t.tempTable.PartitionRows(ctx, part)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	var rows []sql.Row
	for _, r := range t.rows {
		if r == nil {
			continue
		}
		in, err := inRanges(r, desc.ords, ranges)
		if err != nil {
			return nil, err
		}
		if in {
			rows = append(rows, r.Copy())
		}
	}
	return sql.RowsToRowIter(rows...), nil
}

// inRanges reports whether the row's index columns fall in one of the ranges.
func inRanges(row sql.Row, ords []int, ranges sql.MySQLRangeCollection) (bool, error) {
	for _, rang := range ranges {
		in := true
		for i, col := range rang {
			if i >= len(ords) {
				break
			}
			v := row[ords[i]]
			point := sql.NullRangeColumnExpr(col.Typ)
			if v != nil {
				point = sql.ClosedRangeColumnExpr(v, v, col.Typ)
			}
			_, ok, err := col.Overlaps(point)
			if err != nil {
				return false, err
			}
			if !ok {
				in = false
				break
			}
		}
		if in {
			return true, nil
		}
	}
	return false, nil
}
