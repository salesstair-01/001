package sqlengine

import (
	"context"
	"encoding/json"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/salesstair-01/001/internal/encoding"
	"github.com/salesstair-01/001/internal/kv"
	"github.com/salesstair-01/001/internal/kvrpc"
)

// Triggers and stored procedures (issues #19, #20). go-mysql-server parses,
// plans and runs them; a database only stores their CREATE statements.
// Each is a catalog row ("routine", "db.kind.name") -> RoutineInfo, cached
// with the rest of the catalog, so every query engine of the tenant sees
// it once the catalog version moves.

// RoutineInfo is a stored trigger or procedure.
type RoutineInfo struct {
	DB         string    `json:"db"`
	Kind       string    `json:"kind"` // "trigger", "procedure" or "function"
	Name       string    `json:"name"`
	Create     string    `json:"create"`
	SQLMode    string    `json:"sql_mode,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	ModifiedAt time.Time `json:"modified_at"`
}

func routineMapKey(db, kind, name string) string {
	return strings.ToLower(db) + "." + kind + "." + strings.ToLower(name)
}

func (s *Store) routineKey(db, kind, name string) []byte {
	k := encoding.AppendBytes(s.catalogPrefix(), []byte("routine"), false)
	return encoding.AppendBytes(k, []byte(routineMapKey(db, kind, name)), false)
}

func decodeRoutine(b []byte) (*RoutineInfo, error) {
	var r RoutineInfo
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

// Routines returns a database's routines of a kind, oldest first (the
// order triggers of one event and timing fire in).
func (c *Catalog) Routines(ctx context.Context, db, kind string) ([]*RoutineInfo, error) {
	if err := c.ensure(ctx); err != nil {
		return nil, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	var out []*RoutineInfo
	for _, r := range c.routines {
		if r.Kind == kind && (db == "" || strings.EqualFold(r.DB, db)) {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

// Routine finds a routine by database, kind and name (case-insensitive).
func (c *Catalog) Routine(ctx context.Context, db, kind, name string) (*RoutineInfo, error) {
	if err := c.ensure(ctx); err != nil {
		return nil, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.routines[routineMapKey(db, kind, name)], nil
}

// CreateRoutine stores a routine; dup is the error for an existing name.
func (c *Catalog) CreateRoutine(ctx context.Context, r *RoutineInfo, dup error) error {
	return c.ddl(ctx, func(txn *Txn) error {
		err := put(txn, ctx, kv.OpInsertOnly, c.store.routineKey(r.DB, r.Kind, r.Name), r)
		if errIsKind(err, kv.KindDuplicateKey) {
			return dup
		}
		return err
	})
}

// DropRoutine removes a routine; missing is the error when there is none.
func (c *Catalog) DropRoutine(ctx context.Context, db, kind, name string, missing error) error {
	return c.ddl(ctx, func(txn *Txn) error {
		key := c.store.routineKey(db, kind, name)
		vs, err := txn.Get(ctx, [][]byte{key}, kvrpc.ReadOpts{Stmt: 1, IncludeStmt: true, Lock: kv.LockExclusive})
		if err != nil {
			return err
		}
		if vs[0] == nil {
			return missing
		}
		return txn.Mutate(ctx, 1, []kv.Mutation{{Op: kv.OpDelete, Key: key}}, 0)
	})
}

// ---- sql.TriggerDatabase ----------------------------------------------------------

var (
	_ sql.TriggerDatabase         = (*Database)(nil)
	_ sql.StoredProcedureDatabase = (*Database)(nil)
)

func (d *Database) GetTriggers(ctx *sql.Context) ([]sql.TriggerDefinition, error) {
	rs, err := d.cat.Routines(ctx, d.name, "trigger")
	if err != nil {
		return nil, err
	}
	out := make([]sql.TriggerDefinition, len(rs))
	for i, r := range rs {
		out[i] = sql.TriggerDefinition{Name: r.Name, CreateStatement: d.triggerStatement(ctx, r), CreatedAt: r.CreatedAt, SqlMode: r.SQLMode}
	}
	return out, nil
}

func (d *Database) CreateTrigger(ctx *sql.Context, def sql.TriggerDefinition) error {
	at := def.CreatedAt
	if at.IsZero() {
		at = time.Now()
	}
	return d.cat.CreateRoutine(ctx, &RoutineInfo{DB: d.name, Kind: "trigger", Name: def.Name, Create: def.CreateStatement,
		SQLMode: def.SqlMode, CreatedAt: at, ModifiedAt: at}, mysql.NewSQLError(1359, "HY000", "Trigger already exists"))
}

// DropTrigger's error for a missing trigger is go-mysql-server's, which DROP
// TRIGGER IF EXISTS recognises; the handler gives it MySQL's 1360
// (errcodes.go). DropStoredProcedure's likewise (1305).
func (d *Database) DropTrigger(ctx *sql.Context, name string) error {
	return d.cat.DropRoutine(ctx, d.name, "trigger", name, sql.ErrTriggerDoesNotExist.New(name))
}

// ---- sql.StoredProcedureDatabase --------------------------------------------------

func procedureDetails(r *RoutineInfo) sql.StoredProcedureDetails {
	return sql.StoredProcedureDetails{Name: r.Name, CreateStatement: r.Create, CreatedAt: r.CreatedAt, ModifiedAt: r.ModifiedAt, SqlMode: r.SQLMode}
}

func (d *Database) GetStoredProcedure(ctx *sql.Context, name string) (sql.StoredProcedureDetails, bool, error) {
	kind, name := routineKind(name)
	r, err := d.cat.Routine(ctx, d.name, kind, name)
	if err != nil || r == nil {
		return sql.StoredProcedureDetails{}, false, err
	}
	spd := procedureDetails(r)
	if kind == "function" {
		spd.Name = sqlparser.FunctionProcedurePrefix + spd.Name
	}
	return spd, true, nil
}

func (d *Database) GetStoredProcedures(ctx *sql.Context) ([]sql.StoredProcedureDetails, error) {
	rs, err := d.cat.Routines(ctx, d.name, "procedure")
	if err != nil {
		return nil, err
	}
	out := make([]sql.StoredProcedureDetails, len(rs))
	for i, r := range rs {
		out[i] = procedureDetails(r)
	}
	return out, nil
}

func (d *Database) SaveStoredProcedure(ctx *sql.Context, spd sql.StoredProcedureDetails) error {
	at := spd.CreatedAt
	if at.IsZero() {
		at = time.Now()
	}
	mod := spd.ModifiedAt
	if mod.IsZero() {
		mod = at
	}
	kind, name := routineKind(spd.Name)
	return d.cat.CreateRoutine(ctx, &RoutineInfo{DB: d.name, Kind: kind, Name: name, Create: spd.CreateStatement,
		SQLMode: spd.SqlMode, CreatedAt: at, ModifiedAt: mod}, mysql.NewSQLError(1304, "42000", "%s %s already exists", strings.ToUpper(kind), name))
}

func (d *Database) DropStoredProcedure(ctx *sql.Context, name string) error {
	kind, bare := routineKind(name)
	if kind == "function" {
		// errcodes.go reports it as MySQL does, FUNCTION db.f
		name = sqlparser.FunctionProcedurePrefix + d.name + "." + bare
	}
	return d.cat.DropRoutine(ctx, d.name, kind, bare, sql.ErrStoredProcedureDoesNotExist.New(name))
}

// routineKind is where a procedure name is stored: a stored function is
// stored as the procedure s2z_fn_<name> (the grammar's rewrite,
// third_party/vitess), which is kept as the function <name>.
func routineKind(name string) (kind, bare string) {
	if f, ok := strings.CutPrefix(strings.ToLower(name), sqlparser.FunctionProcedurePrefix); ok {
		return "function", f
	}
	return "procedure", name
}

// allPrivileges is every session's privilege set: sqld has no grant tables
// (the proxy authenticates), and go-mysql-server lists triggers and
// routines in information_schema and SHOW ... STATUS only for a session
// with a privilege set.
var allPrivileges = mysql_db.NewPrivilegeSetWithAllPrivileges()

func (s *Session) GetPrivilegeSet() (sql.PrivilegeSet, uint64) {
	if ps, n := s.BaseSession.GetPrivilegeSet(); ps != nil {
		return ps, n
	}
	return allPrivileges, 0
}

// checkCreateTrigger refuses what MySQL refuses before a trigger is stored,
// which go-mysql-server stores: a trigger on a temporary table (1361), and
// one whose table is in another schema. MySQL's trigger schema is the
// trigger name's qualifier or else the current database, and an unqualified
// table name is looked up there (1146), while go-mysql-server resolves it in
// the current database; a qualified one elsewhere is 1435.
func checkCreateTrigger(ctx *sql.Context, n *plan.CreateTrigger) error {
	rt, ok := n.Table.(*plan.ResolvedTable)
	if !ok {
		return nil
	}
	if tt, ok := rt.UnderlyingTable().(sql.TemporaryTable); ok && tt.IsTemporary() {
		return mysql.NewSQLError(1361, "HY000", "Trigger's '%s' is view, temporary table or sequence", rt.Name())
	}
	m := reCreateTrigger.FindStringSubmatch(n.CreateTriggerString)
	if m == nil || rt.Database() == nil {
		return nil
	}
	schema, _ := splitQualified(m[1])
	if schema == "" {
		schema = ctx.GetCurrentDatabase()
	}
	tdb, _ := splitQualified(m[2])
	switch {
	case strings.EqualFold(schema, rt.Database().Name()):
		return nil
	case tdb == "":
		return mysql.NewSQLError(1146, "42S02", "Table '%s.%s' doesn't exist", schema, rt.Name())
	default:
		return mysql.NewSQLError(1435, "HY000", "Trigger in wrong schema")
	}
}

var reCreateTrigger = regexp.MustCompile(`(?is)^\s*create\s+(?:definer\s*=\s*\S+\s+)?trigger\s+(?:if\s+not\s+exists\s+)?(\S+)\s+.*?\bon\s+(\S+)\s+for\s+each\s+row`)

// splitQualified splits [db.]name, unquoting backquotes.
func splitQualified(s string) (db, name string) {
	s = strings.ReplaceAll(s, "`", "")
	if i := strings.LastIndexByte(s, '.'); i >= 0 {
		return s[:i], s[i+1:]
	}
	return "", s
}
