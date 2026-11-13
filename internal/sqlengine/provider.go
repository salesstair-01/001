package sqlengine

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"strconv"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

// Provider is the go-mysql-server database provider. Every database a tenant
// sees lives in its one partition (D1); the tenant is the session's store,
// so one query engine can serve many tenants (slice 7's generic engines).
type Provider struct {
	def *Store // tenant for contexts without a Session (single-tenant engines); may be nil

	binder sql.Catalog         // the analyzer's, to bind stored triggers (triggercheck.go)
	runner sql.StatementRunner // the engine, to run stored functions (storedfunc.go)
}

var (
	_ sql.MutableDatabaseProvider  = (*Provider)(nil)
	_ sql.CollatedDatabaseProvider = (*Provider)(nil)
)

func NewProvider(store *Store) *Provider {
	return &Provider{def: store}
}

// catalogOf returns the catalog of the context's tenant.
func (p *Provider) catalogOf(ctx *sql.Context) (*Catalog, error) {
	if s, ok := ctx.Session.(*Session); ok && s.store != nil {
		return s.store.catalog, nil
	}
	if p.def != nil {
		return p.def.catalog, nil
	}
	return nil, fmt.Errorf("no tenant for this session")
}

func (p *Provider) Database(ctx *sql.Context, name string) (sql.Database, error) {
	cat, err := p.catalogOf(ctx)
	if err != nil {
		return nil, err
	}
	d, err := cat.Database(ctx, name)
	if err != nil {
		return nil, err
	}
	if d == nil {
		// Another query engine may have created it.
		if err := cat.refresh(ctx); err != nil {
			return nil, err
		}
		if d, err = cat.Database(ctx, name); err != nil {
			return nil, err
		}
		if d == nil {
			return nil, sql.ErrDatabaseNotFound.New(name)
		}
	}
	return &Database{cat: cat, pro: p, name: d.Name, collation: d.Collation}, nil
}

func (p *Provider) HasDatabase(ctx *sql.Context, name string) bool {
	_, err := p.Database(ctx, name)
	return err == nil
}

func (p *Provider) AllDatabases(ctx *sql.Context) []sql.Database {
	cat, err := p.catalogOf(ctx)
	if err != nil {
		return nil
	}
	dbs, err := cat.Databases(ctx)
	if err != nil {
		ctx.GetLogger().Warnf("list databases: %v", err)
		return nil
	}
	out := make([]sql.Database, 0, len(dbs))
	for _, d := range dbs {
		out = append(out, &Database{cat: cat, pro: p, name: d.Name, collation: d.Collation})
	}
	return out
}

func (p *Provider) CreateDatabase(ctx *sql.Context, name string) error {
	return p.CreateCollatedDatabase(ctx, name, sql.Collation_utf8mb4_0900_ai_ci)
}

func (p *Provider) CreateCollatedDatabase(ctx *sql.Context, name string, collation sql.CollationID) error {
	if !collationSupported(collation) {
		return fmt.Errorf("collation %s is not supported in v1", collation.Name())
	}
	cat, err := p.catalogOf(ctx)
	if err != nil {
		return err
	}
	return cat.CreateDatabase(ctx, name, collation)
}

func (p *Provider) DropDatabase(ctx *sql.Context, name string) error {
	cat, err := p.catalogOf(ctx)
	if err != nil {
		return err
	}
	return cat.DropDatabase(ctx, name)
}

// Database is one MySQL database of the tenant.
type Database struct {
	cat       *Catalog
	pro       *Provider
	name      string
	collation string
}

var (
	_ sql.Database         = (*Database)(nil)
	_ sql.TableCreator     = (*Database)(nil)
	_ sql.TableDropper     = (*Database)(nil)
	_ sql.TableRenamer     = (*Database)(nil)
	_ sql.CollatedDatabase = (*Database)(nil)
)

func (d *Database) Name() string { return d.name }

func (d *Database) GetTableInsensitive(ctx *sql.Context, name string) (sql.Table, bool, error) {
	if t := sessionTemps(ctx).get(d.name, name); t != nil {
		return t, true, nil
	}
	info, err := d.cat.Table(ctx, d.name, name)
	if err != nil || info == nil {
		return nil, false, err
	}
	t, err := newTable(ctx, d.cat, info)
	if err != nil {
		return nil, false, err
	}
	return t, true, nil
}

func (d *Database) GetTableNames(ctx *sql.Context) ([]string, error) {
	ts, err := d.cat.Tables(ctx, d.name)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(ts))
	for _, t := range ts {
		if !isFulltextTable(t.Name) { // a FULLTEXT index's own table (fulltext.go)
			names = append(names, t.Name)
		}
	}
	return names, nil
}

func (d *Database) GetCollation(*sql.Context) sql.CollationID {
	if c, err := sql.ParseCollation("", d.collation, false); err == nil {
		return c
	}
	return sql.Collation_Default
}

func (d *Database) SetCollation(ctx *sql.Context, c sql.CollationID) error {
	return fmt.Errorf("ALTER DATABASE ... COLLATE is not supported in v1")
}

// CreateTable stores a new table in the catalog. Tables are clustered by
// primary key, or by a hidden _rowid when they have none, as in InnoDB.
func (d *Database) CreateTable(ctx *sql.Context, name string, schema sql.PrimaryKeySchema, collation sql.CollationID, comment string) error {
	if !collationSupported(collation) {
		return fmt.Errorf("collation %s is not supported in v1", collation.Name())
	}
	info := &TableInfo{DB: d.name, Name: name, Collation: collation.Name(), Comment: comment}
	for i, col := range schema.Schema {
		ci, err := columnInfo(col, uint32(i+1))
		if err != nil {
			return err
		}
		info.Columns = append(info.Columns, ci)
	}
	for _, ord := range schema.PkOrdinals {
		col := schema.Schema[ord]
		if !keyable(col.Type) {
			return fmt.Errorf("column %s: type %s cannot be part of a primary key in v1", col.Name, col.Type)
		}
		info.PK = append(info.PK, uint32(ord+1))
	}
	info.NextColumnID = uint32(len(schema.Schema) + 1)
	return d.cat.CreateTable(ctx, info)
}

func (d *Database) DropTable(ctx *sql.Context, name string) error {
	if d.dropTemp(ctx, name) {
		return nil
	}
	info, err := d.cat.Table(ctx, d.name, name)
	if err != nil {
		return err
	}
	if info == nil {
		return sql.ErrTableNotFound.New(name)
	}
	return d.cat.DropTable(ctx, info)
}

func (d *Database) RenameTable(ctx *sql.Context, oldName, newName string) error {
	if ok, err := d.renameTemp(ctx, oldName, newName); ok {
		return err
	}
	info, err := d.cat.Table(ctx, d.name, oldName)
	if err != nil {
		return err
	}
	if info == nil {
		return sql.ErrTableNotFound.New(oldName)
	}
	if info, err = renamedChecks(ctx, d.cat, info, d.name, newName); err != nil {
		return err
	}
	return d.cat.RenameTable(ctx, info, newName)
}

var _ sql.TableMover = (*Database)(nil)

// MoveTable is RENAME TABLE a.t TO b.u: the table goes to database b, its
// generated CHECK names renamed (3822 when b has one of them).
func (d *Database) MoveTable(ctx *sql.Context, oldDb, oldName, newDb, newName string) error {
	info, err := d.cat.Table(ctx, oldDb, oldName)
	if err != nil {
		return err
	}
	if info == nil {
		return mysql.NewSQLError(1146, "42S02", "Table '%s.%s' doesn't exist", oldDb, oldName)
	}
	if db, err := d.cat.Database(ctx, newDb); err != nil {
		return err
	} else if db == nil {
		return sql.ErrDatabaseNotFound.New(newDb)
	}
	if existing, err := d.cat.Table(ctx, newDb, newName); err != nil {
		return err
	} else if existing != nil {
		return mysql.NewSQLError(1050, "42S01", "Table '%s' already exists", newName)
	}
	if info, err = renamedChecks(ctx, d.cat, info, newDb, newName); err != nil {
		return err
	}
	return d.cat.MoveTable(ctx, info, newDb, newName)
}

// renamedChecks is info with its generated CHECK names (<old>_chk_<n>)
// renamed for the table's new name, as MySQL renames them; a name another
// table of database db has is refused (3822).
func renamedChecks(ctx *sql.Context, cat *Catalog, info *TableInfo, db, newName string) (*TableInfo, error) {
	prefix := strings.ToLower(info.Name) + "_chk_"
	out := info.clone()
	for i, c := range out.Checks {
		if rest, ok := strings.CutPrefix(strings.ToLower(c.Name), prefix); ok {
			if _, err := strconv.Atoi(rest); err == nil {
				out.Checks[i].Name = newName + "_chk_" + rest
			}
		}
	}
	for _, c := range out.Checks {
		dup := false
		err := cat.eachTable(ctx, func(ti *TableInfo) {
			if ti.ID == info.ID || !strings.EqualFold(ti.DB, db) {
				return
			}
			for _, x := range ti.Checks {
				dup = dup || strings.EqualFold(x.Name, c.Name)
			}
		})
		if err != nil {
			return nil, err
		}
		if dup {
			return nil, errDuplicateCheck(c.Name)
		}
	}
	return out, nil
}

// alterRenameErrors refuses, before any of its clauses runs, an ALTER TABLE
// ... RENAME TO whose renamed CHECK names clash (3822): ALTER TABLE is not
// atomic here, and MySQL changes nothing.
func alterRenameErrors(ctx *sql.Context, a *analyzer.Analyzer, st sqlparser.Statement) error {
	at, ok := st.(*sqlparser.AlterTable)
	if !ok {
		return nil
	}
	for _, d := range at.Statements {
		if d == nil || !strings.EqualFold(d.Action, sqlparser.RenameStr) || len(d.FromTables) != 1 || len(d.ToTables) != 1 {
			continue
		}
		cur := ctx.GetCurrentDatabase()
		fromDb, toDb := d.FromTables[0].DbQualifier.String(), d.ToTables[0].DbQualifier.String()
		if fromDb == "" {
			fromDb = cur
		}
		if toDb == "" {
			toDb = cur
		}
		sdb, err := a.Catalog.Database(ctx, fromDb)
		if err != nil {
			return nil // the ALTER fails on its own
		}
		db, ok := sdb.(*Database)
		if !ok {
			return nil
		}
		info, err := db.cat.Table(ctx, fromDb, d.FromTables[0].Name.String())
		if err != nil || info == nil {
			return nil
		}
		if _, err := renamedChecks(ctx, db.cat, info, toDb, d.ToTables[0].Name.String()); err != nil {
			return err
		}
	}
	return nil
}
