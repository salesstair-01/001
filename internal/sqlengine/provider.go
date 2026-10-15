package sqlengine

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// Provider is the go-mysql-server database provider for one tenant: every
// database the tenant sees lives in its one partition (D1).
type Provider struct {
	store   *Store
	catalog *Catalog
}

var (
	_ sql.MutableDatabaseProvider  = (*Provider)(nil)
	_ sql.CollatedDatabaseProvider = (*Provider)(nil)
)

func NewProvider(store *Store) *Provider {
	return &Provider{store: store, catalog: NewCatalog(store)}
}

// Catalog returns the provider's catalog.
func (p *Provider) Catalog() *Catalog { return p.catalog }

func (p *Provider) Database(ctx *sql.Context, name string) (sql.Database, error) {
	d, err := p.catalog.Database(ctx, name)
	if err != nil {
		return nil, err
	}
	if d == nil {
		// Another query engine may have created it; reload once.
		if err := p.catalog.load(ctx); err != nil {
			return nil, err
		}
		if d, err = p.catalog.Database(ctx, name); err != nil {
			return nil, err
		}
		if d == nil {
			return nil, sql.ErrDatabaseNotFound.New(name)
		}
	}
	return &Database{p: p, name: d.Name, collation: d.Collation}, nil
}

func (p *Provider) HasDatabase(ctx *sql.Context, name string) bool {
	_, err := p.Database(ctx, name)
	return err == nil
}

func (p *Provider) AllDatabases(ctx *sql.Context) []sql.Database {
	dbs, err := p.catalog.Databases(ctx)
	if err != nil {
		ctx.GetLogger().Warnf("list databases: %v", err)
		return nil
	}
	out := make([]sql.Database, 0, len(dbs))
	for _, d := range dbs {
		out = append(out, &Database{p: p, name: d.Name, collation: d.Collation})
	}
	return out
}

func (p *Provider) CreateDatabase(ctx *sql.Context, name string) error {
	return p.CreateCollatedDatabase(ctx, name, sql.Collation_Default)
}

func (p *Provider) CreateCollatedDatabase(ctx *sql.Context, name string, collation sql.CollationID) error {
	if !supportedCollations[collation] {
		return fmt.Errorf("collation %s is not supported in v1", collation.Name())
	}
	return p.catalog.CreateDatabase(ctx, name, collation)
}

func (p *Provider) DropDatabase(ctx *sql.Context, name string) error {
	return p.catalog.DropDatabase(ctx, name)
}

// Database is one MySQL database of the tenant.
type Database struct {
	p         *Provider
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
	info, err := d.p.catalog.Table(ctx, d.name, name)
	if err != nil || info == nil {
		return nil, false, err
	}
	t, err := newTable(ctx, d.p.store, info)
	if err != nil {
		return nil, false, err
	}
	return t, true, nil
}

func (d *Database) GetTableNames(ctx *sql.Context) ([]string, error) {
	ts, err := d.p.catalog.Tables(ctx, d.name)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(ts))
	for i, t := range ts {
		names[i] = t.Name
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

// CreateTable stores a new table in the catalog. Tables are always clustered
// by primary key, as in InnoDB.
func (d *Database) CreateTable(ctx *sql.Context, name string, schema sql.PrimaryKeySchema, collation sql.CollationID, comment string) error {
	if len(schema.PkOrdinals) == 0 {
		// The hidden _rowid for tables without a primary key comes in slice 5.
		return fmt.Errorf("table %s: a PRIMARY KEY is required in this version", name)
	}
	if !supportedCollations[collation] {
		return fmt.Errorf("collation %s is not supported in v1", collation.Name())
	}
	info := &TableInfo{DB: d.name, Name: name, Collation: collation.Name(), Comment: comment}
	for i, col := range schema.Schema {
		ci, err := columnInfo(col, uint32(i+1))
		if err != nil {
			return err
		}
		if col.AutoIncrement {
			return fmt.Errorf("AUTO_INCREMENT comes in a later version (slice 5)")
		}
		if types.IsGeometry(col.Type) {
			return fmt.Errorf("column %s: spatial types are not supported in v1", col.Name)
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
	return d.p.catalog.CreateTable(ctx, info)
}

func (d *Database) DropTable(ctx *sql.Context, name string) error {
	info, err := d.p.catalog.Table(ctx, d.name, name)
	if err != nil {
		return err
	}
	if info == nil {
		return sql.ErrTableNotFound.New(name)
	}
	return d.p.catalog.DropTable(ctx, info)
}

func (d *Database) RenameTable(ctx *sql.Context, oldName, newName string) error {
	info, err := d.p.catalog.Table(ctx, d.name, oldName)
	if err != nil {
		return err
	}
	if info == nil {
		return sql.ErrTableNotFound.New(oldName)
	}
	return d.p.catalog.RenameTable(ctx, info, newName)
}
