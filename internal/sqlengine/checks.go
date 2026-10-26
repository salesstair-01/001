package sqlengine

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/vitess/go/mysql"
)

// CHECK constraints (issue #21): stored with the table in the catalog, or in
// memory for a temporary table; go-mysql-server parses and enforces them on
// INSERT and UPDATE.

// CheckInfo is a stored CHECK constraint.
type CheckInfo struct {
	Name     string `json:"name"`
	Expr     string `json:"expr"`
	Enforced bool   `json:"enforced"`
}

var (
	_ sql.CheckTable          = (*Table)(nil)
	_ sql.CheckAlterableTable = (*Table)(nil)
	_ sql.CheckTable          = (*tempTable)(nil)
	_ sql.CheckAlterableTable = (*tempTable)(nil)
)

// checkName is MySQL's name for an unnamed constraint: <table>_chk_<n>,
// numbered after the table's others.
func checkName(table string, existing []CheckInfo) string {
	prefix := strings.ToLower(table) + "_chk_"
	n := 0
	for _, c := range existing {
		if rest, ok := strings.CutPrefix(strings.ToLower(c.Name), prefix); ok {
			if i, err := strconv.Atoi(rest); err == nil && i > n {
				n = i
			}
		}
	}
	return fmt.Sprintf("%s_chk_%d", table, n+1)
}

func checkDefs(cs []CheckInfo) []sql.CheckDefinition {
	out := make([]sql.CheckDefinition, len(cs))
	for i, c := range cs {
		out[i] = sql.CheckDefinition{Name: c.Name, CheckExpression: c.Expr, Enforced: c.Enforced}
	}
	return out
}

func errDuplicateCheck(name string) error {
	return mysql.NewSQLError(3822, "HY000", "Duplicate check constraint name '%s'.", name)
}

func errCheckNotFound(name, table string) error {
	return mysql.NewSQLError(3821, "HY000", "Check constraint '%s' is not found in the table.", name)
}

func (t *Table) GetChecks(*sql.Context) ([]sql.CheckDefinition, error) {
	return checkDefs(t.info.Checks), nil
}

// CreateCheck adds a constraint; names are unique within a database, as in
// MySQL.
func (t *Table) CreateCheck(ctx *sql.Context, c *sql.CheckDefinition) error {
	if c.Name == "" {
		c.Name = checkName(t.info.Name, t.info.Checks)
	}
	if dup, err := dbHasCheck(ctx, t.cat, t.info.DB, c.Name); err != nil || dup {
		if err == nil {
			err = errDuplicateCheck(c.Name)
		}
		return err
	}
	if err := t.alter(ctx, func(ti *TableInfo) error {
		ti.Checks = append(ti.Checks, CheckInfo{Name: c.Name, Expr: c.CheckExpression, Enforced: c.Enforced})
		return nil
	}); err != nil {
		return err
	}
	return t.reload(ctx) // the next unnamed constraint numbers after this one
}

// dbHasCheck reports whether a table of database db has a constraint named
// name.
func dbHasCheck(ctx *sql.Context, cat *Catalog, db, name string) (bool, error) {
	var dup bool
	err := cat.eachTable(ctx, func(ti *TableInfo) {
		if !strings.EqualFold(ti.DB, db) {
			return
		}
		for _, x := range ti.Checks {
			dup = dup || strings.EqualFold(x.Name, name)
		}
	})
	return dup, err
}

// checkCreateChecks refuses a CREATE TABLE whose named constraints clash
// with each other or with the database's, before the table is created:
// go-mysql-server adds constraints after the table, and a failure there
// would leave the table behind, where MySQL creates nothing.
func checkCreateChecks(ctx *sql.Context, n *plan.CreateTable) error {
	d, ok := n.Database().(*Database)
	if !ok {
		return nil
	}
	if n.IfNotExists() {
		if _, exists, err := d.GetTableInsensitive(ctx, n.Name()); err != nil || exists {
			return err // nothing is created
		}
	}
	seen := map[string]bool{}
	for _, c := range n.Checks() {
		if c.Name == "" {
			continue
		}
		k := strings.ToLower(c.Name)
		if seen[k] {
			return errDuplicateCheck(c.Name)
		}
		seen[k] = true
		if n.Temporary() {
			continue // a temporary table's names are its own
		}
		if dup, err := dbHasCheck(ctx, d.cat, d.name, c.Name); err != nil || dup {
			if err == nil {
				err = errDuplicateCheck(c.Name)
			}
			return err
		}
	}
	return nil
}

func (t *Table) DropCheck(ctx *sql.Context, name string) error {
	return t.alter(ctx, func(ti *TableInfo) error {
		for i, c := range ti.Checks {
			if strings.EqualFold(c.Name, name) {
				ti.Checks = append(ti.Checks[:i], ti.Checks[i+1:]...)
				return nil
			}
		}
		return errCheckNotFound(name, ti.Name)
	})
}

func (t *tempTable) GetChecks(*sql.Context) ([]sql.CheckDefinition, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return checkDefs(t.checks), nil
}

func (t *tempTable) CreateCheck(_ *sql.Context, c *sql.CheckDefinition) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c.Name == "" {
		c.Name = checkName(t.name, t.checks)
	}
	for _, x := range t.checks {
		if strings.EqualFold(x.Name, c.Name) {
			return errDuplicateCheck(c.Name)
		}
	}
	t.checks = append(t.checks, CheckInfo{Name: c.Name, Expr: c.CheckExpression, Enforced: c.Enforced})
	return nil
}

func (t *tempTable) DropCheck(_ *sql.Context, name string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	for i, c := range t.checks {
		if strings.EqualFold(c.Name, name) {
			t.checks = append(t.checks[:i], t.checks[i+1:]...)
			return nil
		}
	}
	return errCheckNotFound(name, t.name)
}
