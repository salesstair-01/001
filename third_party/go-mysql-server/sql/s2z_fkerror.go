// s2z patch: MySQL's text for foreign key violations.

package sql

import (
	"fmt"
	"strings"
)

// ViolationText is the constraint as MySQL describes it in errors 1451 and
// 1452: `db`.`table`, CONSTRAINT `name` FOREIGN KEY (...) REFERENCES
// `parent` (...), with its actions other than NO ACTION and the default.
func (f *ForeignKeyConstraint) ViolationText() string {
	quote := func(cols []string) string {
		q := make([]string, len(cols))
		for i, c := range cols {
			q[i] = "`" + c + "`"
		}
		return strings.Join(q, ", ")
	}
	parent := "`" + f.ParentTable + "`"
	if f.ParentDatabase != "" && !strings.EqualFold(f.ParentDatabase, f.Database) {
		parent = "`" + f.ParentDatabase + "`." + parent
	}
	s := fmt.Sprintf("`%s`.`%s`, CONSTRAINT `%s` FOREIGN KEY (%s) REFERENCES %s (%s)",
		f.Database, f.Table, f.Name, quote(f.Columns), parent, quote(f.ParentColumns))
	for _, a := range []struct {
		on  string
		act ForeignKeyReferentialAction
	}{{"DELETE", f.OnDelete}, {"UPDATE", f.OnUpdate}} {
		if a.act != ForeignKeyReferentialAction_DefaultAction && a.act != ForeignKeyReferentialAction_NoAction && a.act != "" {
			s += fmt.Sprintf(" ON %s %s", a.on, a.act)
		}
	}
	return s
}
