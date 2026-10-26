package sqlengine

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/vitess/go/mysql"
)

// MySQL stores a trigger whose body names a column the table lacks (NEW.bt)
// and fails the statements that fire it (1054). go-mysql-server binds every
// stored trigger of the database for DROP TRIGGER, DROP TABLE and each
// INSERT, UPDATE or DELETE, so one such trigger failed all of them, on any
// table, and could not be dropped (MySQL's trigger.test, issue #20).
// GetTriggers binds each trigger itself, once per catalog version; one that
// does not bind is handed to go-mysql-server as a stand-in on the same
// table and event whose body raises the binding error, as MySQL does when
// it fires: it can be listed, dropped and dropped with its table.

type checkedTrigger struct {
	version uint64
	create  string // the statement go-mysql-server gets
}

var reTriggerHead = regexp.MustCompile(`(?is)^\s*create\s+(?:definer\s*=\s*\S+\s+)?trigger\s+(?:if\s+not\s+exists\s+)?(\S+)\s+(before|after)\s+(insert|update|delete)\s+on\s+(\S+)\s+for\s+each\s+row`)

// triggerStatement is r's CREATE TRIGGER, or its stand-in if it does not bind.
func (d *Database) triggerStatement(ctx *sql.Context, r *RoutineInfo) string {
	key := routineMapKey(r.DB, r.Kind, r.Name)
	c := d.cat
	c.mu.RLock()
	v, ok := c.trgCheck[key]
	version := c.version
	c.mu.RUnlock()
	if ok && v.version == version {
		return v.create
	}
	create := r.Create
	if err := d.bindTrigger(ctx, r); err != nil {
		if m := reTriggerHead.FindStringSubmatch(r.Create); m != nil {
			create = standInTrigger(m, err)
		}
	}
	c.mu.Lock()
	if c.trgCheck == nil {
		c.trgCheck = map[string]checkedTrigger{}
	}
	c.trgCheck[key] = checkedTrigger{version: version, create: create}
	c.mu.Unlock()
	return create
}

// bindTrigger binds a stored trigger as go-mysql-server does to fire it.
func (d *Database) bindTrigger(ctx *sql.Context, r *RoutineInfo) (err error) {
	if d.pro == nil || d.pro.binder == nil {
		return nil
	}
	b := planbuilder.New(ctx, d.pro.binder, nil, nil)
	b.DisableAuth()
	b.TriggerCtx().Active = true
	b.SetParserOptions(sql.NewSqlModeFromString(r.SQLMode).ParserOptions())
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("%v", p)
		}
	}()
	_, _, _, _, err = b.Parse(r.Create, nil, false)
	return err
}

var reNoTriggerRow = regexp.MustCompile(`(?i)^table not found: (new|old)$`)

// standInTrigger is a trigger on m's table and event that fails as the
// broken one does when it fires.
func standInTrigger(m []string, bindErr error) string {
	code, state, msg := 1105, "HY000", bindErr.Error()
	if se, ok := withMySQLCode(bindErr).(*mysql.SQLError); ok {
		code, state, msg = se.Num, se.State, se.Message
	}
	if r := reNoTriggerRow.FindStringSubmatch(msg); r != nil {
		code, state, msg = 1054, "42S22", fmt.Sprintf("Unknown column in '%s'", strings.ToUpper(r[1]))
	}
	return fmt.Sprintf("CREATE TRIGGER %s %s %s ON %s FOR EACH ROW BEGIN SIGNAL SQLSTATE '%s' SET MESSAGE_TEXT = '%s', MYSQL_ERRNO = %d; END",
		m[1], m[2], m[3], m[4], state, strings.ReplaceAll(msg, "'", "''"), code)
}
