package sqlengine

import (
	"fmt"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/vt/sqlparser"
)

// Stored functions (issue #20). go-mysql-server 0.20 has none; the vendored
// grammar (third_party/vitess, s2z_patches.go) turns CREATE FUNCTION f(a)
// RETURNS t body into the procedure s2z_fn_f(IN a, OUT s2z_ret t), each
// RETURN into setting s2z_ret and leaving the body. The provider resolves
// f(x) to a storedFunc, which runs CALL s2z_fn_f(x, @s2z_fn_ret) and is the
// variable's value.

// storedFuncVar is the user variable a stored function's CALL returns its
// value in. A nested call sets it before the outer CALL does, so one name
// serves every depth.
const storedFuncVar = "s2z_fn_ret"

// builtinNames are go-mysql-server's built-in functions, which win over a
// stored function of the same name, as in MySQL.
var builtinNames = sync.OnceValue(func() map[string]bool {
	m := map[string]bool{}
	for _, f := range function.BuiltIns {
		m[strings.ToLower(f.FunctionName())] = true
	}
	return m
})

// storedFuncSig is a stored function's parameters and return type, parsed
// from its CREATE statement (keyed by it).
type storedFuncSig struct {
	params int
	typ    sql.Type
	writes []string // the tables its body changes, lower case
}

var storedFuncSigs sync.Map

func parseStoredFunc(create string) (*storedFuncSig, error) {
	if s, ok := storedFuncSigs.Load(create); ok {
		return s.(*storedFuncSig), nil
	}
	stmt, err := sqlparser.Parse(create)
	if err != nil {
		return nil, err
	}
	ddl, ok := stmt.(*sqlparser.DDL)
	if !ok || ddl.ProcedureSpec == nil || len(ddl.ProcedureSpec.Params) == 0 {
		return nil, fmt.Errorf("stored function: unexpected definition %q", create)
	}
	ps := ddl.ProcedureSpec.Params
	typ, err := types.ColumnTypeToType(&ps[len(ps)-1].Type)
	if err != nil {
		return nil, err
	}
	sig := &storedFuncSig{params: len(ps) - 1, typ: typ, writes: bodyWrites(ddl.ProcedureSpec.Body, nil)}
	storedFuncSigs.Store(create, sig)
	return sig, nil
}

// storedFunction is the stored function name in the current database.
func (p *Provider) storedFunction(ctx *sql.Context, name string) (sql.Function, bool) {
	name = strings.ToLower(name)
	if ctx == nil || p.runner == nil || builtinNames()[name] {
		return nil, false
	}
	dbName := ctx.GetCurrentDatabase()
	if dbName == "" {
		return nil, false
	}
	db, err := p.Database(ctx, dbName)
	if err != nil {
		return nil, false
	}
	pd, ok := db.(sql.StoredProcedureDatabase)
	if !ok {
		return nil, false
	}
	spd, ok, err := pd.GetStoredProcedure(ctx, sqlparser.FunctionProcedurePrefix+name)
	if err != nil || !ok {
		return nil, false
	}
	sig, err := parseStoredFunc(spd.CreateStatement)
	if err != nil {
		return nil, false
	}
	return sql.FunctionN{Name: name, Fn: func(args ...sql.Expression) (sql.Expression, error) {
		if len(args) != sig.params {
			return nil, mysql.NewSQLError(1318, "42000", "Incorrect number of arguments for FUNCTION %s.%s; expected %d, got %d", dbName, name, sig.params, len(args))
		}
		return &storedFunc{db: dbName, name: name, args: args, typ: sig.typ, writes: sig.writes, runner: p.runner}, nil
	}}, true
}

// storedFunc is a call of a stored function.
type storedFunc struct {
	db, name string
	args     []sql.Expression
	typ      sql.Type
	writes   []string
	runner   sql.StatementRunner
}

var (
	_ sql.Expression                 = (*storedFunc)(nil)
	_ sql.NonDeterministicExpression = (*storedFunc)(nil)
)

func (f *storedFunc) Resolved() bool {
	for _, a := range f.args {
		if !a.Resolved() {
			return false
		}
	}
	return true
}

func (f *storedFunc) String() string {
	args := make([]string, len(f.args))
	for i, a := range f.args {
		args[i] = a.String()
	}
	return fmt.Sprintf("%s(%s)", f.name, strings.Join(args, ","))
}

func (f *storedFunc) Type() sql.Type             { return f.typ }
func (f *storedFunc) IsNullable() bool           { return true }
func (f *storedFunc) Children() []sql.Expression { return f.args }
func (f *storedFunc) IsNonDeterministic() bool   { return true }

func (f *storedFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != len(f.args) {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), len(f.args))
	}
	nf := *f
	nf.args = children
	return &nf, nil
}

// activeFuncs are the stored functions running in each session: MySQL
// refuses a recursive one (1424).
var activeFuncs sync.Map

var errRecursiveFunc = mysql.NewSQLError(1424, "HY000", "Recursive stored functions and triggers are not allowed.")

// injected is a value given to the CALL's AST as it is.
type injected struct{ e sql.Expression }

func (i injected) WithResolvedChildren([]any) (any, error) { return i.e, nil }

// outVar is the AST of @s2z_fn_ret.
var outVar = sync.OnceValue(func() sqlparser.Expr {
	stmt, err := sqlparser.Parse("CALL p(@" + storedFuncVar + ")")
	if err != nil {
		panic(err)
	}
	return stmt.(*sqlparser.Call).Params[0]
})

func (f *storedFunc) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	params := make([]sqlparser.Expr, 0, len(f.args)+1)
	for _, a := range f.args {
		v, err := a.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		params = append(params, sqlparser.InjectedExpr{Expression: injected{expression.NewLiteral(v, a.Type())}})
	}
	params = append(params, outVar())

	key := fmt.Sprintf("%d/%s.%s", ctx.Session.ID(), f.db, f.name)
	if _, running := activeFuncs.LoadOrStore(key, true); running {
		return nil, errRecursiveFunc
	}
	defer activeFuncs.Delete(key)

	call := &sqlparser.Call{ProcName: sqlparser.ProcedureName{
		Name:      sqlparser.NewColIdent(sqlparser.FunctionProcedurePrefix + f.name),
		Qualifier: sqlparser.NewTableIdent(f.db),
	}, Params: params}
	if err := ctx.SetUserVariable(ctx, storedFuncVar, nil, f.typ); err != nil {
		return nil, err
	}
	// The CALL ends as a query of its own would, ending the query in the
	// process list, which cancels the statement calling the function: it
	// runs without one.
	cctx := ctx.WithContext(ctx.Context)
	cctx.ProcessList = sql.EmptyProcessList{}
	// Interpreted, as a procedure's statements are, it runs in the
	// caller's transaction rather than committing it.
	_, err := sql.RunInterpreted(cctx, func(cctx *sql.Context) ([]sql.Row, error) {
		_, iter, _, err := f.runner.QueryWithBindings(cctx, "", call, nil, nil)
		if err != nil {
			return nil, err
		}
		return sql.RowIterToRows(cctx, iter)
	})
	if err != nil {
		return nil, err
	}
	_, v, err := ctx.GetUserVariable(ctx, storedFuncVar)
	if err != nil || v == nil {
		return nil, err
	}
	v, _, err = f.typ.Convert(ctx, v)
	return v, err
}

// bodyWrites adds the tables a routine body's INSERT, UPDATE, DELETE and
// REPLACE statements change to out.
func bodyWrites(s sqlparser.Statement, out []string) []string {
	each := func(ss sqlparser.Statements) {
		for _, st := range ss {
			out = bodyWrites(st, out)
		}
	}
	tables := func(ts sqlparser.TableExprs) {
		for _, te := range ts {
			if a, ok := te.(*sqlparser.AliasedTableExpr); ok {
				if tn, ok := a.Expr.(sqlparser.TableName); ok {
					out = append(out, strings.ToLower(tn.Name.String()))
				}
			}
		}
	}
	switch n := s.(type) {
	case *sqlparser.Insert:
		out = append(out, strings.ToLower(n.Table.Name.String()))
	case *sqlparser.Update:
		tables(n.TableExprs)
	case *sqlparser.Delete:
		if len(n.Targets) > 0 {
			for _, tn := range n.Targets {
				out = append(out, strings.ToLower(tn.Name.String()))
			}
		} else {
			tables(n.TableExprs)
		}
	case *sqlparser.BeginEndBlock:
		each(n.Statements)
	case *sqlparser.IfStatement:
		for _, c := range n.Conditions {
			each(c.Statements)
		}
		each(n.Else)
	case *sqlparser.CaseStatement:
		for _, c := range n.Cases {
			each(c.Statements)
		}
		each(n.Else)
	case *sqlparser.Loop:
		each(n.Statements)
	case *sqlparser.While:
		each(n.Statements)
	case *sqlparser.Repeat:
		each(n.Statements)
	case *sqlparser.Declare:
		if n.Handler != nil {
			out = bodyWrites(n.Handler.Statement, out)
		}
	}
	return out
}

// checkFuncWrites refuses a statement that calls a stored function which
// changes a table the statement uses, as MySQL does (1442): the function
// would change the rows the statement is reading or writing.
func checkFuncWrites(n sql.Node) error {
	var funcs []*storedFunc
	used := map[string]bool{}
	var visit func(n sql.Node)
	visit = func(n sql.Node) {
		transform.Inspect(n, func(n sql.Node) bool {
			switch n := n.(type) {
			case *plan.ResolvedTable:
				used[strings.ToLower(n.Name())] = true
			case *plan.TableAlias:
				if rt, ok := n.Child.(*plan.ResolvedTable); ok {
					used[strings.ToLower(rt.Name())] = true
				}
			case *plan.InsertInto:
				if n.Destination != nil {
					visit(n.Destination)
				}
			}
			if ex, ok := n.(sql.Expressioner); ok {
				for _, e := range ex.Expressions() {
					transform.InspectExpr(e, func(e sql.Expression) bool {
						switch e := e.(type) {
						case *storedFunc:
							funcs = append(funcs, e)
						case *plan.Subquery:
							visit(e.Query)
						}
						return false
					})
				}
			}
			return true
		})
	}
	visit(n)
	for _, f := range funcs {
		for _, w := range f.writes {
			if used[w] {
				return mysql.NewSQLError(1442, "HY000", "Can't update table '%s' in stored function/trigger because it is already used by statement which invoked this stored function/trigger.", w)
			}
		}
	}
	return nil
}
