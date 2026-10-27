package sqlparser

import "strconv"

// sizedLobType is the type MySQL makes of TEXT(n) or BLOB(n) (s2z patch):
// the smallest of tiny, plain, medium and long that holds n characters of
// at most bytesPerChar bytes each.
func sizedLobType(base string, n []byte, bytesPerChar uint64) string {
	v, err := strconv.ParseUint(string(n), 10, 64)
	if err != nil {
		return "long" + base
	}
	switch b := v * bytesPerChar; {
	case b < 1<<8:
		return "tiny" + base
	case b < 1<<16:
		return base
	case b < 1<<24:
		return "medium" + base
	}
	return "long" + base
}

// parenSelect is (inner) ORDER BY ... LIMIT ... (s2z patch): the outer
// clauses go on the inner query when it has none of its own, and otherwise
// apply to its result, as a derived table.
func parenSelect(inner SelectStatement, order OrderBy, limit *Limit) SelectStatement {
	if len(order) == 0 && limit == nil {
		return inner
	}
	if sel, ok := inner.(*Select); ok && len(sel.OrderBy) == 0 && sel.Limit == nil && sel.Into == nil {
		sel.OrderBy, sel.Limit = order, limit
		return sel
	}
	return &Select{
		SelectExprs: SelectExprs{&StarExpr{}},
		From: TableExprs{&AliasedTableExpr{
			Expr: &Subquery{Select: inner},
			As:   NewTableIdent("__s2z_paren"),
			Auth: AuthInformation{AuthType: AuthType_IGNORE},
		}},
		OrderBy: order,
		Limit:   limit,
	}
}

// typedLiteral makes DATE '...', TIME '...' and TIMESTAMP '...' in an
// expression a call of s2z_date_literal (s2z_time_literal,
// s2z_timestamp_literal), which sqld defines to check and type the value as
// MySQL does (issue #25); as a column default they stay strings.
func typedLiteral(e Expr) Expr {
	v, ok := e.(*SQLVal)
	if !ok || v.Typed == "" {
		return e
	}
	return &FuncExpr{
		Name:  NewColIdent("s2z_" + v.Typed + "_literal"),
		Exprs: SelectExprs{&AliasedExpr{Expr: &SQLVal{Type: v.Type, Val: v.Val}}},
	}
}

// Stored functions (s2z patch): CREATE FUNCTION f(a INT) RETURNS T body is
// stored and run as the procedure s2z_fn_f(IN a INT, OUT s2z_ret T), whose
// body is s2z_fn: BEGIN body END with each RETURN e turned into
// BEGIN SET s2z_ret = e; LEAVE s2z_fn; END. sqld calls it where f(...) is
// used. go-mysql-server 0.20 has no stored functions.
const (
	FunctionProcedurePrefix = "s2z_fn_"
	FunctionReturnParam     = "s2z_ret"
	functionBodyLabel       = "s2z_fn"
)

func functionAsProcedure(name ProcedureName, definer string, params []ProcedureParam, ret ColumnType, chars []Characteristic, body Statement) *ProcedureSpec {
	name.Name = NewColIdent(FunctionProcedurePrefix + name.Name.String())
	params = append(append([]ProcedureParam(nil), params...), ProcedureParam{Direction: ProcedureParamDirection_Out, Name: FunctionReturnParam, Type: ret})
	return &ProcedureSpec{
		ProcName:        name,
		Definer:         definer,
		Params:          params,
		Characteristics: chars,
		Body:            &BeginEndBlock{Label: functionBodyLabel, Statements: Statements{rewriteReturns(body)}},
	}
}

func rewriteReturns(s Statement) Statement {
	each := func(ss Statements) {
		for i := range ss {
			ss[i] = rewriteReturns(ss[i])
		}
	}
	switch n := s.(type) {
	case *Return:
		return &BeginEndBlock{Statements: Statements{
			&Set{Exprs: SetVarExprs{&SetVarExpr{Scope: SetScope_None, Name: &ColName{Name: NewColIdent(FunctionReturnParam)}, Expr: n.Expr}}},
			&Leave{Label: functionBodyLabel},
		}}
	case *BeginEndBlock:
		each(n.Statements)
	case *IfStatement:
		for i := range n.Conditions {
			each(n.Conditions[i].Statements)
		}
		each(n.Else)
	case *CaseStatement:
		for i := range n.Cases {
			each(n.Cases[i].Statements)
		}
		each(n.Else)
	case *Loop:
		each(n.Statements)
	case *While:
		each(n.Statements)
	case *Repeat:
		each(n.Statements)
	case *Declare:
		if n.Handler != nil {
			n.Handler.Statement = rewriteReturns(n.Handler.Statement)
		}
	}
	return s
}

// UserVarAssignFunction is what @v := expr in an expression becomes:
// s2z_assign_user_var('v', expr), which sqld defines to set @v and be
// its value (internal/sqlengine/uservar.go); upstream has no such
// expression.
const UserVarAssignFunction = "s2z_assign_user_var"
