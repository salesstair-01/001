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
