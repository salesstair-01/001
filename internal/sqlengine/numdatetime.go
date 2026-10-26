package sqlengine

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/shopspring/decimal"
)

// Numeric datetime values (issue #25): MySQL reads 20010202105916,
// 010202105916, 20010202 and 010202, as numbers or digit strings, as
// YYYYMMDDhhmmss, YYMMDDhhmmss, YYYYMMDD and YYMMDD; go-mysql-server refuses
// them (1292). This pre-analyze rule rewrites such a constant into
// 'YYYY-MM-DD hh:mm:ss' where it becomes a DATE, DATETIME or TIMESTAMP: a
// value in INSERT ... VALUES or UPDATE ... SET for such a column, a CAST to
// DATE or DATETIME, and a comparison with such a column.

// An INSERT of only literal values skips the analyzer's custom rules, so
// its values are rewritten when it is built (exactUpdates.Build).
const numDatetimeID analyzer.RuleId = 10010

func numDatetime(ctx *sql.Context, a *analyzer.Analyzer, n sql.Node, scope *plan.Scope, sel analyzer.RuleSelector, qf *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	n, same1, err := numDatetimeInserts(n)
	if err != nil {
		return nil, transform.SameTree, err
	}
	n, same2, err := transform.NodeExprsWithOpaque(n, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		switch e := e.(type) {
		case *expression.Convert:
			if types.IsTime(e.Type()) {
				if l := datetimeLiteral(e.Child); l != nil {
					c, err := e.WithChildren(l)
					return c, transform.NewTree, err
				}
			}
		case *expression.SetField:
			if types.IsTime(e.LeftChild.Type()) {
				if l := datetimeLiteral(e.RightChild); l != nil {
					c, err := e.WithChildren(e.LeftChild, l)
					return c, transform.NewTree, err
				}
			}
		case *expression.Equals, *expression.NullSafeEquals, *expression.GreaterThan, *expression.LessThan,
			*expression.GreaterThanOrEqual, *expression.LessThanOrEqual:
			c := e.(expression.Comparer)
			l, r := c.Left(), c.Right()
			if _, ok := l.(*expression.GetField); ok && types.IsTime(l.Type()) {
				if lit := datetimeLiteral(r); lit != nil {
					ne, err := e.WithChildren(l, lit)
					return ne, transform.NewTree, err
				}
			}
			if _, ok := r.(*expression.GetField); ok && types.IsTime(r.Type()) {
				if lit := datetimeLiteral(l); lit != nil {
					ne, err := e.WithChildren(lit, r)
					return ne, transform.NewTree, err
				}
			}
		case *expression.Between:
			if _, ok := e.Val.(*expression.GetField); ok && types.IsTime(e.Val.Type()) {
				lo, hi := datetimeLiteral(e.Lower), datetimeLiteral(e.Upper)
				if lo != nil || hi != nil {
					if lo == nil {
						lo = e.Lower
					}
					if hi == nil {
						hi = e.Upper
					}
					return expression.NewBetween(e.Val, lo, hi), transform.NewTree, nil
				}
			}
		case *expression.Interval:
			// '4.5' SECOND_MICROSECOND is 4.5 seconds in MySQL, 4 seconds
			// and 5 microseconds in go-mysql-server: pad the fraction.
			if lit, ok := e.Child.(*expression.Literal); ok && strings.HasSuffix(strings.ToUpper(e.Unit), "_MICROSECOND") {
				if v, ok := lit.Value().(string); ok {
					if i := strings.LastIndexByte(v, '.'); i >= 0 && len(v)-i-1 < 6 {
						return expression.NewInterval(expression.NewLiteral(v+strings.Repeat("0", 6-(len(v)-i-1)), lit.Type()), e.Unit), transform.NewTree, nil
					}
				}
			}
		case *plan.Subquery:
			q, same, err := numDatetime(ctx, a, e.Query, scope, sel, qf)
			if err != nil || same {
				return e, transform.SameTree, err
			}
			return e.WithQuery(q), transform.NewTree, nil
		}
		return e, transform.SameTree, nil
	})
	if !same1 {
		return n, transform.NewTree, err
	}
	return n, same2, err
}

// numDatetimeInserts rewrites the values of INSERT ... VALUES for DATE,
// DATETIME and TIMESTAMP columns. The VALUES are the INSERT's source or,
// once analyzed, under the projection onto the table's columns.
func numDatetimeInserts(n sql.Node) (sql.Node, bool, error) {
	same := true
	out, _, err := transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		ii, ok := n.(*plan.InsertInto)
		if !ok || ii.Destination == nil {
			return n, transform.SameTree, nil
		}
		var cols []*sql.Column
		vals, ok := ii.Source.(*plan.Values)
		proj, isProj := ii.Source.(*plan.Project)
		switch {
		case ok:
			cols = insertColumns(ii)
		case isProj:
			if vals, ok = proj.Child.(*plan.Values); !ok {
				// INSERT ... SELECT: the select's own constants
				np, changed := datetimeProjections(proj, insertColumns(ii))
				if !changed {
					return n, transform.SameTree, nil
				}
				same = false
				return ii.WithSource(np), transform.NewTree, nil
			}
			sch := ii.Destination.Schema()
			for i, p := range proj.Projections {
				if a, ok := p.(*expression.Alias); ok {
					p = a.Child
				}
				gf, ok := p.(*expression.GetField)
				if !ok || i >= len(sch) {
					continue
				}
				for len(cols) <= gf.Index() {
					cols = append(cols, nil)
				}
				cols[gf.Index()] = sch[i]
			}
		default:
			return n, transform.SameTree, nil
		}
		nv, changed := datetimeValues(vals, cols)
		if !changed {
			return n, transform.SameTree, nil
		}
		same = false
		if isProj {
			np, err := proj.WithChildren(nv)
			if err != nil {
				return nil, transform.SameTree, err
			}
			return ii.WithSource(np), transform.NewTree, nil
		}
		return ii.WithSource(nv), transform.NewTree, nil
	})
	return out, same, err
}

// datetimeProjections rewrites the constants a projection gives the
// temporal columns of cols.
func datetimeProjections(p *plan.Project, cols []*sql.Column) (sql.Node, bool) {
	var out []sql.Expression
	for i, e := range p.Projections {
		if i >= len(cols) || !types.IsTime(cols[i].Type) {
			continue
		}
		inner, alias := e, ""
		if a, ok := e.(*expression.Alias); ok {
			inner, alias = a.Child, a.Name()
		}
		l := datetimeLiteral(inner)
		if l == nil {
			continue
		}
		if out == nil {
			out = append([]sql.Expression(nil), p.Projections...)
		}
		if alias != "" {
			l = expression.NewAlias(alias, l)
		}
		out[i] = l
	}
	if out == nil {
		return p, false
	}
	return plan.NewProject(out, p.Child), true
}

// datetimeValues rewrites the values for the temporal columns of cols.
func datetimeValues(vals *plan.Values, cols []*sql.Column) (*plan.Values, bool) {
	changed := false
	tuples := make([][]sql.Expression, len(vals.ExpressionTuples))
	for r, tuple := range vals.ExpressionTuples {
		tuples[r] = tuple
		copied := false
		for i, e := range tuple {
			if i >= len(cols) || cols[i] == nil || !types.IsTime(cols[i].Type) {
				continue
			}
			if l := datetimeLiteral(e); l != nil {
				if !copied {
					tuples[r] = append([]sql.Expression(nil), tuple...)
					copied = true
				}
				changed = true
				tuples[r][i] = l
			}
		}
	}
	if !changed {
		return vals, false
	}
	return plan.NewValuesWithAlias(vals.AliasName, vals.ColumnNames, tuples), true
}

// insertColumns is the destination column of each value of an INSERT, nil
// if a name is unknown.
func insertColumns(ii *plan.InsertInto) []*sql.Column {
	sch := ii.Destination.Schema()
	if len(ii.ColumnNames) == 0 {
		return sch
	}
	cols := make([]*sql.Column, 0, len(ii.ColumnNames))
	for _, name := range ii.ColumnNames {
		i := sch.IndexOfColName(name)
		if i < 0 {
			return nil
		}
		cols = append(cols, sch[i])
	}
	return cols
}

// datetimeLiteral is the 'YYYY-MM-DD hh:mm:ss' literal for e when it is a
// numeric or digit-string constant MySQL reads as a datetime; nil when it
// isn't one.
func datetimeLiteral(e sql.Expression) sql.Expression {
	lit, ok := e.(*expression.Literal)
	if !ok {
		return nil
	}
	var digits, frac string
	switch v := lit.Value().(type) {
	case int8, int16, int32, int64, uint8, uint16, uint32, uint64:
		digits = fmt.Sprint(v)
	case decimal.Decimal:
		if v.IsNegative() {
			return nil
		}
		s := v.String()
		digits, frac, _ = strings.Cut(s, ".")
	case string:
		d, f, _ := strings.Cut(v, ".")
		if !allDigits(d) || (f != "" && !allDigits(f)) {
			return nil
		}
		switch len(d) {
		case 6, 12: // YYMMDD[hhmmss]
			y, _ := strconv.Atoi(d[:2])
			if y < 70 {
				d = "20" + d
			} else {
				d = "19" + d
			}
		case 8, 14:
		default:
			return nil
		}
		if len(d) == 8 {
			d += "000000"
		}
		return formatDigits(d, f)
	default:
		return nil
	}
	n, err := strconv.ParseInt(digits, 10, 64)
	if err != nil {
		return nil
	}
	n, ok = numberToDatetime(n)
	if !ok {
		return nil
	}
	return formatDigits(fmt.Sprintf("%014d", n), frac)
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// numberToDatetime is MySQL's number_to_datetime: the number as
// YYYYMMDDhhmmss, its two-digit years in 1970-2069.
func numberToDatetime(n int64) (int64, bool) {
	switch {
	case n < 101:
		return 0, false
	case n <= 691231:
		return (n + 20000000) * 1000000, true
	case n < 700101:
		return 0, false
	case n <= 991231:
		return (n + 19000000) * 1000000, true
	case n < 10000101:
		return 0, false
	case n <= 99991231:
		return n * 1000000, true
	case n < 101000000:
		return 0, false
	case n <= 691231235959:
		return n + 20000000000000, true
	case n < 700101000000:
		return 0, false
	case n <= 991231235959:
		return n + 19000000000000, true
	case n <= 99991231235959:
		return n, true
	}
	return 0, false
}

// formatDigits writes YYYYMMDDhhmmss and a fraction as a datetime string
// literal; nil for an impossible date or time, which is left to
// go-mysql-server to refuse.
func formatDigits(d, frac string) sql.Expression {
	n := func(a, b int) int { x, _ := strconv.Atoi(d[a:b]); return x }
	mo, day, h, mi, s := n(4, 6), n(6, 8), n(8, 10), n(10, 12), n(12, 14)
	if mo < 1 || mo > 12 || day < 1 || day > 31 || h > 23 || mi > 59 || s > 59 {
		return nil
	}
	out := fmt.Sprintf("%s-%s-%s %s:%s:%s", d[:4], d[4:6], d[6:8], d[8:10], d[10:12], d[12:14])
	if frac != "" {
		out += "." + frac
	}
	return expression.NewLiteral(out, types.LongText)
}
