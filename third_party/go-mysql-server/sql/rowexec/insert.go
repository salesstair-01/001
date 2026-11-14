// Copyright 2023 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rowexec

import (
	"fmt"
	"strings"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/shopspring/decimal"
	"io"

	"github.com/dolthub/vitess/go/vt/proto/query"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type insertIter struct {
	schema       sql.Schema
	inserter     sql.RowInserter
	replacer     sql.RowReplacer
	updater      sql.RowUpdater
	rowSource    sql.RowIter
	unlocker     func()
	ctx          *sql.Context
	insertExprs  []sql.Expression
	updateExprs  []sql.Expression
	checks       sql.CheckConstraints
	tableNode    sql.Node
	closed       bool
	ignore       bool
	returnExprs  []sql.Expression
	returnSchema sql.Schema

	firstGeneratedAutoIncRowIdx int

	deferredDefaults sql.FastIntSet

	// rowNum is the number of the row being inserted, from 1, for MySQL's
	// warnings (s2z patch)
	rowNum int
}

func getInsertExpressions(values sql.Node) []sql.Expression {
	var exprs []sql.Expression
	transform.Inspect(values, func(node sql.Node) bool {
		switch node := node.(type) {
		case *plan.Project:
			exprs = node.Projections
			return false
		}
		return true
	})
	return exprs
}

func (i *insertIter) Next(ctx *sql.Context) (returnRow sql.Row, returnErr error) {
	row, err := i.rowSource.Next(ctx)
	if err == io.EOF {
		return nil, err
	}
	i.rowNum++

	if err != nil {
		return nil, i.ignoreOrClose(ctx, row, err)
	}

	// Prune the row down to the size of the schema. It can be larger in the case of running with an outer scope, in which
	// case the additional scope variables are prepended to the row.
	if len(row) > len(i.schema) {
		row = row[len(row)-len(i.schema):]
	}

	err = i.validateNullability(ctx, i.schema, row)
	if err != nil {
		return nil, i.ignoreOrClose(ctx, row, err)
	}

	err = i.evaluateChecks(ctx, row)
	if err != nil {
		return nil, i.ignoreOrClose(ctx, row, err)
	}

	origRow := make(sql.Row, len(row))
	copy(origRow, row)

	// Do any necessary type conversions to the target schema
	for idx, col := range i.schema {
		if row[idx] != nil {
			converted, inRange, cErr := col.Type.Convert(ctx, row[idx])
			if cErr == nil && !inRange {
				cErr = sql.ErrValueOutOfRange.New(row[idx], col.Type)
			}
			if cErr == nil {
				noteDecimalRounding(ctx, col, row[idx], converted, i.rowNum) // s2z patch
				if str, ok := row[idx].(string); ok && types.IsTimespan(col.Type) {
					// s2z patch: a TIME string with more after it
					if err := s2zTimeStringWarnings(ctx, col, str, i.rowNum, strictMode(ctx) && !i.ignore); err != nil {
						return nil, sql.NewWrappedInsertError(origRow, err)
					}
				}
			}
			if cErr != nil {
				// Ignore individual column errors when INSERT IGNORE, UPDATE IGNORE, etc. is specified.
				// For JSON column types, always throw an error. MySQL throws the following error even when
				// IGNORE is specified:
				// ERROR 3140 (22032): Invalid JSON text: "Invalid value." at position 0 in value for column
				// 'table.column'.
				// s2z patch: and without strict mode, as MySQL
				if (i.ignore || !strictMode(ctx)) && col.Type.Type() != query.Type_JSON {
					if types.IsTimespan(col.Type) && converted != nil && sql.ErrValueOutOfRange.Is(cErr) {
						// s2z patch: a TIME out of range is its bound (or
						// 00:00:00), with MySQL's warning
						orig := row[idx]
						row[idx] = converted
						ctx.Session.Warn(ignoreWarning(col, orig, cErr, i.rowNum))
						continue
					}
					if _, ok := col.Type.(sql.NumberType); ok {
						orig := row[idx]
						if converted == nil {
							converted = i.schema[idx].Type.Zero()
						}
						row[idx] = converted
						// Add a warning instead (s2z patch: MySQL's)
						ctx.Session.Warn(ignoreWarning(col, orig, cErr, i.rowNum))
					} else {
						row = convertDataAndWarnRow(ctx, i.schema, row, idx, cErr, i.rowNum)
					}
					continue
				} else {
					// Fill in error with information
					if types.ErrLengthBeyondLimit.Is(cErr) {
						cErr = types.ErrLengthBeyondLimit.New(row[idx], col.Name)
					} else if sql.ErrNotMatchingSRID.Is(cErr) {
						cErr = sql.ErrNotMatchingSRIDWithColName.New(col.Name, cErr)
					} else if types.IsTimespan(col.Type) && (sql.ErrValueOutOfRange.Is(cErr) || types.ErrConvertingToTimeType.Is(cErr)) {
						// s2z patch: MySQL's error for a number that is no TIME
						cErr = mysql.NewSQLError(1292, "22007", "Incorrect time value: '%s' for column '%s' at row %d", s2zNumberText(row[idx]), col.Name, i.rowNum)
					} else if _, ok := col.Type.(sql.NumberType); ok && sql.ErrValueOutOfRange.Is(cErr) {
						// s2z patch: in strict mode, the warning MySQL gives
						// outside it is the error (1264)
						w := ignoreWarning(col, row[idx], cErr, i.rowNum)
						cErr = mysql.NewSQLError(int(w.Code), "22003", "%s", w.Message)
					}
					return nil, sql.NewWrappedInsertError(origRow, cErr)
				}
			}
			row[idx] = converted
		}
	}

	if i.replacer != nil {
		toReturn := make(sql.Row, len(row)*2)
		for i := 0; i < len(row); i++ {
			toReturn[i+len(row)] = row[i]
		}
		// May have multiple duplicate pk & unique errors due to multiple indexes
		//TODO: how does this interact with triggers?
		for {
			if err := i.replacer.Insert(ctx, row); err != nil {
				if !sql.ErrPrimaryKeyViolation.Is(err) && !sql.ErrUniqueKeyViolation.Is(err) {
					i.rowSource.Close(ctx)
					i.rowSource = nil
					return nil, sql.NewWrappedInsertError(row, err)
				}

				ue := err.(*errors.Error).Cause().(sql.UniqueKeyError)
				if err = i.replacer.Delete(ctx, ue.Existing); err != nil {
					i.rowSource.Close(ctx)
					i.rowSource = nil
					return nil, sql.NewWrappedInsertError(row, err)
				}
				// the row had to be deleted, write the values into the toReturn row
				copy(toReturn, ue.Existing)
			} else {
				break
			}
		}
		return toReturn, nil
	} else {
		if err := i.inserter.Insert(ctx, row); err != nil {
			if (!sql.ErrPrimaryKeyViolation.Is(err) && !sql.ErrUniqueKeyViolation.Is(err) && !sql.ErrDuplicateEntry.Is(err)) || len(i.updateExprs) == 0 {
				return nil, i.ignoreOrClose(ctx, row, err)
			}

			ue := err.(*errors.Error).Cause().(sql.UniqueKeyError)
			return i.handleOnDuplicateKeyUpdate(ctx, ue.Existing, row)
		}
	}

	i.updateLastInsertId(ctx, row)

	if len(i.returnExprs) > 0 {
		var retExprRow sql.Row
		for _, returnExpr := range i.returnExprs {
			result, err := returnExpr.Eval(ctx, row)
			if err != nil {
				return nil, err
			}
			retExprRow = append(retExprRow, result)
		}
		return retExprRow, nil
	}

	return row, nil
}

func (i *insertIter) handleOnDuplicateKeyUpdate(ctx *sql.Context, oldRow, newRow sql.Row) (returnRow sql.Row, returnErr error) {
	var err error
	updateAcc := append(oldRow, newRow...)
	var evalRow sql.Row
	for _, updateExpr := range i.updateExprs {
		// this SET <val> indexes into LHS, but the <expr> can
		// reference the new row on RHS
		val, err := updateExpr.Eval(i.ctx, updateAcc)
		if err != nil {
			if i.ignore {
				idx, ok := getFieldIndexFromUpdateExpr(updateExpr)
				if !ok {
					return nil, err
				}

				val = convertDataAndWarn(ctx, i.schema, newRow, idx, err)
			} else {
				return nil, err
			}
		}

		updateAcc = val.(sql.Row)
	}
	// project LHS only
	evalRow = updateAcc[:len(oldRow)]

	// Should revaluate the check conditions.
	err = i.evaluateChecks(ctx, evalRow)
	if err != nil {
		return nil, i.ignoreOrClose(ctx, newRow, err)
	}

	err = i.updater.Update(ctx, oldRow, evalRow)
	if err != nil {
		return nil, i.ignoreOrClose(ctx, newRow, err)
	}

	// In the case that we attempted an update, return a concatenated [old,new] row just like update.
	return oldRow.Append(evalRow), nil
}

func getFieldIndexFromUpdateExpr(updateExpr sql.Expression) (int, bool) {
	setField, ok := updateExpr.(*expression.SetField)
	if !ok {
		return 0, false
	}

	getField, ok := setField.LeftChild.(*expression.GetField)
	if !ok {
		return 0, false
	}

	return getField.Index(), true
}

// resolveValues resolves all VALUES functions.
func (i *insertIter) resolveValues(ctx *sql.Context, insertRow sql.Row) error {
	for _, updateExpr := range i.updateExprs {
		var err error
		sql.Inspect(updateExpr, func(expr sql.Expression) bool {
			valuesExpr, ok := expr.(*function.Values)
			if !ok {
				return true
			}
			getField, ok := valuesExpr.Child.(*expression.GetField)
			if !ok {
				err = fmt.Errorf("VALUES functions may only contain column names")
				return false
			}
			valuesExpr.Value = insertRow[getField.Index()]
			return false
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (i *insertIter) Close(ctx *sql.Context) error {
	if !i.closed {
		i.closed = true
		if i.unlocker != nil {
			i.unlocker()
		}
		var rsErr, iErr, rErr, uErr error
		if i.rowSource != nil {
			rsErr = i.rowSource.Close(ctx)
		}
		if i.inserter != nil {
			iErr = i.inserter.Close(ctx)
		}
		if i.replacer != nil {
			rErr = i.replacer.Close(ctx)
		}
		if i.updater != nil {
			uErr = i.updater.Close(ctx)
		}
		if rsErr != nil {
			return rsErr
		}
		if iErr != nil {
			return iErr
		}
		if rErr != nil {
			return rErr
		}
		if uErr != nil {
			return uErr
		}
	}
	return nil
}

func (i *insertIter) updateLastInsertId(ctx *sql.Context, row sql.Row) {
	if i.firstGeneratedAutoIncRowIdx < 0 {
		return
	}
	if i.firstGeneratedAutoIncRowIdx == 0 {
		autoIncVal := i.getAutoIncVal(row)
		ctx.SetLastQueryInfoInt(sql.LastInsertId, autoIncVal)
	}
	i.firstGeneratedAutoIncRowIdx--
}

func (i *insertIter) getAutoIncVal(row sql.Row) int64 {
	for i, expr := range i.insertExprs {
		if _, ok := expr.(*expression.AutoIncrement); ok {
			return toInt64(row[i])
		}
	}
	return 0
}

func (i *insertIter) ignoreOrClose(ctx *sql.Context, row sql.Row, err error) error {
	if !i.ignore {
		return sql.NewWrappedInsertError(row, err)
	}

	return warnOnIgnorableError(ctx, row, err)
}

// convertDataAndWarn modifies a row with data conversion issues in INSERT/UPDATE IGNORE calls
// Per MySQL docs "Rows set to values that would cause data conversion errors are set to the closest valid values instead"
// cc. https://dev.mysql.com/doc/refman/8.0/en/sql-mode.html#sql-mode-strict
func convertDataAndWarn(ctx *sql.Context, tableSchema sql.Schema, row sql.Row, columnIdx int, err error) sql.Row {
	return convertDataAndWarnRow(ctx, tableSchema, row, columnIdx, err, 1)
}

// convertDataAndWarnRow is convertDataAndWarn for row number rowNum, with
// MySQL's warning (s2z patch).
func convertDataAndWarnRow(ctx *sql.Context, tableSchema sql.Schema, row sql.Row, columnIdx int, err error, rowNum int) sql.Row {
	orig := row[columnIdx]
	if types.ErrLengthBeyondLimit.Is(err) {
		maxLength := tableSchema[columnIdx].Type.(sql.StringType).MaxCharacterLength()
		// s2z patch: a number is cut as its text, and by characters
		str, _, cerr := types.LongText.Convert(ctx, row[columnIdx])
		text, _ := str.(string)
		if cerr == nil && int64(len([]rune(text))) > maxLength {
			text = string([]rune(text)[:maxLength])
		}
		row[columnIdx] = text // truncate string
	} else if dt, ok := tableSchema[columnIdx].Type.(types.DecimalType_); ok && types.ErrConvertToDecimalLimit.Is(err) {
		// s2z patch: a DECIMAL out of range is its bound, as MySQL
		row[columnIdx] = tableSchema[columnIdx].Type.Zero()
		if c, ok := dt.S2zClamp(orig); ok {
			row[columnIdx] = c
		}
	} else {
		row[columnIdx] = tableSchema[columnIdx].Type.Zero()
	}

	// Add a warning instead
	if ctx != nil && ctx.Session != nil {
		ctx.Session.Warn(ignoreWarning(tableSchema[columnIdx], orig, err, rowNum))
	}

	return row
}

func warnOnIgnorableError(ctx *sql.Context, row sql.Row, err error) error {
	// Check that this error is a part of the list of Ignorable Errors and create the relevant warning
	for _, ie := range plan.IgnorableErrors {
		if ie.Is(err) {
			sqlerr := sql.CastSQLError(err)

			// Add a warning instead
			if ctx != nil && ctx.Session != nil {
				ctx.Session.Warn(&sql.Warning{
					Level:   "Warning", // s2z patch: as MySQL
					Code:    sqlerr.Num,
					Message: sqlerr.Message, // s2z patch: the error's MySQL text
				})
			}

			// In this case the default value gets updated so return nil
			if sql.ErrInsertIntoNonNullableDefaultNullColumn.Is(err) {
				return nil
			}

			// Return the InsertIgnore err to ensure our accumulator doesn't count this row.
			return sql.NewIgnorableError(row)
		}
	}

	return err
}

func (i *insertIter) evaluateChecks(ctx *sql.Context, row sql.Row) error {
	for _, check := range i.checks {
		if !check.Enforced {
			continue
		}

		res, err := sql.EvaluateCondition(ctx, check.Expr, row)

		if err != nil {
			return err
		}

		if sql.IsFalse(res) {
			return sql.ErrCheckConstraintViolated.New(check.Name)
		}
	}

	return nil
}

func (i *insertIter) validateNullability(ctx *sql.Context, dstSchema sql.Schema, row sql.Row) error {
	for count, col := range dstSchema {
		if !col.Nullable && row[count] == nil {
			// In the case of an IGNORE we set the nil value to a default and add a warning
			if !i.ignore {
				if i.deferredDefaults.Contains(count) {
					return sql.ErrInsertIntoNonNullableDefaultNullColumn.New(col.Name)
				}
				return sql.ErrInsertIntoNonNullableProvidedNull.New(col.Name)
			}
			row[count] = col.Type.Zero()
			_ = warnOnIgnorableError(ctx, row, sql.ErrInsertIntoNonNullableProvidedNull.New(col.Name)) // will always return nil
		}
	}
	return nil
}

func toInt64(x interface{}) int64 {
	switch x := x.(type) {
	case int:
		return int64(x)
	case uint:
		return int64(x)
	case int8:
		return int64(x)
	case uint8:
		return int64(x)
	case int16:
		return int64(x)
	case uint16:
		return int64(x)
	case int32:
		return int64(x)
	case uint32:
		return int64(x)
	case int64:
		return x
	case uint64:
		return int64(x)
	case float32:
		return int64(x)
	case float64:
		return int64(x)
	default:
		panic(fmt.Sprintf("Expected a numeric auto increment value, but got %T", x))
	}
}

// s2zNumberText is a number as MySQL quotes it in an error: a float as
// MySQL writes floats. (s2z patch)
func s2zNumberText(v interface{}) string {
	switch x := v.(type) {
	case float64:
		return types.MySQLFloatString(x, false)
	case float32:
		return types.MySQLFloatString(float64(x), true)
	case decimal.Decimal:
		return x.String()
	}
	return fmt.Sprint(v)
}

// s2zTimeStringWarnings gives MySQL's warnings for a string stored in a TIME
// column: 4096 for a space around it, 1265 (1292 in strict mode) for
// characters after it. (s2z patch)
func s2zTimeStringWarnings(ctx *sql.Context, col *sql.Column, s string, row int, strict bool) error {
	_, end, st := types.ParseTimeString(s)
	if st == types.TimeStringInvalid {
		return nil
	}
	if st == types.TimeStringTruncated && strict {
		return mysql.NewSQLError(1292, "22007", "Incorrect time value: '%s' for column '%s' at row %d", s, col.Name, row)
	}
	delim := func(pos int) {
		ctx.Session.Warn(&sql.Warning{Level: "Warning", Code: 4096, Message: fmt.Sprintf(
			"Delimiter ' ' in position %d in datetime value '%s' at row %d is superfluous and is deprecated. Please remove.", pos, s, row)})
	}
	if len(s) > 0 && s[0] == ' ' {
		delim(0)
	} else if end < len(s) && s[end] == ' ' && strings.ContainsRune(s[:end], ':') {
		delim(end)
	}
	if st == types.TimeStringTruncated {
		ctx.Session.Warn(&sql.Warning{Level: "Warning", Code: 1265, Message: fmt.Sprintf("Data truncated for column '%s' at row %d", col.Name, row)})
	}
	return nil
}
