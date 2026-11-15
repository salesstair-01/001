// Copyright 2020-2021 Dolthub, Inc.
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

package function

import (
	"context"
	"fmt"
	"math"

	"github.com/shopspring/decimal"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// Ceil returns the smallest integer value not less than X.
type Ceil struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*Ceil)(nil)
var _ sql.CollationCoercible = (*Ceil)(nil)

// NewCeil creates a new Ceil expression.
func NewCeil(num sql.Expression) sql.Expression {
	return &Ceil{expression.UnaryExpression{Child: num}}
}

// FunctionName implements sql.FunctionExpression
func (c *Ceil) FunctionName() string {
	return "ceil"
}

// Description implements sql.FunctionExpression
func (c *Ceil) Description() string {
	return "returns the smallest integer value that is greater than or equal to number."
}

// Type implements the Expression interface.
func (c *Ceil) Type() sql.Type {
	return s2zCeilFloorType(c.Child.Type()) // s2z patch: MySQL's
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Ceil) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (c *Ceil) String() string {
	return fmt.Sprintf("%s(%s)", c.FunctionName(), c.Child)
}

// WithChildren implements the Expression interface.
func (c *Ceil) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
	}
	return NewCeil(children[0]), nil
}

// Eval implements the Expression interface.
func (c *Ceil) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	v, err := c.evalS2z(ctx, row)
	return s2zCeilFloorValue(ctx, c.Type(), v, err)
}

func (c *Ceil) evalS2z(ctx *sql.Context, row sql.Row) (interface{}, error) {
	child, err := c.Child.Eval(ctx, row)

	if err != nil {
		return nil, err
	}

	if child == nil {
		return nil, nil
	}

	// non number type will be caught here
	if !types.IsNumber(c.Child.Type()) {
		child, _, err = types.Float64.Convert(ctx, child)
		if err != nil {
			return int32(0), nil
		}

		return int32(math.Ceil(child.(float64))), nil
	}

	// if it's number type and not float value, it does not need ceil-ing
	switch num := child.(type) {
	case float64:
		return math.Ceil(num), nil
	case float32:
		return float32(math.Ceil(float64(num))), nil
	case decimal.Decimal:
		return num.Ceil(), nil
	default:
		return child, nil
	}
}

// Floor returns the biggest integer value not less than X.
type Floor struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*Floor)(nil)
var _ sql.CollationCoercible = (*Floor)(nil)

// NewFloor returns a new Floor expression.
func NewFloor(num sql.Expression) sql.Expression {
	return &Floor{expression.UnaryExpression{Child: num}}
}

// FunctionName implements sql.FunctionExpression
func (f *Floor) FunctionName() string {
	return "floor"
}

// Description implements sql.FunctionExpression
func (f *Floor) Description() string {
	return "returns the largest integer value that is less than or equal to number."
}

// Type implements the Expression interface.
func (f *Floor) Type() sql.Type {
	return s2zCeilFloorType(f.Child.Type()) // s2z patch: MySQL's
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Floor) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (f *Floor) String() string {
	return fmt.Sprintf("%s(%s)", f.FunctionName(), f.Child)
}

// WithChildren implements the Expression interface.
func (f *Floor) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 1)
	}
	return NewFloor(children[0]), nil
}

// Eval implements the Expression interface.
func (f *Floor) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	v, err := f.evalS2z(ctx, row)
	return s2zCeilFloorValue(ctx, f.Type(), v, err)
}

func (f *Floor) evalS2z(ctx *sql.Context, row sql.Row) (interface{}, error) {
	child, err := f.Child.Eval(ctx, row)

	if err != nil {
		return nil, err
	}

	if child == nil {
		return nil, nil
	}

	// non number type will be caught here
	if !types.IsNumber(f.Child.Type()) {
		child, _, err = types.Float64.Convert(ctx, child)
		if err != nil {
			return int32(0), nil
		}

		return int32(math.Floor(child.(float64))), nil
	}

	// if it's number type and not float value, it does not need floor-ing
	switch num := child.(type) {
	case float64:
		return math.Floor(num), nil
	case float32:
		return float32(math.Floor(float64(num))), nil
	case decimal.Decimal:
		return num.Floor(), nil
	default:
		return child, nil
	}
}

// Round returns the number (x) with (d) requested decimal places.
// If d is negative, the number is returned with the (abs(d)) least significant
// digits of it's integer part set to 0. If d is not specified or nil/null
// it defaults to 0.
type Round struct {
	expression.BinaryExpressionStub
}

var _ sql.FunctionExpression = (*Round)(nil)
var _ sql.CollationCoercible = (*Round)(nil)

// NewRound returns a new Round expression.
func NewRound(args ...sql.Expression) (sql.Expression, error) {
	argLen := len(args)
	if argLen == 0 || argLen > 2 {
		return nil, sql.ErrInvalidArgumentNumber.New("ROUND", "1 or 2", argLen)
	}

	var right sql.Expression
	if len(args) == 2 {
		right = args[1]
	}

	return &Round{expression.BinaryExpressionStub{LeftChild: args[0], RightChild: right}}, nil
}

// FunctionName implements sql.FunctionExpression
func (r *Round) FunctionName() string {
	return "round"
}

// Description implements sql.FunctionExpression
func (r *Round) Description() string {
	return "rounds the number to decimals decimal places."
}

// Children implements the Expression interface.
func (r *Round) Children() []sql.Expression {
	if r.RightChild == nil {
		return []sql.Expression{r.LeftChild}
	}

	return r.BinaryExpressionStub.Children()
}

// Eval implements the Expression interface.
func (r *Round) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := r.LeftChild.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	if ts, ok := val.(types.Timespan); ok { // s2z patch: a TIME is its number
		val = types.TimespanDecimal(ts)
	}
	decType := types.MustCreateDecimalType(types.DecimalTypeMaxPrecision, types.DecimalTypeMaxScale)
	val, _, err = decType.Convert(ctx, val)
	if err != nil {
		// TODO: truncate
		return nil, err
	}

	prec := int32(0)
	if r.RightChild != nil {
		var tmp interface{}
		tmp, err = r.RightChild.Eval(ctx, row)
		if err != nil {
			return nil, err
		}

		if tmp == nil {
			return nil, nil
		}

		if tmp != nil {
			tmp, _, err = types.Int32.Convert(ctx, tmp)
			if err != nil {
				// TODO: truncate
				return nil, err
			}
			prec = tmp.(int32)
			// MySQL cuts off at 30 for larger values
			// TODO: these limits are fine only because we can't handle decimals larger than this
			if prec > types.DecimalTypeMaxPrecision {
				prec = types.DecimalTypeMaxPrecision
			}
			if prec < -types.DecimalTypeMaxScale {
				prec = -types.DecimalTypeMaxScale
			}
		}
	}

	var res interface{}
	tmp := val.(decimal.Decimal)
	if -tmp.Exponent() > prec {
		tmp = tmp.Round(prec) // s2z patch: more digits than it has add none
	}
	if types.IsSigned(r.LeftChild.Type()) && tmp.BigInt().IsInt64() {
		res = tmp.BigInt().Int64() // s2z patch: exactly
	} else if types.IsUnsigned(r.LeftChild.Type()) && tmp.BigInt().IsUint64() {
		res = tmp.BigInt().Uint64()
	} else if types.IsSigned(r.LeftChild.Type()) {
		res, _, err = types.Int64.Convert(ctx, tmp)
	} else if types.IsUnsigned(r.LeftChild.Type()) {
		res, _, err = types.Uint64.Convert(ctx, tmp)
	} else if types.IsFloat(r.LeftChild.Type()) {
		res, _, err = types.Float64.Convert(ctx, tmp)
	} else if types.IsDecimal(r.LeftChild.Type()) {
		res = tmp
	} else if types.IsTimespan(r.LeftChild.Type()) {
		// s2z patch: a TIME rounds as the DOUBLE it is, half to even
		f, _ := val.(decimal.Decimal).Float64()
		p := math.Pow10(int(prec))
		res = math.RoundToEven(f*p) / p
	} else if types.IsTextBlob(r.LeftChild.Type()) {
		res, _, err = types.Float64.Convert(ctx, tmp)
	}

	return res, err
}

// IsNullable implements the Expression interface.
func (r *Round) IsNullable() bool {
	return r.LeftChild.IsNullable()
}

func (r *Round) String() string {
	if r.RightChild == nil {
		return fmt.Sprintf("%s(%s,0)", r.FunctionName(), r.LeftChild.String())
	}

	return fmt.Sprintf("%s(%s,%s)", r.FunctionName(), r.LeftChild.String(), r.RightChild.String())
}

// Resolved implements the Expression interface.
func (r *Round) Resolved() bool {
	return r.LeftChild.Resolved() && (r.RightChild == nil || r.RightChild.Resolved())
}

// Type implements the Expression interface.
func (r *Round) Type() sql.Type {
	leftChildType := r.LeftChild.Type()
	if dt, ok := leftChildType.(sql.DecimalType); ok {
		// s2z patch: ROUND(x, n) of a DECIMAL keeps at most n decimals
		// (ROUND(2.567, 1) is 2.6, not 2.600)
		n := 0
		if r.RightChild != nil {
			lit, isLit := r.RightChild.(*expression.Literal)
			if !isLit {
				return leftChildType
			}
			v, _, err := types.Int64.Convert(context.Background(), lit.Value())
			if err != nil || v == nil {
				return leftChildType
			}
			n = int(max(v.(int64), 0))
		}
		if scale := int(dt.Scale()); n < scale {
			intDigits := int(dt.Precision()) - scale + 1 // rounding may carry
			if t, err := types.CreateDecimalType(uint8(min(intDigits+n, 65)), uint8(n)); err == nil {
				return t
			}
		}
		return leftChildType
	}
	if types.IsNumber(leftChildType) {
		return leftChildType
	}
	if types.IsTimespan(leftChildType) {
		return types.Float64 // s2z patch: MySQL's ROUND of a TIME is a DOUBLE
	}
	return types.Int32
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Round) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// WithChildren implements the Expression interface.
func (r *Round) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewRound(children...)
}

// s2zCeilFloorType is MySQL's type of CEILING / FLOOR: an integer's own, a
// DECIMAL's integer digits (plus one with a fraction) as BIGINT up to 18,
// else DECIMAL(n,0), and DOUBLE for anything else. (s2z patch)
func s2zCeilFloorType(t sql.Type) sql.Type {
	switch {
	case types.IsUnsigned(t):
		return types.Uint64
	case types.IsInteger(t):
		return types.Int64
	case types.IsDecimal(t):
		dt := t.(sql.DecimalType)
		n := int(dt.Precision()) - int(dt.Scale())
		if dt.Scale() > 0 {
			n++
		}
		if n <= 18 {
			return types.Int64
		}
		return types.MustCreateDecimalType(uint8(min(n, 65)), 0)
	}
	return types.Float64
}

func s2zCeilFloorValue(ctx *sql.Context, t sql.Type, v interface{}, err error) (interface{}, error) {
	if err != nil || v == nil {
		return v, err
	}
	r, _, cerr := t.Convert(ctx, v)
	if cerr != nil {
		return v, nil
	}
	return r, nil
}
