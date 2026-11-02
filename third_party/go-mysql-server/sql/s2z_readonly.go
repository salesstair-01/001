// s2z patch: READ ONLY sessions and next transactions.

package sql

import "strings"

// NextTransactionVar is the user variable SET TRANSACTION READ ONLY | READ
// WRITE (without SESSION or GLOBAL) sets: it holds for the next
// transaction only, as in MySQL.
const NextTransactionVar = "s2z_next_transaction_read_only"

// SessionReadOnly is whether the session's transactions are READ ONLY
// (SET SESSION TRANSACTION READ ONLY, transaction_read_only = 1).
func SessionReadOnly(ctx *Context) bool {
	if ctx == nil || ctx.Session == nil {
		return false
	}
	v, err := ctx.Session.GetSessionVariable(ctx, "transaction_read_only")
	if err != nil {
		return false
	}
	switch x := v.(type) {
	case int8:
		return x != 0
	case int64:
		return x != 0
	case bool:
		return x
	case string:
		return x == "1" || strings.EqualFold(x, "on")
	}
	return false
}

// StartCharacteristic is the characteristic of a transaction the
// session starts: READ ONLY when SET TRANSACTION said so for it (the
// setting is then used up), or when the session's transactions are,
// unless the statement asked for one explicitly.
func StartCharacteristic(ctx *Context, explicit TransactionCharacteristic) TransactionCharacteristic {
	if explicit == ReadOnly {
		return explicit
	}
	if ctx != nil && ctx.Session != nil {
		if _, v, err := ctx.GetUserVariable(ctx, NextTransactionVar); err == nil && v != nil {
			_ = ctx.SetUserVariable(ctx, NextTransactionVar, nil, nil)
			if b, ok := v.(bool); ok {
				if b {
					return ReadOnly
				}
				return ReadWrite
			}
		}
	}
	if SessionReadOnly(ctx) {
		return ReadOnly
	}
	return explicit
}
