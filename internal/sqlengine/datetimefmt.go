package sqlengine

import (
	"bytes"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	querypb "github.com/dolthub/vitess/go/vt/proto/query"
)

// go-mysql-server prints a DATETIME(n) or TIMESTAMP(n) value without its
// fraction's trailing zeros ('...10.12', '...10'); MySQL prints n digits
// ('...10.120000', '...10.000000'). padFractions wraps a result callback to
// pad them to the column's decimals. A query's later callbacks carry no
// fields, so the first one's are kept.
func padFractions(callback mysql.ResultSpoolFn) mysql.ResultSpoolFn {
	var pad []int // decimals per column, 0 for none
	return func(res *sqltypes.Result, more bool) error {
		if res != nil && len(res.Fields) > 0 {
			pad = nil
			for i, f := range res.Fields {
				if (f.Type == querypb.Type_DATETIME || f.Type == querypb.Type_TIMESTAMP) && f.Decimals > 0 && f.Decimals <= 6 {
					if pad == nil {
						pad = make([]int, len(res.Fields))
					}
					pad[i] = int(f.Decimals)
				}
			}
		}
		if res != nil && pad != nil {
			for _, row := range res.Rows {
				for i, n := range pad {
					if n > 0 && i < len(row) && !row[i].IsNull() {
						row[i] = sqltypes.MakeTrusted(row[i].Type(), padFraction(row[i].Raw(), n))
					}
				}
			}
		}
		return callback(res, more)
	}
}

func padFraction(v []byte, n int) []byte {
	have := 0
	if i := bytes.LastIndexByte(v, '.'); i >= 0 {
		have = len(v) - i - 1
	}
	if have >= n {
		return v
	}
	out := make([]byte, 0, len(v)+n-have+1)
	out = append(out, v...)
	if have == 0 && bytes.IndexByte(v, '.') < 0 {
		out = append(out, '.')
	}
	for ; have < n; have++ {
		out = append(out, '0')
	}
	return out
}
