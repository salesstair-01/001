package encoding

import (
	"bytes"
	"math"
	"math/rand"
	"sort"
	"testing"
)

func TestIntOrder(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	vals := []int64{math.MinInt64, -1, 0, 1, math.MaxInt64}
	for i := 0; i < 2000; i++ {
		vals = append(vals, r.Int63()-r.Int63())
	}
	for _, desc := range []bool{false, true} {
		checkOrder(t, len(vals), func(i, j int) bool {
			if desc {
				return vals[i] > vals[j]
			}
			return vals[i] < vals[j]
		}, func(i int) []byte { return AppendInt(nil, vals[i], desc) })
	}
	for _, v := range vals {
		got, rest, err := DecodeInt(AppendInt(nil, v, true), true)
		if err != nil || got != v || len(rest) != 0 {
			t.Fatalf("round trip %d: %d %v", v, got, err)
		}
	}
}

func TestFloatOrder(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	vals := []float64{math.Inf(-1), -1e300, -1, -1e-300, 0, 1e-300, 1, 1e300, math.Inf(1)}
	for i := 0; i < 2000; i++ {
		vals = append(vals, r.NormFloat64()*math.Pow(10, float64(r.Intn(40)-20)))
	}
	checkOrder(t, len(vals), func(i, j int) bool { return vals[i] < vals[j] },
		func(i int) []byte { return AppendFloat(nil, vals[i], false) })
	if !bytes.Equal(AppendFloat(nil, math.Copysign(0, -1), false), AppendFloat(nil, 0, false)) {
		t.Fatal("-0 must encode as 0")
	}
	for _, v := range vals {
		got, _, err := DecodeFloat(AppendFloat(nil, v, false), false)
		if err != nil || got != v {
			t.Fatalf("round trip %v: %v", v, got)
		}
	}
}

func TestBytesOrderAndSelfDelimiting(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	type pair struct{ a, b []byte }
	var vals []pair
	gen := func() []byte {
		n := r.Intn(5)
		b := make([]byte, n)
		for i := range b {
			b[i] = []byte{0x00, 0x01, 0xFF, 'a', 'b'}[r.Intn(5)]
		}
		return b
	}
	for i := 0; i < 3000; i++ {
		vals = append(vals, pair{gen(), gen()})
	}
	// Composite keys (a, b) must sort exactly like the tuple order.
	for _, desc := range []bool{false, true} {
		checkOrder(t, len(vals), func(i, j int) bool {
			c := bytes.Compare(vals[i].a, vals[j].a)
			if c == 0 {
				c = bytes.Compare(vals[i].b, vals[j].b)
			}
			if desc {
				return c > 0
			}
			return c < 0
		}, func(i int) []byte {
			k := AppendBytes(nil, vals[i].a, desc)
			return AppendBytes(k, vals[i].b, desc)
		})
	}
	for _, p := range vals {
		k := AppendBytes(AppendBytes(nil, p.a, true), p.b, true)
		a, rest, err := DecodeBytes(k, true)
		if err != nil || !bytes.Equal(a, p.a) {
			t.Fatalf("decode a: %v", err)
		}
		b, rest, err := DecodeBytes(rest, true)
		if err != nil || !bytes.Equal(b, p.b) || len(rest) != 0 {
			t.Fatalf("decode b: %v", err)
		}
	}
}

func TestNullOrder(t *testing.T) {
	null := AppendNull(nil, false)
	v := AppendInt(nil, math.MinInt64, false)
	if bytes.Compare(null, v) >= 0 {
		t.Fatal("NULL must sort first ascending")
	}
	nullD := AppendNull(nil, true)
	vD := AppendInt(nil, math.MaxInt64, true)
	if bytes.Compare(nullD, vD) <= 0 {
		t.Fatal("NULL must sort last descending")
	}
}

func TestMVCCOrder(t *testing.T) {
	uk := AppendInt(Prefix(7, 1024, 0), 5, false)
	k1 := MVCCKey(uk, 1)
	k2 := MVCCKey(uk, 2)
	if bytes.Compare(k2, k1) >= 0 {
		t.Fatal("newer version must sort first")
	}
	if Split(k1) != len(uk) {
		t.Fatal("split")
	}
	gotUK, ts, err := SplitMVCC(k2)
	if err != nil || ts != 2 || !bytes.Equal(gotUK, uk) {
		t.Fatal("SplitMVCC")
	}
	// A later user key sorts after every version of an earlier user key.
	uk2 := AppendInt(Prefix(7, 1024, 0), 6, false)
	if bytes.Compare(MVCCKey(uk2, math.MaxUint64), MVCCKey(uk, 0)) <= 0 {
		t.Fatal("user key order")
	}
}

func TestPrefixEnd(t *testing.T) {
	if !bytes.Equal(PrefixEnd([]byte{1, 2, 0xFF}), []byte{1, 3}) {
		t.Fatal("prefix end")
	}
	if PrefixEnd([]byte{0xFF, 0xFF}) != nil {
		t.Fatal("prefix end of all-FF")
	}
}

func TestRowRoundTrip(t *testing.T) {
	for _, large := range []bool{false, true} {
		cols := []RowValue{
			{ID: 3, Bytes: []byte("hello")},
			{ID: 1, Bytes: ValueInt(-42)},
			{ID: 2, Null: true},
			{ID: 9, Bytes: nil},
		}
		if large {
			cols = append(cols, RowValue{ID: 300, Bytes: make([]byte, 70000)})
		}
		b, err := EncodeRow(cols)
		if err != nil {
			t.Fatal(err)
		}
		r, err := DecodeRow(b)
		if err != nil {
			t.Fatal(err)
		}
		st, v, _ := r.Lookup(1)
		if n, _ := ParseInt(v); st != ColValue || n != -42 {
			t.Fatal("col 1")
		}
		if st, _, _ := r.Lookup(2); st != ColNull {
			t.Fatal("col 2")
		}
		if st, v, _ := r.Lookup(3); st != ColValue || string(v) != "hello" {
			t.Fatal("col 3")
		}
		if st, v, _ := r.Lookup(9); st != ColValue || len(v) != 0 {
			t.Fatal("col 9 empty value")
		}
		if st, _, _ := r.Lookup(4); st != ColMissing {
			t.Fatal("col 4 should be missing")
		}
		if large {
			if st, v, _ := r.Lookup(300); st != ColValue || len(v) != 70000 {
				t.Fatal("col 300")
			}
		}
	}
	if _, err := EncodeRow([]RowValue{{ID: 1, Bytes: make([]byte, MaxRowSize)}}); err != ErrRowTooLarge {
		t.Fatal("expected too large")
	}
}

func checkOrder(t *testing.T, n int, less func(i, j int) bool, enc func(i int) []byte) {
	t.Helper()
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}
	byValue := append([]int(nil), idx...)
	sort.SliceStable(byValue, func(a, b int) bool { return less(byValue[a], byValue[b]) })
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = enc(i)
	}
	for i := 1; i < n; i++ {
		a, b := byValue[i-1], byValue[i]
		c := bytes.Compare(keys[a], keys[b])
		if less(a, b) && c >= 0 {
			t.Fatalf("order mismatch between %d and %d", a, b)
		}
		if !less(a, b) && !less(b, a) && c != 0 {
			t.Fatalf("equal values encode differently: %d %d", a, b)
		}
	}
}

func TestDecimalOrder(t *testing.T) {
	// Values as (sign, exp, digits) for sign × 0.digits × 10^exp, in ascending order.
	type dec struct {
		sign   int
		exp    int32
		digits string
	}
	ordered := []dec{
		{-1, 3, "1"},   // -100
		{-1, 1, "123"}, // -1.23
		{-1, 1, "12"},  // -1.2
		{-1, 0, "5"},   // -0.5
		{-1, -2, "1"},  // -0.001
		{0, 0, ""},     // 0
		{1, -2, "1"},   // 0.001
		{1, 0, "5"},    // 0.5
		{1, 1, "12"},   // 1.2
		{1, 1, "123"},  // 1.23
		{1, 3, "1"},    // 100
	}
	for i := 1; i < len(ordered); i++ {
		a, b := ordered[i-1], ordered[i]
		ka := AppendDecimal(nil, a.sign, a.exp, a.digits, false)
		kb := AppendDecimal(nil, b.sign, b.exp, b.digits, false)
		if bytes.Compare(ka, kb) >= 0 {
			t.Fatalf("%v should sort before %v", a, b)
		}
	}
	if !bytes.Equal(AppendDecimal(nil, 1, 1, "15", false), AppendDecimal(nil, 1, 1, "150", false)) {
		t.Fatal("1.5 and 1.50 must encode the same")
	}
}

func TestSkipColumn(t *testing.T) {
	for _, desc := range []bool{false, true} {
		var k []byte
		k = AppendInt(k, -7, desc)
		k = AppendNull(k, desc)
		k = AppendBytes(k, []byte{0, 1, 0xFF, 'x'}, desc)
		k = AppendDecimal(k, -1, 2, "125", desc)
		k = AppendDecimal(k, 0, 0, "", desc)
		k = AppendDecimal(k, 1, -3, "9", desc)
		k = AppendFloat(k, 2.5, desc)
		kinds := []ColumnKind{KindFixed8, KindBytes, KindBytes, KindDecimal, KindDecimal, KindDecimal, KindFixed8}
		rest := k
		for i, kind := range kinds {
			n, err := SkipColumn(rest, kind, desc)
			if err != nil {
				t.Fatalf("desc=%v col %d: %v", desc, i, err)
			}
			rest = rest[n:]
		}
		if len(rest) != 0 {
			t.Fatalf("desc=%v: %d bytes left", desc, len(rest))
		}
	}
}
