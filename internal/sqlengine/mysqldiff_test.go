package sqlengine

// Differential property tests against a real MySQL 8 (slice 5): the same
// seeded random tables and queries run on MySQL and on this engine, and the
// results must match. They check the key encoding against MySQL's own
// ordering and equality: collation weights, numeric and temporal order, range
// scans over indexes, and unique-key conflicts.
//
//	S2Z_MYSQL_DSN='root@tcp(127.0.0.1:3310)/' go test ./internal/sqlengine -run MySQLDiff -diff-seed 1 -diff-rounds 20

import (
	"database/sql"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

var (
	diffSeed   = flag.Int64("diff-seed", 1, "first seed of the MySQL differential tests")
	diffRounds = flag.Int("diff-rounds", 5, "seeds per MySQL differential test")
	diffGaps   = flag.Bool("diff-known-gaps", false, "also generate the inputs of the known collation gaps (docs/compatibility.md)")
)

// Known gaps, inherited from go-mysql-server's collations (one weight per
// character): MySQL's expansions (ß and ẞ sort as ss, æ as ae, ﬁ as fi), and
// PAD SPACE (utf8mb4_bin and utf8mb4_general_ci ignore trailing spaces).
// They are left out of the generated data unless -diff-known-gaps is set.
var expansionPieces = map[string]bool{"ß": true, "ẞ": true, "æ": true, "ﬁ": true}

var padSpaceCollations = map[string]bool{"utf8mb4_bin": true, "utf8mb4_general_ci": true}

type diffPair struct {
	t           *testing.T
	mysql, ours *sql.DB
	mismatches  int
}

func newDiffPair(t *testing.T) *diffPair {
	dsn := os.Getenv("S2Z_MYSQL_DSN")
	if dsn == "" {
		t.Skip("S2Z_MYSQL_DSN is not set")
	}
	my, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { my.Close() })
	my.SetMaxOpenConns(1) // keep the session's database and sql_mode
	for _, q := range []string{"DROP DATABASE IF EXISTS s2zdiff", "CREATE DATABASE s2zdiff", "USE s2zdiff",
		"SET SESSION sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'"} {
		if _, err := my.Exec(q); err != nil {
			t.Fatalf("mysql: %s: %v", q, err)
		}
	}
	// S2Z_KVD_ADDR runs our side against an external kvd (either owner), on
	// a fresh partition.
	c := startClusterOn(t, os.Getenv("S2Z_KVD_ADDR"), uint64(time.Now().UnixNano()%(1<<40))+1<<20)
	c.db(t)                                                    // waits for the server
	ours, err := sql.Open("mysql", "root@tcp("+c.addr+")/app") // text values, as from MySQL
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ours.Close() })
	ours.SetMaxOpenConns(1)
	return &diffPair{t: t, mysql: my, ours: ours}
}

// exec runs q on both; it reports whether both succeeded, and a mismatch
// when only one did.
func (d *diffPair) exec(q string, args ...any) bool {
	d.t.Helper()
	_, e1 := d.mysql.Exec(q, args...)
	_, e2 := d.ours.Exec(q, args...)
	if (e1 == nil) != (e2 == nil) {
		d.mismatch("%s %v\n  mysql: %v\n  ours:  %v", q, args, e1, e2)
	}
	return e1 == nil && e2 == nil
}

// must runs q on both and fails the test if either errors.
func (d *diffPair) must(q string) {
	d.t.Helper()
	if _, err := d.mysql.Exec(q); err != nil {
		d.t.Fatalf("mysql: %s: %v", q, err)
	}
	if _, err := d.ours.Exec(q); err != nil {
		d.t.Fatalf("ours: %s: %v", q, err)
	}
}

// query runs q on both and compares the rows, each normalised by norm.
func (d *diffPair) query(q string, norm func(col int, v sql.NullString) string, args ...any) {
	d.t.Helper()
	a, e1 := rowsOf(d.mysql, q, norm, args...)
	b, e2 := rowsOf(d.ours, q, norm, args...)
	if e1 != nil || e2 != nil {
		if (e1 == nil) != (e2 == nil) {
			d.mismatch("%s %v\n  mysql: %v\n  ours:  %v", q, args, e1, e2)
		}
		return
	}
	if strings.Join(a, "\n") != strings.Join(b, "\n") {
		d.mismatch("%s %v\n  mysql: %s\n  ours:  %s", q, args, short(a), short(b))
	}
}

func (d *diffPair) mismatch(f string, args ...any) {
	d.t.Helper()
	d.mismatches++
	if d.mismatches <= 20 {
		d.t.Errorf(f, args...)
	}
}

func short(rows []string) string {
	s := strings.Join(rows, " | ")
	if len(s) > 600 {
		s = s[:600] + "..."
	}
	return fmt.Sprintf("(%d rows) %s", len(rows), s)
}

func rowsOf(db *sql.DB, q string, norm func(int, sql.NullString) string, args ...any) ([]string, error) {
	rows, err := db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	var out []string
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		parts := make([]string, len(cols))
		for i, v := range vals {
			parts[i] = norm(i, v)
		}
		out = append(out, strings.Join(parts, ","))
	}
	return out, rows.Err()
}

func plain(_ int, v sql.NullString) string {
	if !v.Valid {
		return "NULL"
	}
	return v.String
}

// ---- collations -----------------------------------------------------------------

// collationPieces mixes the cases that separate collations: case, accents,
// ß/ss, trailing spaces (PAD SPACE vs NO PAD), punctuation, Turkish i, and
// characters outside the BMP.
var collationPieces = []string{
	"a", "A", "á", "Á", "à", "ä", "Ä", "æ", "b", "B", "e", "é", "E", "É", "i", "I", "ı", "İ",
	"o", "ö", "Ö", "ø", "s", "S", "ß", "ss", "SS", "ẞ", "z", "Z", "ž", "ﬁ", "fi",
	" ", "  ", "-", "_", ".", "'", "0", "1", "9", "10", "😀", "😃", "中", "日", " ", "",
}

func randString(r *rand.Rand) string {
	n := r.Intn(5)
	var b strings.Builder
	for i := 0; i < n; i++ {
		p := collationPieces[r.Intn(len(collationPieces))]
		if expansionPieces[p] && !*diffGaps {
			p = "x"
		}
		b.WriteString(p)
	}
	return b.String()
}

// randStringFor is randString without trailing spaces for PAD SPACE
// collations, unless the known gaps are wanted.
func randStringFor(r *rand.Rand, coll string) string {
	s := randString(r)
	if padSpaceCollations[coll] && !*diffGaps {
		s = strings.TrimRight(s, " ")
	}
	return s
}

func TestMySQLDiffCollations(t *testing.T) {
	d := newDiffPair(t)
	collations := []string{"utf8mb4_0900_ai_ci", "utf8mb4_general_ci", "utf8mb4_0900_bin", "utf8mb4_bin"}
	for seed := *diffSeed; seed < *diffSeed+int64(*diffRounds); seed++ {
		r := rand.New(rand.NewSource(seed))
		for ci, coll := range collations {
			tbl := fmt.Sprintf("c_%d_%d", seed, ci)
			d.must(fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, s VARCHAR(40) COLLATE %s, KEY ks (s))", tbl, coll))
			var vals []string
			for id := 1; id <= 60; id++ {
				v := randStringFor(r, coll)
				vals = append(vals, v)
				if r.Intn(10) == 0 {
					d.must(fmt.Sprintf("INSERT INTO %s VALUES (%d, NULL)", tbl, id))
					continue
				}
				d.exec(fmt.Sprintf("INSERT INTO %s VALUES (?, ?)", tbl), id, v)
			}
			// Order and equality, with ties broken by id.
			d.query(fmt.Sprintf("SELECT id FROM %s ORDER BY s, id", tbl), plain)
			d.query(fmt.Sprintf("SELECT MIN(id), COUNT(*) FROM %s GROUP BY s ORDER BY MIN(id)", tbl), plain)
			// Index ranges around random pivots.
			for k := 0; k < 8; k++ {
				p := vals[r.Intn(len(vals))]
				for _, op := range []string{"=", "<", "<=", ">", ">="} {
					d.query(fmt.Sprintf("SELECT id FROM %s WHERE s %s ? ORDER BY id", tbl, op), plain, p)
				}
				// IGNORE INDEX: MySQL 8.0's LIKE range over a utf8mb4_bin index
				// stops at U+FFFF and drops strings continuing with emoji.
				d.query(fmt.Sprintf("SELECT id FROM %s IGNORE INDEX (ks) WHERE s LIKE ? ORDER BY id", tbl), plain, p+"%")
			}
			// Unique-key conflicts follow collation equality.
			d.exec(fmt.Sprintf("CREATE UNIQUE INDEX us ON %s (s)", tbl))
			for k := 0; k < 10; k++ {
				d.exec(fmt.Sprintf("INSERT INTO %s VALUES (?, ?)", tbl), 1000+k, randStringFor(r, coll))
			}
		}
	}
	t.Logf("%d mismatches", d.mismatches)
}

// ---- types ------------------------------------------------------------------------

type diffType struct {
	ddl  string
	gen  func(r *rand.Rand) any
	norm func(int, sql.NullString) string
}

// noCompareTypes skip comparisons with literals: known gap (an ENUM compares
// by member index in go-mysql-server, as a string in MySQL).
var noCompareTypes = map[string]bool{"ENUM('b','a','c')": true}

// normFraction trims trailing zeros of fractional seconds: go-mysql-server
// prints DATETIME(6) as .12, MySQL as .120000 (known gap).
func normFraction(_ int, v sql.NullString) string {
	if !v.Valid {
		return "NULL"
	}
	if strings.Contains(v.String, ".") {
		return strings.TrimRight(strings.TrimRight(v.String, "0"), ".")
	}
	return v.String
}

func pick[T any](r *rand.Rand, xs ...T) T { return xs[r.Intn(len(xs))] }

func normFloat(bits int) func(int, sql.NullString) string {
	return func(_ int, v sql.NullString) string {
		if !v.Valid {
			return "NULL"
		}
		f, err := strconv.ParseFloat(v.String, bits)
		if err != nil {
			return v.String
		}
		return strconv.FormatFloat(f, 'g', -1, bits)
	}
}

func normSig(digits int) func(int, sql.NullString) string {
	return func(_ int, v sql.NullString) string {
		if !v.Valid {
			return "NULL"
		}
		// Round the float32 itself, as MySQL does, not its shortest text.
		f, err := strconv.ParseFloat(v.String, 32)
		if err != nil {
			return v.String
		}
		return strconv.FormatFloat(float64(float32(f)), 'g', digits, 64)
	}
}

var diffTypes = []diffType{
	{"TINYINT", func(r *rand.Rand) any { return r.Intn(256) - 128 }, plain},
	{"TINYINT UNSIGNED", func(r *rand.Rand) any { return r.Intn(256) }, plain},
	{"SMALLINT", func(r *rand.Rand) any { return r.Intn(65536) - 32768 }, plain},
	{"INT", func(r *rand.Rand) any {
		return pick(r, int64(math.MinInt32), int64(math.MaxInt32), 0, -1, r.Int63n(1<<32)-1<<31)
	}, plain},
	{"BIGINT", func(r *rand.Rand) any {
		return pick(r, int64(math.MinInt64), int64(math.MaxInt64), 0, -1, r.Int63()-r.Int63())
	}, plain},
	// Sent as integers: go-mysql-server compares a BIGINT UNSIGNED with a
	// string through float64, which is inexact near 2^64 (known gap).
	{"BIGINT UNSIGNED", func(r *rand.Rand) any { return pick(r, uint64(math.MaxUint64), 0, 1, r.Uint64()) }, plain},
	{"DECIMAL(12,4)", func(r *rand.Rand) any {
		return pick(r, "0", "-0.0001", "99999999.9999", "-99999999.9999", fmt.Sprintf("%d.%04d", r.Intn(2000000)-1000000, r.Intn(10000)))
	}, plain},
	{"DECIMAL(30,10)", func(r *rand.Rand) any {
		return fmt.Sprintf("%s%d%09d.%010d", pick(r, "", "-"), r.Intn(1000), r.Intn(1000000000), r.Int63n(10000000000))
	}, plain},
	{"DOUBLE", func(r *rand.Rand) any { return pick(r, 0.0, -0.5, 1e300, -1e-300, r.NormFloat64()*1e6) }, normFloat(64)},
	// MySQL prints FLOAT with 6 significant digits (go-mysql-server prints all).
	{"FLOAT", func(r *rand.Rand) any { return pick(r, float32(0), float32(-1.5), float32(r.NormFloat64()*1000)) }, normSig(6)},
	{"DATE", func(r *rand.Rand) any {
		return fmt.Sprintf("%04d-%02d-%02d", pick(r, 1000, 1969, 1970, 2000+r.Intn(50), 9999), 1+r.Intn(12), 1+r.Intn(28))
	}, plain},
	{"DATETIME(6)", func(r *rand.Rand) any {
		return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d", 1900+r.Intn(200), 1+r.Intn(12), 1+r.Intn(28), r.Intn(24), r.Intn(60), r.Intn(60), r.Intn(1000000))
	}, normFraction},
	{"TIME", func(r *rand.Rand) any {
		return fmt.Sprintf("%s%02d:%02d:%02d", pick(r, "", "-"), r.Intn(839), r.Intn(60), r.Intn(60))
	}, plain},
	{"YEAR", func(r *rand.Rand) any { return 1901 + r.Intn(255) }, plain},
	{"CHAR(8)", func(r *rand.Rand) any { return randString(r) }, plain},
	{"VARBINARY(16)", func(r *rand.Rand) any {
		b := make([]byte, r.Intn(6))
		r.Read(b)
		if r.Intn(3) == 0 {
			b = append(b, 0, 0)
		}
		return b
	}, func(_ int, v sql.NullString) string {
		if !v.Valid {
			return "NULL"
		}
		return fmt.Sprintf("%x", v.String)
	}},
	{"BINARY(4)", func(r *rand.Rand) any { b := make([]byte, r.Intn(4)); r.Read(b); return b },
		func(_ int, v sql.NullString) string {
			if !v.Valid {
				return "NULL"
			}
			return fmt.Sprintf("%x", v.String)
		}},
	{"ENUM('b','a','c')", func(r *rand.Rand) any { return pick(r, "a", "b", "c") }, plain},
	{"BIT(8)", func(r *rand.Rand) any { return r.Intn(256) }, func(_ int, v sql.NullString) string {
		if !v.Valid {
			return "NULL"
		}
		return fmt.Sprintf("%x", v.String)
	}},
}

func TestMySQLDiffTypes(t *testing.T) {
	d := newDiffPair(t)
	for seed := *diffSeed; seed < *diffSeed+int64(*diffRounds); seed++ {
		r := rand.New(rand.NewSource(seed))
		for ti, ty := range diffTypes {
			tbl := fmt.Sprintf("t_%d_%d", seed, ti)
			d.must(fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, v %s, KEY kv (v))", tbl, ty.ddl))
			var vals []any
			for id := 1; id <= 40; id++ {
				v := ty.gen(r)
				if r.Intn(12) == 0 {
					v = nil
				}
				vals = append(vals, v)
				d.exec(fmt.Sprintf("INSERT INTO %s VALUES (?, ?)", tbl), id, v)
			}
			norm := func(col int, v sql.NullString) string {
				if col == 0 {
					return plain(col, v)
				}
				return ty.norm(col, v)
			}
			d.query(fmt.Sprintf("SELECT id, v FROM %s ORDER BY id", tbl), norm) // round trip
			d.query(fmt.Sprintf("SELECT id FROM %s ORDER BY v, id", tbl), plain)
			d.query(fmt.Sprintf("SELECT id FROM %s ORDER BY v DESC, id", tbl), plain)
			for k := 0; k < 6 && !(noCompareTypes[ty.ddl] && !*diffGaps); k++ {
				p := vals[r.Intn(len(vals))]
				if p == nil {
					continue
				}
				for _, op := range []string{"=", "<", ">="} {
					d.query(fmt.Sprintf("SELECT id FROM %s WHERE v %s ? ORDER BY id", tbl, op), plain, p)
				}
				q := vals[r.Intn(len(vals))]
				if q != nil {
					d.query(fmt.Sprintf("SELECT id FROM %s WHERE v BETWEEN ? AND ? ORDER BY id", tbl), plain, p, q)
				}
			}
			d.query(fmt.Sprintf("SELECT id FROM %s WHERE v IS NULL ORDER BY id", tbl), plain)
			// The same values as a primary key: duplicates and order.
			pk := fmt.Sprintf("p_%d_%d", seed, ti)
			d.must(fmt.Sprintf("CREATE TABLE %s (v %s NOT NULL PRIMARY KEY)", pk, ty.ddl))
			for _, v := range vals {
				if v != nil {
					d.exec(fmt.Sprintf("INSERT INTO %s VALUES (?)", pk), v)
				}
			}
			d.query(fmt.Sprintf("SELECT v FROM %s ORDER BY v", pk), ty.norm)
		}
	}
	t.Logf("%d mismatches", d.mismatches)
}
