package kv

// A metamorphic test in the style of Pebble's (pebble/metamorphic): a seeded
// generator produces a sequence of operations together with the result each
// must have, computed by a small in-memory model of the transaction
// semantics, and the sequence is replayed against a real partition under
// several configurations that must not change any result: checkpoint
// frequency, local SST cache, memtable size and group-commit window.
//
// Operations: Begin (RR or RC, optionally a consistent snapshot), Get and
// Scan (plain, shared- or exclusive-locking, with and without the current
// statement's writes, scans paginated by a limit), Mutate (put, delete,
// insert-only, several keys per call), statement and transaction rollback,
// Commit, and storage events that must be invisible: Checkpoint, MVCC GC, and
// Restart, which closes the owner and takes the partition over again (open
// transactions are lost, committed data is not).
//
//	go test ./internal/kv -run Metamorphic -meta-seed 1 -meta-runs 50 -meta-ops 500

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/salesstair-01/001/internal/encoding"
	"github.com/salesstair-01/001/internal/objstore"
)

var (
	metaSeed = flag.Int64("meta-seed", 1, "first seed of the metamorphic test")
	metaRuns = flag.Int("meta-runs", 8, "seeds the metamorphic test runs")
	metaOps  = flag.Int("meta-ops", 300, "operations per seed")
)

const (
	metaKeys     = 12
	metaLockWait = time.Millisecond // conflicts time out at once; nothing waits in this single-threaded test
)

type metaOpKind int

const (
	opBegin metaOpKind = iota
	opGet
	opScan
	opMutate
	opNextStmt
	opRollbackStmt
	opRollback
	opCommit
	opCheckpoint
	opGC
	opRestart
)

type metaMut struct {
	op  MutationOp
	key int
	val string
}

// metaOp is one generated operation.
type metaOp struct {
	kind    metaOpKind
	txn     int // generator's transaction number
	iso     Isolation
	snap    bool // Begin with a consistent snapshot
	keys    []int
	lo, hi  int // scan [lo, hi)
	limit   int
	lock    LockMode
	include bool // reads see the current statement's writes
	muts    []metaMut
}

func (o metaOp) String() string {
	switch o.kind {
	case opBegin:
		iso := "RR"
		if o.iso == ReadCommitted {
			iso = "RC"
		}
		return fmt.Sprintf("t%d.Begin(%s, snapshot=%v)", o.txn, iso, o.snap)
	case opGet:
		return fmt.Sprintf("t%d.Get(%v, lock=%d, include=%v)", o.txn, o.keys, o.lock, o.include)
	case opScan:
		return fmt.Sprintf("t%d.Scan([%d,%d), limit=%d, lock=%d, include=%v)", o.txn, o.lo, o.hi, o.limit, o.lock, o.include)
	case opMutate:
		var parts []string
		for _, m := range o.muts {
			parts = append(parts, fmt.Sprintf("%s(%d,%q)", [...]string{"put", "del", "ins"}[m.op], m.key, m.val))
		}
		return fmt.Sprintf("t%d.Mutate(%s)", o.txn, strings.Join(parts, " "))
	case opNextStmt:
		return fmt.Sprintf("t%d.NextStatement()", o.txn)
	case opRollbackStmt:
		return fmt.Sprintf("t%d.RollbackStatement()", o.txn)
	case opRollback:
		return fmt.Sprintf("t%d.Rollback()", o.txn)
	case opCommit:
		return fmt.Sprintf("t%d.Commit()", o.txn)
	case opCheckpoint:
		return "Checkpoint()"
	case opGC:
		return "GC()"
	case opRestart:
		return "Restart()"
	}
	return "?"
}

// ---- the model ------------------------------------------------------------------

type metaLayer struct {
	stmt   uint64
	writes map[int]*string // nil = delete
	ins    map[int]bool    // written insert-only
}

type metaTxn struct {
	iso    Isolation
	stmt   uint64
	snap   map[int]string // nil until the first consistent read (RR)
	layers []*metaLayer
}

type metaLock struct {
	x int          // exclusive holder, -1 = none
	s map[int]bool // shared holders
}

type metaModel struct {
	committed map[int]string
	txns      map[int]*metaTxn
	locks     map[int]*metaLock
}

func newMetaModel() *metaModel {
	return &metaModel{committed: map[int]string{}, txns: map[int]*metaTxn{}, locks: map[int]*metaLock{}}
}

func (m *metaModel) copyCommitted() map[int]string {
	out := make(map[int]string, len(m.committed))
	for k, v := range m.committed {
		out[k] = v
	}
	return out
}

func (m *metaModel) lock(txn, key int, mode LockMode) bool {
	l := m.locks[key]
	if l == nil {
		l = &metaLock{x: -1, s: map[int]bool{}}
		m.locks[key] = l
	}
	if l.x != -1 && l.x != txn {
		return false
	}
	if mode == LockExclusive {
		for h := range l.s {
			if h != txn {
				return false
			}
		}
		l.x = txn
		return true
	}
	if l.x != txn {
		l.s[txn] = true
	}
	return true
}

func (m *metaModel) unlockAll(txn int) {
	for _, l := range m.locks {
		if l.x == txn {
			l.x = -1
		}
		delete(l.s, txn)
	}
}

// own returns the transaction's latest write to key, skipping the current
// statement unless include.
func (t *metaTxn) own(key int, include bool) (*string, bool) {
	for i := len(t.layers) - 1; i >= 0; i-- {
		l := t.layers[i]
		if !include && l.stmt == t.stmt {
			continue
		}
		if v, ok := l.writes[key]; ok {
			return v, true
		}
	}
	return nil, false
}

func (t *metaTxn) layer() *metaLayer {
	if n := len(t.layers); n > 0 && t.layers[n-1].stmt == t.stmt {
		return t.layers[n-1]
	}
	l := &metaLayer{stmt: t.stmt, writes: map[int]*string{}, ins: map[int]bool{}}
	t.layers = append(t.layers, l)
	return l
}

// base is the committed state a read sees: the snapshot, or the latest for a
// locking read.
func (m *metaModel) base(t *metaTxn, lock LockMode) map[int]string {
	if lock != LockNone {
		return m.committed
	}
	if t.snap == nil { // RR: the first consistent read; RC: the first in each statement
		t.snap = m.copyCommitted()
	}
	return t.snap
}

func fmtVal(v string, ok bool) string {
	if !ok {
		return "-"
	}
	return fmt.Sprintf("%q", v)
}

// apply runs op on the model and returns its expected result.
func (m *metaModel) apply(op metaOp) string {
	t := m.txns[op.txn]
	switch op.kind {
	case opBegin:
		t = &metaTxn{iso: op.iso, stmt: 1}
		if op.snap && op.iso == RepeatableRead { // RC takes a snapshot per statement regardless
			t.snap = m.copyCommitted()
		}
		m.txns[op.txn] = t
		return "ok"
	case opNextStmt:
		t.stmt++
		if t.iso == ReadCommitted {
			t.snap = nil
		}
		return "ok"
	case opGet:
		// Like the owner: the snapshot is taken (or refreshed for RC) first;
		// keys the transaction wrote are answered from its writes, unlocked;
		// the others are locked in order, if locking, and read.
		var base map[int]string
		if op.lock == LockNone {
			base = m.base(t, op.lock)
		}
		var out []string
		for _, k := range op.keys {
			if v, ok := t.own(k, op.include); ok {
				if v == nil {
					out = append(out, "-")
				} else {
					out = append(out, fmt.Sprintf("%q", *v))
				}
				continue
			}
			if op.lock != LockNone {
				if !m.lock(op.txn, k, op.lock) {
					return "err:LockTimeout"
				}
				base = m.committed
			}
			v, ok := base[k]
			out = append(out, fmtVal(v, ok))
		}
		return strings.Join(out, " ")
	case opScan:
		base := m.base(t, op.lock)
		var rows []string
		for k := op.lo; k < op.hi; k++ {
			v, own := t.own(k, op.include)
			if own {
				if v != nil {
					rows = append(rows, fmt.Sprintf("%d=%q", k, *v))
				}
				continue
			}
			bv, ok := base[k]
			if !ok {
				continue
			}
			if op.lock != LockNone && !m.lock(op.txn, k, op.lock) {
				return "err:LockTimeout"
			}
			rows = append(rows, fmt.Sprintf("%d=%q", k, bv))
		}
		return "[" + strings.Join(rows, " ") + "]"
	case opMutate:
		for _, mu := range op.muts {
			if !m.lock(op.txn, mu.key, LockExclusive) {
				return "err:LockTimeout"
			}
		}
		l := t.layer()
		for _, mu := range op.muts {
			switch mu.op {
			case OpPut:
				v := mu.val
				l.writes[mu.key] = &v
			case OpDelete:
				l.writes[mu.key] = nil
			case OpInsertOnly:
				own, ok := t.own(mu.key, true)
				if ok && own != nil {
					return "err:DuplicateKey"
				}
				if !ok {
					if _, live := m.committed[mu.key]; live {
						return "err:DuplicateKey"
					}
				}
				v := mu.val
				l.writes[mu.key] = &v
			}
		}
		return "ok"
	case opRollbackStmt:
		for i, l := range t.layers {
			if l.stmt == t.stmt {
				t.layers = append(t.layers[:i], t.layers[i+1:]...)
				break
			}
		}
		return "ok"
	case opRollback:
		delete(m.txns, op.txn)
		m.unlockAll(op.txn)
		return "ok"
	case opCommit:
		merged := map[int]*string{}
		for _, l := range t.layers {
			for k, v := range l.writes {
				merged[k] = v
			}
		}
		delete(m.txns, op.txn)
		m.unlockAll(op.txn)
		if len(merged) == 0 {
			return "ok:empty"
		}
		for k, v := range merged {
			if v == nil {
				delete(m.committed, k)
			} else {
				m.committed[k] = *v
			}
		}
		return "ok:committed"
	case opCheckpoint, opGC:
		return "ok"
	case opRestart:
		for id := range m.txns {
			m.unlockAll(id)
		}
		m.txns = map[int]*metaTxn{}
		return "ok"
	}
	return "?"
}

// ---- the generator --------------------------------------------------------------

func generateMeta(r *rand.Rand, n int) ([]metaOp, []string) {
	m := newMetaModel()
	var ops []metaOp
	var want []string
	nextTxn := 0
	for len(ops) < n {
		var open []int
		for id := range m.txns {
			open = append(open, id)
		}
		sort.Ints(open)
		var op metaOp
		w := r.Intn(100)
		switch {
		case len(open) == 0 || (w < 8 && len(open) < 4):
			op = metaOp{kind: opBegin, txn: nextTxn, iso: Isolation(r.Intn(2)), snap: r.Intn(4) == 0}
			nextTxn++
		case w < 9:
			op = metaOp{kind: opCheckpoint}
		case w < 10:
			op = metaOp{kind: opGC}
		case w < 11:
			op = metaOp{kind: opRestart}
		default:
			txn := open[r.Intn(len(open))]
			lock := LockNone
			if r.Intn(4) == 0 {
				lock = LockMode(1 + r.Intn(2))
			}
			switch x := r.Intn(100); {
			case x < 22:
				var keys []int
				for i := 0; i < 1+r.Intn(3); i++ {
					keys = append(keys, r.Intn(metaKeys))
				}
				op = metaOp{kind: opGet, txn: txn, keys: keys, lock: lock, include: r.Intn(3) == 0}
			case x < 36:
				lo := r.Intn(metaKeys)
				op = metaOp{kind: opScan, txn: txn, lo: lo, hi: lo + 1 + r.Intn(metaKeys-lo), limit: r.Intn(4), lock: lock, include: r.Intn(3) == 0}
			case x < 70:
				var muts []metaMut
				for i := 0; i < 1+r.Intn(3); i++ {
					mu := metaMut{op: MutationOp(r.Intn(3)), key: r.Intn(metaKeys)}
					if mu.op != OpDelete {
						if r.Intn(8) == 0 {
							mu.val = "" // empty values (secondary-index entries) must read back as present
						} else {
							mu.val = fmt.Sprintf("v%d", len(ops))
						}
					}
					muts = append(muts, mu)
				}
				op = metaOp{kind: opMutate, txn: txn, muts: muts}
			case x < 80:
				op = metaOp{kind: opNextStmt, txn: txn}
			case x < 84:
				op = metaOp{kind: opRollbackStmt, txn: txn}
			case x < 89:
				op = metaOp{kind: opRollback, txn: txn}
			default:
				op = metaOp{kind: opCommit, txn: txn}
			}
		}
		ops = append(ops, op)
		want = append(want, m.apply(op))
	}
	return ops, want
}

// ---- running against a partition -----------------------------------------------

type metaRunner struct {
	t     *testing.T
	store objstore.Store
	cfg   Config
	p     *Partition
	ids   map[int]TxnID
	stmts map[int]uint64
	n     int
}

func (r *metaRunner) takeover() {
	r.n++
	cfg := r.cfg
	cfg.OwnerID = fmt.Sprintf("meta-%d", r.n)
	cfg.DataDir = r.t.TempDir()
	p, err := Takeover(context.Background(), cfg, pid)
	if err != nil {
		r.t.Fatalf("takeover: %v", err)
	}
	r.p = p
}

func errResult(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return "err:" + e.Kind.String()
	}
	return "err:" + err.Error()
}

func (r *metaRunner) run(op metaOp) string {
	p := r.p
	id := r.ids[op.txn]
	stmt := r.stmts[op.txn]
	ro := ReadOptions{Stmt: stmt, IncludeStmt: op.include, Lock: op.lock, LockWait: metaLockWait}
	switch op.kind {
	case opBegin:
		id = txnID()
		r.ids[op.txn], r.stmts[op.txn] = id, 1
		if err := p.Begin(p.Epoch(), id, BeginOptions{Isolation: op.iso, ConsistentSnapshot: op.snap}); err != nil {
			return errResult(err)
		}
		return "ok"
	case opNextStmt:
		r.stmts[op.txn]++
		return "ok"
	case opGet:
		keys := make([][]byte, len(op.keys))
		for i, k := range op.keys {
			keys[i] = metaUserKey(k)
		}
		vs, err := p.Get(p.Epoch(), id, keys, ro)
		if err != nil {
			return errResult(err)
		}
		out := make([]string, len(vs))
		for i, v := range vs {
			out[i] = fmtVal(string(v), v != nil)
		}
		return strings.Join(out, " ")
	case opScan:
		// Page through with the limit, as query engines do.
		var rows []string
		start, end := metaUserKey(op.lo), metaUserKey(op.hi)
		for {
			kvs, resume, err := p.Scan(p.Epoch(), id, start, end, op.limit, ro)
			if err != nil {
				return errResult(err)
			}
			if op.limit > 0 && len(kvs) > op.limit {
				return fmt.Sprintf("scan returned %d rows over its limit %d", len(kvs), op.limit)
			}
			for _, kv := range kvs {
				rows = append(rows, fmt.Sprintf("%d=%q", keyIndex(kv.Key), kv.Value))
			}
			if resume == nil {
				break
			}
			if bytes.Compare(resume, start) <= 0 {
				return "scan resume key did not advance"
			}
			start = resume
		}
		return "[" + strings.Join(rows, " ") + "]"
	case opMutate:
		muts := make([]Mutation, len(op.muts))
		for i, mu := range op.muts {
			muts[i] = Mutation{Op: mu.op, Key: metaUserKey(mu.key)}
			if mu.op != OpDelete {
				muts[i].Value = []byte(mu.val)
			}
		}
		if err := p.Mutate(p.Epoch(), id, stmt, muts, metaLockWait); err != nil {
			return errResult(err)
		}
		return "ok"
	case opRollbackStmt:
		if err := p.RollbackStatement(p.Epoch(), id, stmt); err != nil {
			return errResult(err)
		}
		return "ok"
	case opRollback:
		if err := p.Rollback(p.Epoch(), id); err != nil {
			return errResult(err)
		}
		return "ok"
	case opCommit:
		ts, err := p.Commit(p.Epoch(), id)
		if err != nil {
			return errResult(err)
		}
		if ts == 0 {
			return "ok:empty"
		}
		return "ok:committed"
	case opCheckpoint:
		if err := p.Checkpoint(context.Background()); err != nil {
			return errResult(err)
		}
		return "ok"
	case opGC:
		if _, err := p.gcVersions(); err != nil {
			return errResult(err)
		}
		return "ok"
	case opRestart:
		if err := p.Close(); err != nil {
			return errResult(err)
		}
		r.takeover()
		return "ok"
	}
	return "?"
}

// metaUserKey returns user key i of the run: raw bytes, each key a prefix of
// the next ("", "\x00", "a", "a\x00", "aa", ...), so every version of a key
// must sort before the longer keys (issue #3), and "a\x00" sits right
// where a seek past "a"'s versions lands.
func metaUserKey(i int) []byte {
	k := append(encoding.Prefix(pid, encoding.FirstUserTableID, 0), strings.Repeat("a", i/2)...)
	if i%2 == 1 {
		k = append(k, 0)
	}
	return k
}

func keyIndex(k []byte) int {
	for i := 0; i < metaKeys+1; i++ {
		if bytes.Equal(k, metaUserKey(i)) {
			return i
		}
	}
	return -1
}

// metaConfigs change how the owner stores data, never what it returns.
var metaConfigs = []struct {
	name string
	set  func(*Config)
}{
	{"default", func(*Config) {}},
	{"checkpoint-often", func(c *Config) { c.CheckpointBytes = 1; c.CheckpointInterval = 10 * time.Millisecond }},
	{"no-cache-small-memtable", func(c *Config) { c.CacheMaxBytes = -1; c.MemTableBytes = 64 << 10; c.BlockCacheBytes = 1 << 20 }},
	{"no-group-window", func(c *Config) { c.GroupWindow = time.Microsecond; c.GroupMaxBytes = 64 }},
}

func TestMetamorphic(t *testing.T) {
	for seed := *metaSeed; seed < *metaSeed+int64(*metaRuns); seed++ {
		ops, want := generateMeta(rand.New(rand.NewSource(seed)), *metaOps)
		for _, mc := range metaConfigs {
			seed, mc := seed, mc
			t.Run(fmt.Sprintf("seed=%d/%s", seed, mc.name), func(t *testing.T) {
				cfg := testConfig(t, objstore.NewMem(), "meta")
				mc.set(&cfg)
				r := &metaRunner{t: t, store: cfg.Store, cfg: cfg, ids: map[int]TxnID{}, stmts: map[int]uint64{}}
				r.takeover()
				defer func() { r.p.Close() }()
				for i, op := range ops {
					if got := r.run(op); got != want[i] {
						var hist []string
						for j := max(0, i-15); j <= i; j++ {
							hist = append(hist, fmt.Sprintf("  %3d %-60s -> %s", j, ops[j], want[j]))
						}
						t.Fatalf("seed %d, op %d: %s\n  got:  %s\n  want: %s\nlast operations (with expected results):\n%s\nreplay: go test ./internal/kv -run 'Metamorphic/seed=%d/%s' -meta-seed %d -meta-runs 1 -meta-ops %d",
							seed, i, op, got, want[i], strings.Join(hist, "\n"), seed, mc.name, seed, *metaOps)
					}
				}
			})
		}
	}
}
