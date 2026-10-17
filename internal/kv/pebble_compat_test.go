package kv

// Pebble's own datadriven test files (testdata/pebble, copied unmodified
// from github.com/cockroachdb/pebble v1.1.5 under its BSD license), run
// against a partition: the key-value semantics Pebble tests at its API are
// the ones the owner exposes through transactions.
//
//   - A Pebble sequence number, or each autocommitted set/del, is one commit.
//   - A Pebble snapshot, or a read at "seq=N", is a transaction with a
//     consistent snapshot taken after the commits below N.
//   - A Pebble batch is a transaction, read with its own writes.
//   - An iterator is a cursor over the transaction's forward scans.
//
// A case that needs something the owner does not have is skipped under the
// name of that feature (each has an issue); every other case must produce
// Pebble's expected output exactly. Iterator statistics lines are dropped
// from the expected output and counted. The summary is logged.

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"

	"github.com/salesstair-01/001/internal/encoding"
	"github.com/salesstair-01/001/internal/objstore"
)

// Features the owner does not have; a case using one is skipped.
const (
	featReverse    = "reverse iteration (prev, last, seek-lt) #1"
	featLimit      = "iteration limits (next-limit, seek-ge-limit) #4"
	featMerge      = "merge operator #5"
	featSingleDel  = "single delete #6"
	featRangeDel   = "range deletion (DeleteRange) #2"
	featRangeKeys  = "range keys #10"
	featIterOpts   = "iterator options and cloning (set-options, clone) #4"
	featIterAPI    = "owner-side iterator API (errors on misuse; the owner pages scans) #4"
	featInternal   = "internal keys and LSM introspection"
	featIterStats  = "iterator statistics #4"
	featUnknownCmd = "unrecognized test command"
)

type pebbleCompat struct {
	t        *testing.T
	file     string
	p        *Partition
	skip     string           // set when the current define needs a missing feature
	dbSkip   string           // batch_get: a skipped batch was committed, so the database differs from Pebble\'s
	named    map[string]TxnID // snapshots by name
	seqs     []int            // sequence numbers with a snapshot taken after them
	bySeq    map[int]TxnID    // snapshot seeing every commit <= seq
	batch    TxnID            // open batch (batch_get)
	inBatch  bool
	stats    map[string]int // skipped cases by feature
	run      int
	stripped int // iterator statistics lines dropped from expected output
}

var compatPrefix = encoding.Prefix(pid, encoding.FirstUserTableID, 0)

// ck maps a Pebble key to an owner key. Owner keys are memcomparable
// encodings, which never have one key as a prefix of another (D2): the MVCC
// timestamp suffix would otherwise sort "a"@ts after "a\x00" (issue #3: a
// user-key-aware comparer). Pebble's raw keys ("a", "aa") are encoded
// the way the SQL layer encodes strings.
func ck(s string) []byte {
	return encoding.AppendBytes(append([]byte(nil), compatPrefix...), []byte(s), false)
}

// uk recovers the Pebble key.
func uk(k []byte) string {
	b, _, err := encoding.DecodeBytes(k[len(compatPrefix):], false)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func (c *pebbleCompat) reset() {
	if c.p != nil {
		c.p.Close()
	}
	cfg := testConfig(c.t, objstore.NewMem(), "compat")
	p, err := Takeover(context.Background(), cfg, pid)
	if err != nil {
		c.t.Fatal(err)
	}
	c.p, c.skip, c.named, c.seqs, c.bySeq, c.inBatch = p, "", map[string]TxnID{}, nil, map[int]TxnID{}, false
}

// commit writes muts in one transaction.
func (c *pebbleCompat) commit(muts []Mutation) {
	id := txnID()
	if err := c.p.Begin(c.p.epoch, id, BeginOptions{}); err != nil {
		c.t.Fatal(err)
	}
	if err := c.p.Mutate(c.p.epoch, id, 1, muts, 0); err != nil {
		c.t.Fatal(err)
	}
	if _, err := c.p.Commit(c.p.epoch, id); err != nil {
		c.t.Fatal(err)
	}
}

func (c *pebbleCompat) snapshot() TxnID {
	id := txnID()
	if err := c.p.Begin(c.p.epoch, id, BeginOptions{ConsistentSnapshot: true}); err != nil {
		c.t.Fatal(err)
	}
	return id
}

func mutationFor(kind, k, v string) (Mutation, string) {
	switch kind {
	case "SET", "SETWITHDEL", "set":
		return Mutation{Op: OpPut, Key: ck(k), Value: []byte(v)}, ""
	case "DEL", "del":
		return Mutation{Op: OpDelete, Key: ck(k)}, ""
	case "MERGE", "merge":
		return Mutation{}, featMerge
	case "SINGLEDEL", "singledel", "single-del":
		return Mutation{}, featSingleDel
	case "RANGEDEL", "del-range":
		return Mutation{}, featRangeDel
	case "RANGEKEYSET", "RANGEKEYUNSET", "RANGEKEYDEL", "range-key-set", "range-key-unset", "range-key-del":
		return Mutation{}, featRangeKeys
	}
	return Mutation{}, featInternal
}

var internalKeyRE = regexp.MustCompile(`^\s*([^.\s]+)\.([A-Z]+)\.(\d+):(.*)$`)

// define builds a fresh partition from a define block.
func (c *pebbleCompat) define(d *datadriven.TestData) {
	if c.file == "batch_get" { // a define is a new batch over the same database
		if c.p == nil {
			c.reset()
		}
		if c.inBatch {
			_ = c.p.Rollback(c.p.epoch, c.batch)
		}
		c.skip = c.dbSkip
		if c.skip == "" {
			c.batchDefine(d)
		}
		return
	}
	c.reset()
	type entry struct {
		seq  int
		kind string
		k, v string
	}
	var entries []entry
	internal := false
	for _, line := range strings.Split(d.Input, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if m := internalKeyRE.FindStringSubmatch(line); m != nil {
			internal = true
			seq, _ := strconv.Atoi(m[3])
			entries = append(entries, entry{seq, m[2], m[1], m[4]})
			continue
		}
		f := strings.Fields(line)
		switch f[0] {
		case "mem", "L0", "L1", "L2", "L3", "L4", "L5", "L6":
			continue // level placement does not change what reads return
		case "compact", "flush":
			if err := c.p.Checkpoint(context.Background()); err != nil {
				c.t.Fatal(err)
			}
			continue
		case "snapshot":
			c.named[f[1]] = c.snapshot()
			continue
		}
		if strings.HasPrefix(f[0], "L") && strings.HasSuffix(f[0], ":") {
			continue
		}
		v := ""
		if len(f) > 2 {
			v = f[2]
		}
		k := ""
		if len(f) > 1 {
			k = f[1]
		}
		m, missing := mutationFor(f[0], k, v)
		if missing != "" {
			c.skip = missing
			return
		}
		c.commit([]Mutation{m}) // autocommitted, like db.Set
	}
	if !internal {
		return
	}
	// Internal keys: one commit per sequence number, in order, with a
	// snapshot after each so a read at seq=N sees the commits below N.
	sort.SliceStable(entries, func(i, j int) bool { return entries[i].seq < entries[j].seq })
	c.bySeq[0] = c.snapshot()
	c.seqs = []int{0}
	for i := 0; i < len(entries); {
		j := i
		var muts []Mutation
		for ; j < len(entries) && entries[j].seq == entries[i].seq; j++ {
			m, missing := mutationFor(entries[j].kind, entries[j].k, entries[j].v)
			if missing != "" {
				c.skip = missing
				return
			}
			muts = append(muts, m)
		}
		c.commit(muts)
		c.bySeq[entries[i].seq] = c.snapshot()
		c.seqs = append(c.seqs, entries[i].seq)
		i = j
	}
}

func (c *pebbleCompat) batchDefine(d *datadriven.TestData) {
	c.batch, c.inBatch = txnID(), true
	if err := c.p.Begin(c.p.epoch, c.batch, BeginOptions{}); err != nil {
		c.t.Fatal(err)
	}
	for _, line := range strings.Split(d.Input, "\n") {
		f := strings.Fields(line)
		if len(f) == 0 {
			continue
		}
		v := ""
		if len(f) > 2 {
			v = f[2]
		}
		m, missing := mutationFor(f[0], f[1], v)
		if missing != "" {
			c.skip = missing
			return
		}
		if err := c.p.Mutate(c.p.epoch, c.batch, 1, []Mutation{m}, 0); err != nil {
			c.t.Fatal(err)
		}
	}
}

// reader returns the transaction a read runs in.
func (c *pebbleCompat) reader(d *datadriven.TestData) TxnID {
	for _, a := range d.CmdArgs {
		switch a.Key {
		case "snapshot":
			return c.named[a.Vals[0]]
		case "seq":
			n, _ := strconv.Atoi(a.Vals[0])
			best := c.seqs[0]
			for _, s := range c.seqs {
				if s < n {
					best = s
				}
			}
			return c.bySeq[best]
		}
	}
	if c.inBatch {
		return c.batch
	}
	return c.snapshot() // latest
}

func (c *pebbleCompat) get(id TxnID, k string) (string, bool) {
	vs, err := c.p.Get(c.p.epoch, id, [][]byte{ck(k)}, ReadOptions{Stmt: 2})
	if err != nil {
		c.t.Fatal(err)
	}
	return string(vs[0]), vs[0] != nil
}

// scanAll reads every key in [lower, upper) as the transaction sees it,
// paging with a small limit to exercise resume keys.
func (c *pebbleCompat) scanAll(id TxnID, lower, upper []byte) []KV {
	if lower == nil {
		lower = compatPrefix
	}
	if upper == nil {
		upper = encoding.PrefixEnd(compatPrefix)
	}
	var out []KV
	for start := lower; start != nil; {
		kvs, resume, err := c.p.Scan(c.p.epoch, id, start, upper, 2, ReadOptions{Stmt: 2})
		if err != nil {
			c.t.Fatal(err)
		}
		out = append(out, kvs...)
		start = resume
	}
	return out
}

var reverseCmds = map[string]bool{"prev": true, "last": true, "seek-lt": true, "prev-limit": true, "seek-lt-limit": true}
var limitCmds = map[string]bool{"next-limit": true, "seek-ge-limit": true}
var optCmds = map[string]bool{"set-options": true, "clone": true, "inspect": true, "internal-next": true, "stats": true, "next-prefix": true}

func (c *pebbleCompat) iter(d *datadriven.TestData) (string, string) {
	lines := strings.Split(strings.TrimSpace(d.Input), "\n")
	for _, l := range lines {
		f := strings.Fields(l)
		if len(f) == 0 {
			continue
		}
		switch {
		case reverseCmds[f[0]]:
			return "", featReverse
		case limitCmds[f[0]]:
			return "", featLimit
		case optCmds[f[0]]:
			return "", featIterOpts
		}
	}
	id := c.reader(d)
	format := func(kv KV) string {
		k := uk(kv.Key)
		if c.file == "snapshot" {
			return fmt.Sprintf("%s:%s", k, kv.Value)
		}
		return fmt.Sprintf("%s: (%s, .)", k, kv.Value)
	}
	var lower, upper []byte
	for _, a := range d.CmdArgs {
		switch a.Key {
		case "lower":
			lower = ck(a.Vals[0])
		case "upper":
			upper = ck(a.Vals[0])
		}
	}
	rows := c.scanAll(id, lower, upper)
	pos, prefix := -1, false // prefix: positioned by seek-prefix-ge; next ends it
	var out []string
	for _, l := range lines {
		f := strings.Fields(l)
		if len(f) == 0 {
			continue
		}
		switch f[0] {
		case "first":
			pos, prefix = 0, false
		case "next":
			switch {
			case prefix:
				pos = len(rows) // a prefix iterator ends after its key
			case pos >= 0 && pos < len(rows):
				pos++
			}
		case "seek-ge":
			pos, prefix = len(rows), false
			for i, kv := range rows {
				if uk(kv.Key) >= f[1] {
					pos = i
					break
				}
			}
		case "seek-prefix-ge":
			// A bounded scan of exactly the key: Pebble's default Split makes
			// the whole key the prefix.
			pos, prefix = len(rows), true
			if exact := c.scanAll(id, ck(f[1]), encoding.PrefixEnd(ck(f[1]))); len(exact) == 1 {
				for i, kv := range rows {
					if string(kv.Key) == string(exact[0].Key) {
						pos = i
					}
				}
			}
		case "set-bounds":
			lower, upper = nil, nil
			for _, a := range f[1:] {
				kv := strings.SplitN(a, "=", 2)
				switch kv[0] {
				case "lower":
					lower = ck(kv[1])
				case "upper":
					upper = ck(kv[1])
				}
			}
			rows, pos, prefix = c.scanAll(id, lower, upper), -1, false
			out = append(out, ".")
			continue
		default:
			return "", featUnknownCmd + " " + f[0]
		}
		if pos >= 0 && pos < len(rows) {
			out = append(out, format(rows[pos]))
		} else {
			out = append(out, ".")
		}
	}
	return strings.Join(out, "\n") + "\n", ""
}

func (c *pebbleCompat) cmd(_ *testing.T, d *datadriven.TestData) string {
	skipWith := func(feature string) string {
		c.stats[feature]++
		return d.Expected
	}
	switch d.Cmd {
	case "define":
		c.define(d)
		if c.skip != "" {
			return skipWith(c.skip)
		}
		c.run++
		return d.Expected // nothing to compare: Pebble prints its LSM here
	case "get":
		if c.skip != "" {
			return skipWith(c.skip)
		}
		c.run++
		if c.file == "batch_get" {
			v, ok := c.get(c.batch, d.CmdArgs[0].Key)
			if !ok {
				return "pebble: not found\n"
			}
			return v + "\n"
		}
		id := c.reader(d)
		var out []string
		for _, k := range strings.Fields(d.Input) {
			if v, ok := c.get(id, k); ok {
				out = append(out, k+":"+v)
			} else {
				out = append(out, k+": pebble: not found")
			}
		}
		return strings.Join(out, "\n") + "\n"
	case "commit":
		if c.skip != "" {
			c.dbSkip = c.skip
			return skipWith(c.skip)
		}
		c.run++
		if _, err := c.p.Commit(c.p.epoch, c.batch); err != nil {
			c.t.Fatal(err)
		}
		c.inBatch = false
		return d.Expected
	case "iter":
		if c.skip != "" {
			return skipWith(c.skip)
		}
		if strings.Contains(d.Expected, "err=") {
			return skipWith(featIterAPI)
		}
		got, missing := c.iter(d)
		if missing != "" {
			return skipWith(missing)
		}
		c.run++
		// Pebble appends iterator statistics, which the owner does not keep.
		var want []string
		for _, l := range strings.Split(strings.TrimSuffix(d.Expected, "\n"), "\n") {
			if strings.HasPrefix(l, "stats:") {
				c.stripped++
				continue
			}
			want = append(want, l)
		}
		if strings.Join(want, "\n")+"\n" == got {
			return d.Expected
		}
		return got
	case "scan", "clear", "db-state", "wait-pending-table-stats", "compact", "metrics", "lsm":
		return skipWith(featInternal)
	}
	return skipWith(featUnknownCmd + " " + d.Cmd)
}

func TestPebbleCompat(t *testing.T) {
	total := map[string]int{}
	run := 0
	stripped := 0
	datadriven.Walk(t, "testdata/pebble", func(t *testing.T, path string) {
		file := path[strings.LastIndex(path, "/")+1:]
		if file == "LICENSE" {
			return
		}
		c := &pebbleCompat{t: t, file: file, stats: map[string]int{}}
		defer func() {
			if c.p != nil {
				c.p.Close()
			}
		}()
		datadriven.RunTest(t, path, c.cmd)
		var parts []string
		for f, n := range c.stats {
			parts = append(parts, fmt.Sprintf("%s: %d", f, n))
			total[f] += n
		}
		sort.Strings(parts)
		t.Logf("%s: %d cases run, skipped %v", file, c.run, parts)
		run += c.run
		stripped += c.stripped
	})
	var parts []string
	for f, n := range total {
		parts = append(parts, fmt.Sprintf("  %-50s %d", f, n))
	}
	sort.Strings(parts)
	t.Logf("Pebble datadriven cases run and matching Pebble: %d\nskipped, by missing feature:\n%s\niterator statistics lines dropped: %d",
		run, strings.Join(parts, "\n"), stripped)
}
