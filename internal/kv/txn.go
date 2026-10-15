package kv

import (
	"bytes"
	"encoding/hex"
	"math"
	"sort"
	"time"

	"github.com/cockroachdb/pebble"

	"github.com/salesstair-01/001/internal/encoding"
)

// TxnID is the query engine's id for a transaction (a UUIDv7).
type TxnID [16]byte

func (id TxnID) String() string { return hex.EncodeToString(id[:]) }

// Isolation levels (D6).
type Isolation int

const (
	RepeatableRead Isolation = iota
	ReadCommitted
)

// MutationOp is the kind of a write.
type MutationOp int

const (
	OpPut        MutationOp = iota // write a value
	OpDelete                       // write a delete marker
	OpInsertOnly                   // write a value; DuplicateKey if a live version exists
)

// Mutation is one write in a Mutate call.
type Mutation struct {
	Op    MutationOp
	Key   []byte // user key
	Value []byte
}

// BeginOptions start a transaction on the owner.
type BeginOptions struct {
	Isolation Isolation
	// ConsistentSnapshot takes the snapshot now (START TRANSACTION WITH CONSISTENT SNAPSHOT)
	// instead of at the first consistent read.
	ConsistentSnapshot bool
}

type intent struct {
	value      []byte // row value or delete marker
	insertOnly bool
	readTS     uint64 // the snapshot the writing statement ran against
}

type stmtLayer struct {
	id     uint64
	writes map[string]intent
}

// TxnState is reported by TxnStatus.
type TxnState int

const (
	TxnUnknown   TxnState = iota // aborted, or never existed (definitive after a takeover)
	TxnActive                    // open on this owner
	TxnCommitted                 // committed, CommitTS set
)

// Txn is a transaction open on this owner. All fields are guarded by Partition.mu.
type Txn struct {
	id         TxnID
	iso        Isolation
	snapshotTS uint64 // 0 until the first consistent read (RR) or statement (RC)
	snapStmt   uint64 // statement the RC snapshot belongs to
	layers     []*stmtLayer
	bytes      int
	lastSeen   time.Time
	committing bool
	aborted    bool
}

func (t *Txn) abortLocked() { t.aborted = true }

func (t *Txn) layer(stmt uint64) *stmtLayer {
	if n := len(t.layers); n > 0 && t.layers[n-1].id == stmt {
		return t.layers[n-1]
	}
	l := &stmtLayer{id: stmt, writes: map[string]intent{}}
	t.layers = append(t.layers, l)
	return l
}

// lookup returns the transaction's own newest intent for key.
func (t *Txn) lookup(key []byte) (intent, bool) { return t.lookupBefore(key, 0) }

// lookupBefore is lookup ignoring the layer of statement skip (0 = none).
func (t *Txn) lookupBefore(key []byte, skip uint64) (intent, bool) {
	for i := len(t.layers) - 1; i >= 0; i-- {
		if skip != 0 && t.layers[i].id == skip {
			continue
		}
		if in, ok := t.layers[i].writes[string(key)]; ok {
			return in, true
		}
	}
	return intent{}, false
}

// merged returns the transaction's final write per key, in key order.
func (t *Txn) merged() ([]string, map[string]intent) { return t.mergedBefore(0) }

// mergedBefore is merged ignoring the layer of statement skip (0 = none).
func (t *Txn) mergedBefore(skip uint64) ([]string, map[string]intent) {
	out := map[string]intent{}
	for _, l := range t.layers {
		if skip != 0 && l.id == skip {
			continue
		}
		for k, in := range l.writes {
			if prev, ok := out[k]; ok && prev.insertOnly && !in.insertOnly {
				// An update of a row this transaction inserted still must not exist outside it.
				in.insertOnly = true
			}
			out[k] = in
		}
	}
	keys := make([]string, 0, len(out))
	for k := range out {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys, out
}

func (p *Partition) txnLocked(id TxnID) (*Txn, error) {
	if p.stopErr != nil {
		return nil, p.stopErr
	}
	t, ok := p.txns[id]
	if !ok || t.aborted {
		return nil, newErr(KindTxnNotFound, "transaction %s is not open on this owner", id)
	}
	t.lastSeen = time.Now()
	return t, nil
}

// Begin opens a transaction. Beginning an id that is already open is a no-op.
func (p *Partition) Begin(epoch uint64, id TxnID, opts BeginOptions) error {
	if err := p.checkEpoch(epoch); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopErr != nil {
		return p.stopErr
	}
	if _, ok := p.txns[id]; ok {
		return nil
	}
	if c, ok := p.recent[id]; ok {
		return newErr(KindInternal, "transaction %s already committed at %d", id, c.ts)
	}
	t := &Txn{id: id, iso: opts.Isolation, lastSeen: time.Now()}
	if opts.ConsistentSnapshot {
		t.snapshotTS = p.visibleTS.Load()
	}
	p.txns[id] = t
	return nil
}

// snapshotLocked returns the read timestamp for a consistent read in stmt.
func (p *Partition) snapshotLocked(t *Txn, stmt uint64) uint64 {
	switch {
	case t.iso == ReadCommitted && (t.snapshotTS == 0 || t.snapStmt != stmt):
		t.snapshotTS, t.snapStmt = p.visibleTS.Load(), stmt
	case t.snapshotTS == 0:
		t.snapshotTS = p.visibleTS.Load()
	}
	return t.snapshotTS
}

// KV is a key and its value, as returned by reads.
type KV struct {
	Key   []byte
	Value []byte
}

// Get reads keys in the transaction: its own intents over its snapshot.
// A nil value means the key does not exist. Writes of statement stmt itself
// are visible only with includeStmt, so a statement doesn't read its own
// output (an UPDATE that moves a primary key must not update a row twice);
// duplicate-key checks set it.
func (p *Partition) Get(epoch uint64, id TxnID, stmt uint64, keys [][]byte, includeStmt bool) ([][]byte, error) {
	if err := p.checkEpoch(epoch); err != nil {
		return nil, err
	}
	p.mu.Lock()
	t, err := p.txnLocked(id)
	if err != nil {
		p.mu.Unlock()
		return nil, err
	}
	ts := p.snapshotLocked(t, stmt)
	skip := stmt
	if includeStmt {
		skip = 0
	}
	out := make([][]byte, len(keys))
	var misses []int
	for i, k := range keys {
		if in, ok := t.lookupBefore(k, skip); ok {
			if !encoding.IsDelete(in.value) {
				out[i] = in.value
			}
			continue
		}
		misses = append(misses, i)
	}
	p.mu.Unlock()
	for _, i := range misses {
		v, _, err := p.readVersion(keys[i], ts)
		if err != nil {
			return nil, err
		}
		if v != nil && !encoding.IsDelete(v) {
			out[i] = v
		}
	}
	return out, nil
}

// readVersion returns the newest version of userKey at or below ts (which may
// be a delete marker), and its commit_ts. A nil value means no version.
func (p *Partition) readVersion(userKey []byte, ts uint64) ([]byte, uint64, error) {
	upper := append(encoding.MVCCKey(userKey, 0), 0)
	it, err := p.db.NewIter(&pebble.IterOptions{LowerBound: encoding.MVCCKey(userKey, ts), UpperBound: upper})
	if err != nil {
		return nil, 0, err
	}
	defer it.Close()
	if !it.First() {
		return nil, 0, it.Error()
	}
	uk, vts, err := encoding.SplitMVCC(it.Key())
	if err != nil || !bytes.Equal(uk, userKey) {
		return nil, 0, err
	}
	return append([]byte(nil), it.Value()...), vts, nil
}

// latestLocked returns the newest committed version of key, including
// commits that are durable-pending but not yet applied. Requires p.mu.
func (p *Partition) latestLocked(key []byte) ([]byte, uint64, error) {
	if pw, ok := p.pending[string(key)]; ok {
		return pw.value, pw.ts, nil
	}
	return p.readVersion(key, math.MaxUint64)
}

// Scan reads [start, end) in key order in the transaction, up to limit rows
// (0 = no limit). more reports whether rows remain after the last one. As
// with Get, writes of statement stmt itself are not visible.
func (p *Partition) Scan(epoch uint64, id TxnID, stmt uint64, start, end []byte, limit int) ([]KV, bool, error) {
	if err := p.checkEpoch(epoch); err != nil {
		return nil, false, err
	}
	p.mu.Lock()
	t, err := p.txnLocked(id)
	if err != nil {
		p.mu.Unlock()
		return nil, false, err
	}
	ts := p.snapshotLocked(t, stmt)
	// Collect the transaction's intents in range.
	var ikeys []string
	iv := map[string]intent{}
	if len(t.layers) > 0 {
		keys, all := t.mergedBefore(stmt)
		for _, k := range keys {
			if k >= string(start) && (end == nil || k < string(end)) {
				ikeys = append(ikeys, k)
				iv[k] = all[k]
			}
		}
	}
	p.mu.Unlock()

	it, err := p.db.NewIter(&pebble.IterOptions{LowerBound: start, UpperBound: end})
	if err != nil {
		return nil, false, err
	}
	defer it.Close()

	// nextCommitted advances to the next user key visible at ts.
	valid := it.First()
	nextCommitted := func() ([]byte, []byte, bool, error) {
		for valid {
			uk, vts, err := encoding.SplitMVCC(it.Key())
			if err != nil {
				return nil, nil, false, err
			}
			if bytes.Compare(uk, start) < 0 {
				valid = it.SeekGE(append(encoding.MVCCKey(uk, 0), 0))
				continue
			}
			if vts > ts {
				valid = it.SeekGE(encoding.MVCCKey(uk, ts))
				continue
			}
			var k, v []byte
			if !encoding.IsDelete(it.Value()) {
				k = append([]byte(nil), uk...)
				v = append([]byte(nil), it.Value()...)
			}
			valid = it.SeekGE(append(encoding.MVCCKey(uk, 0), 0))
			if k != nil {
				return k, v, true, nil
			}
		}
		return nil, nil, false, it.Error()
	}

	var out []KV
	ck, cv, cok, err := nextCommitted()
	if err != nil {
		return nil, false, err
	}
	ii := 0
	for ii < len(ikeys) || cok {
		var k, v []byte
		if ii < len(ikeys) && (!cok || ikeys[ii] <= string(ck)) {
			in := iv[ikeys[ii]]
			k = []byte(ikeys[ii])
			ii++
			if cok && bytes.Equal(k, ck) { // our own write replaces the committed row
				if ck, cv, cok, err = nextCommitted(); err != nil {
					return nil, false, err
				}
			}
			if encoding.IsDelete(in.value) {
				continue
			}
			v = in.value
		} else {
			k, v = ck, cv
			if ck, cv, cok, err = nextCommitted(); err != nil {
				return nil, false, err
			}
		}
		out = append(out, KV{Key: k, Value: v})
		if limit > 0 && len(out) >= limit {
			return out, ii < len(ikeys) || cok, nil
		}
	}
	return out, false, nil
}

// Mutate adds writes to statement stmt's layer. Insert-only keys are checked
// against the transaction's own writes and the latest committed version.
func (p *Partition) Mutate(epoch uint64, id TxnID, stmt uint64, muts []Mutation) error {
	if err := p.checkEpoch(epoch); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	t, err := p.txnLocked(id)
	if err != nil {
		return err
	}
	if t.committing {
		return newErr(KindInternal, "transaction %s is committing", id)
	}
	readTS := p.snapshotLocked(t, stmt)
	l := t.layer(stmt)
	for _, m := range muts {
		in := intent{value: m.Value, readTS: readTS}
		switch m.Op {
		case OpDelete:
			in.value = []byte{encoding.DeleteMarker}
		case OpInsertOnly:
			in.insertOnly = true
			if own, ok := t.lookup(m.Key); ok {
				if !encoding.IsDelete(own.value) {
					return &Error{Kind: KindDuplicateKey, Msg: "duplicate key", Key: m.Key}
				}
				// Re-inserting a row this transaction deleted overwrites a committed
				// version, if any, so it is a plain put from here on.
				in.insertOnly = false
			} else {
				v, _, err := p.latestLocked(m.Key)
				if err != nil {
					return err
				}
				if v != nil && !encoding.IsDelete(v) {
					return &Error{Kind: KindDuplicateKey, Msg: "duplicate key", Key: m.Key}
				}
			}
		}
		if len(m.Value) > encoding.MaxRowSize {
			return encoding.ErrRowTooLarge
		}
		if prev, ok := l.writes[string(m.Key)]; ok {
			t.bytes -= len(prev.value)
		} else {
			t.bytes += len(m.Key)
		}
		t.bytes += len(in.value)
		if t.bytes > p.cfg.IntentCap {
			return newErr(KindTxnTooLarge, "transaction exceeds %d bytes of intents", p.cfg.IntentCap)
		}
		l.writes[string(m.Key)] = in
	}
	return nil
}

// RollbackStatement drops every write made by statement stmt.
func (p *Partition) RollbackStatement(epoch uint64, id TxnID, stmt uint64) error {
	if err := p.checkEpoch(epoch); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	t, err := p.txnLocked(id)
	if err != nil {
		return err
	}
	for i, l := range t.layers {
		if l.id == stmt {
			for k, in := range l.writes {
				t.bytes -= len(k) + len(in.value)
			}
			t.layers = append(t.layers[:i], t.layers[i+1:]...)
			break
		}
	}
	return nil
}

// Rollback aborts the transaction. Rolling back an unknown id is a no-op.
func (p *Partition) Rollback(epoch uint64, id TxnID) error {
	if err := p.checkEpoch(epoch); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if t, ok := p.txns[id]; ok && !t.committing {
		t.abortLocked()
		delete(p.txns, id)
	}
	return nil
}

// Heartbeat extends the lease of open transactions.
func (p *Partition) Heartbeat(epoch uint64, ids []TxnID) error {
	if err := p.checkEpoch(epoch); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopErr != nil {
		return p.stopErr
	}
	now := time.Now()
	for _, id := range ids {
		if t, ok := p.txns[id]; ok {
			t.lastSeen = now
		}
	}
	return nil
}

// TxnStatus reports what happened to a transaction. After a takeover an
// unknown id is definitive: anything the old owner wrote was replayed.
func (p *Partition) TxnStatus(epoch uint64, id TxnID) (TxnState, uint64, error) {
	if err := p.checkEpoch(epoch); err != nil {
		return TxnUnknown, 0, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.recent[id]; ok {
		return TxnCommitted, c.ts, nil
	}
	if t, ok := p.txns[id]; ok && !t.aborted {
		return TxnActive, 0, nil
	}
	return TxnUnknown, 0, nil
}

// oldestSnapshotLocked returns the GC safe point: the oldest snapshot any open
// transaction may still read.
func (p *Partition) oldestSnapshotLocked() uint64 {
	safe := p.visibleTS.Load()
	for _, t := range p.txns {
		if t.snapshotTS != 0 && t.snapshotTS < safe {
			safe = t.snapshotTS
		}
		for _, l := range t.layers {
			for _, in := range l.writes {
				if in.readTS < safe {
					safe = in.readTS
				}
			}
		}
	}
	return safe
}
