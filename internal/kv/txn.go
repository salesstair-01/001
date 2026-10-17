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

// ReadOptions qualify a Get or Scan.
type ReadOptions struct {
	Stmt uint64
	// IncludeStmt makes the statement's own writes visible (duplicate-key
	// checks); otherwise a statement never reads its own output, so an UPDATE
	// that moves a primary key can't update a row twice.
	IncludeStmt bool
	// Lock makes this a locking read (UPDATE, DELETE, FOR UPDATE, LOCK IN
	// SHARE MODE): each row is locked and its latest committed version is read,
	// not the snapshot (D6).
	Lock     LockMode
	LockWait time.Duration // 0 = DefaultLockWait
}

func (o ReadOptions) skip() uint64 {
	if o.IncludeStmt {
		return 0
	}
	return o.Stmt
}

func lockWait(d time.Duration) time.Duration {
	if d <= 0 {
		return DefaultLockWait
	}
	return d
}

type intent struct {
	value      []byte // row value or delete marker
	insertOnly bool
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
	began      time.Time // for picking the youngest deadlock victim
	snapshotTS uint64    // read timestamp, once hasSnap
	hasSnap    bool      // a snapshot was taken (snapshotTS can be 0: nothing committed yet)
	snapStmt   uint64    // statement the RC snapshot belongs to
	layers     []*stmtLayer
	bytes      int
	lastSeen   time.Time
	committing bool
	aborted    bool
	autoinc    map[uint32]bool // tables whose AUTO_INCREMENT counter this transaction used
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

// lookup returns the transaction's own newest intent for key, ignoring the
// layer of statement skip (0 = none).
func (t *Txn) lookup(key []byte, skip uint64) (intent, bool) {
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

// merged returns the transaction's final write per key, in key order,
// ignoring the layer of statement skip (0 = none).
func (t *Txn) merged(skip uint64) ([]string, map[string]intent) {
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
		return nil, newErr(KindTxnNotFound, "transaction %s is not open on this owner (aborted)", id)
	}
	t.lastSeen = time.Now()
	return t, nil
}

// txn looks a transaction up without holding p.mu afterwards.
func (p *Partition) txn(id TxnID) (*Txn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	t, err := p.txnLocked(id)
	if err == nil && t.committing {
		return nil, newErr(KindInternal, "transaction %s is committing", id)
	}
	return t, err
}

// abortTxnLocked aborts a transaction and frees its locks. Requires p.mu.
func (p *Partition) abortTxnLocked(t *Txn) {
	t.abortLocked()
	if p.txns[t.id] == t {
		delete(p.txns, t.id)
	}
	p.locks.ReleaseAll(t.id)
}

func (p *Partition) abortTxn(t *Txn) {
	p.mu.Lock()
	if !t.committing {
		p.abortTxnLocked(t)
	}
	p.mu.Unlock()
}

// lockErr aborts the transaction when a lock request made it a deadlock victim.
func (p *Partition) lockErr(t *Txn, err error) error {
	if KindOf(err) == KindDeadlock {
		p.abortTxn(t)
	}
	return err
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
	if p.suspending {
		return errSuspended
	}
	if _, ok := p.txns[id]; ok {
		return nil
	}
	if c, ok := p.recent[id]; ok {
		return newErr(KindInternal, "transaction %s already committed at %d", id, c.ts)
	}
	now := time.Now()
	t := &Txn{id: id, iso: opts.Isolation, began: now, lastSeen: now}
	if opts.ConsistentSnapshot {
		t.snapshotTS, t.hasSnap = p.visibleTS.Load(), true
	}
	p.txns[id] = t
	return nil
}

// snapshotLocked returns the read timestamp for a consistent read in stmt.
func (p *Partition) snapshotLocked(t *Txn, stmt uint64) uint64 {
	switch {
	case t.iso == ReadCommitted && (!t.hasSnap || t.snapStmt != stmt):
		t.snapshotTS, t.snapStmt, t.hasSnap = p.visibleTS.Load(), stmt, true
	case !t.hasSnap:
		t.snapshotTS, t.hasSnap = p.visibleTS.Load(), true
	}
	return t.snapshotTS
}

// KV is a key and its value, as returned by reads.
type KV struct {
	Key   []byte
	Value []byte
}

// Get reads keys in the transaction: its own intents over its snapshot, or
// for a locking read, over the latest committed versions. A nil value means
// the key does not exist.
func (p *Partition) Get(epoch uint64, id TxnID, keys [][]byte, o ReadOptions) ([][]byte, error) {
	if err := p.checkEpoch(epoch); err != nil {
		return nil, err
	}
	p.mu.Lock()
	t, err := p.txnLocked(id)
	if err != nil {
		p.mu.Unlock()
		return nil, err
	}
	ts := uint64(math.MaxUint64)
	if o.Lock == LockNone {
		ts = p.snapshotLocked(t, o.Stmt)
	}
	out := make([][]byte, len(keys))
	var misses []int
	for i, k := range keys {
		if in, ok := t.lookup(k, o.skip()); ok {
			if !encoding.IsDelete(in.value) {
				out[i] = in.value
			}
			continue
		}
		misses = append(misses, i)
	}
	p.mu.Unlock()
	for _, i := range misses {
		if o.Lock != LockNone {
			// Lock first: once held, no other transaction can have a commit in
			// flight for this key, so Pebble has its latest version.
			if err := p.locks.Acquire(t, keys[i], o.Lock, lockWait(o.LockWait)); err != nil {
				return nil, p.lockErr(t, err)
			}
		}
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
	release, err := p.readDB()
	if err != nil {
		return nil, 0, err
	}
	defer release()
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
	// make, not append to nil: an empty value (a secondary-index entry) must
	// stay non-nil, or it reads as absent.
	v := make([]byte, len(it.Value()))
	copy(v, it.Value())
	return v, vts, nil
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
// (0 = no limit). resume is where the next page starts, or nil at the end.
func (p *Partition) Scan(epoch uint64, id TxnID, start, end []byte, limit int, o ReadOptions) ([]KV, []byte, error) {
	if err := p.checkEpoch(epoch); err != nil {
		return nil, nil, err
	}
	p.mu.Lock()
	t, err := p.txnLocked(id)
	if err != nil {
		p.mu.Unlock()
		return nil, nil, err
	}
	ts := uint64(math.MaxUint64)
	if o.Lock == LockNone {
		ts = p.snapshotLocked(t, o.Stmt)
	}
	// Collect the transaction's intents in range.
	var ikeys []string
	iv := map[string]intent{}
	if len(t.layers) > 0 {
		keys, all := t.merged(o.skip())
		for _, k := range keys {
			if k >= string(start) && (end == nil || k < string(end)) {
				ikeys = append(ikeys, k)
				iv[k] = all[k]
			}
		}
	}
	p.mu.Unlock()

	rows, own, resume, err := p.scanMerged(start, end, limit, ts, ikeys, iv)
	if err != nil || o.Lock == LockNone {
		return rows, resume, err
	}
	// Locking read: lock each row found, then read its latest version; a row
	// deleted while we waited drops out. Rows inserted into the range by
	// others are not blocked (no gap locks, D5).
	out := rows[:0]
	for i, r := range rows {
		if own[i] {
			out = append(out, r)
			continue
		}
		if err := p.locks.Acquire(t, r.Key, o.Lock, lockWait(o.LockWait)); err != nil {
			return nil, nil, p.lockErr(t, err)
		}
		v, _, err := p.readVersion(r.Key, math.MaxUint64)
		if err != nil {
			return nil, nil, err
		}
		if v != nil && !encoding.IsDelete(v) {
			out = append(out, KV{Key: r.Key, Value: v})
		}
	}
	return out, resume, nil
}

// scanMerged merges the visible committed rows at ts with the given intents.
// own[i] reports whether row i came from an intent.
func (p *Partition) scanMerged(start, end []byte, limit int, ts uint64, ikeys []string, iv map[string]intent) ([]KV, []bool, []byte, error) {
	release, err := p.readDB()
	if err != nil {
		return nil, nil, nil, err
	}
	defer release()
	it, err := p.db.NewIter(&pebble.IterOptions{LowerBound: start, UpperBound: end})
	if err != nil {
		return nil, nil, nil, err
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
				v = append([]byte{}, it.Value()...)
			}
			valid = it.SeekGE(append(encoding.MVCCKey(uk, 0), 0))
			if k != nil {
				return k, v, true, nil
			}
		}
		return nil, nil, false, it.Error()
	}

	var out []KV
	var own []bool
	ck, cv, cok, err := nextCommitted()
	if err != nil {
		return nil, nil, nil, err
	}
	ii := 0
	for ii < len(ikeys) || cok {
		var k, v []byte
		fromIntent := false
		if ii < len(ikeys) && (!cok || ikeys[ii] <= string(ck)) {
			in := iv[ikeys[ii]]
			k = []byte(ikeys[ii])
			ii++
			if cok && bytes.Equal(k, ck) { // our own write replaces the committed row
				if ck, cv, cok, err = nextCommitted(); err != nil {
					return nil, nil, nil, err
				}
			}
			if encoding.IsDelete(in.value) {
				continue
			}
			v, fromIntent = in.value, true
		} else {
			k, v = ck, cv
			if ck, cv, cok, err = nextCommitted(); err != nil {
				return nil, nil, nil, err
			}
		}
		out = append(out, KV{Key: k, Value: v})
		own = append(own, fromIntent)
		if limit > 0 && len(out) >= limit {
			if ii < len(ikeys) || cok {
				return out, own, append(append([]byte(nil), k...), 0), nil
			}
			return out, own, nil, nil
		}
	}
	return out, own, nil, nil
}

// Mutate adds writes to statement stmt's layer. Every written key is locked
// exclusively first (D5); insert-only keys are then checked against the
// transaction's own writes and the latest committed version.
func (p *Partition) Mutate(epoch uint64, id TxnID, stmt uint64, muts []Mutation, wait time.Duration) error {
	if err := p.checkEpoch(epoch); err != nil {
		return err
	}
	t, err := p.txn(id)
	if err != nil {
		return err
	}
	for _, m := range muts {
		if err := p.locks.Acquire(t, m.Key, LockExclusive, lockWait(wait)); err != nil {
			return p.lockErr(t, err)
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if t, err = p.txnLocked(id); err != nil {
		return err
	}
	l := t.layer(stmt)
	for _, m := range muts {
		in := intent{value: m.Value}
		switch m.Op {
		case OpDelete:
			in.value = []byte{encoding.DeleteMarker}
		case OpInsertOnly:
			in.insertOnly = true
			if own, ok := t.lookup(m.Key, 0); ok {
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

// RollbackStatement drops every write made by statement stmt. Locks taken by
// the statement stay until the transaction ends, as in InnoDB.
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
		p.abortTxnLocked(t)
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
		if t.hasSnap && t.snapshotTS < safe {
			safe = t.snapshotTS
		}
	}
	return safe
}
