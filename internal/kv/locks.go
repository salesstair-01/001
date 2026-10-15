package kv

import (
	"bytes"
	"sync"
	"time"
)

// LockMode is a row lock mode (D5). Locks are held in the owner's memory, so
// owner loss aborts every open transaction on the partition.
type LockMode int

const (
	LockNone LockMode = iota
	LockShared
	LockExclusive
)

// DefaultLockWait is MySQL's default innodb_lock_wait_timeout.
const DefaultLockWait = 50 * time.Second

type lockWaiter struct {
	txn  *Txn
	mode LockMode
	key  string
	done chan error // receives nil when granted, or the reason it never will be
}

type lockEntry struct {
	holders map[TxnID]LockMode
	queue   []*lockWaiter
}

// lockTable holds row locks for one partition, keyed by user key. There are no
// gap or next-key locks, as in TiDB.
type lockTable struct {
	mu      sync.Mutex
	locks   map[string]*lockEntry
	held    map[TxnID]map[string]LockMode
	waiting map[TxnID]*lockWaiter
	// onVictim aborts a transaction chosen as a deadlock victim.
	onVictim func(*Txn)
}

func newLockTable(onVictim func(*Txn)) *lockTable {
	return &lockTable{
		locks: map[string]*lockEntry{}, held: map[TxnID]map[string]LockMode{},
		waiting: map[TxnID]*lockWaiter{}, onVictim: onVictim,
	}
}

func compatible(a, b LockMode) bool { return a == LockShared && b == LockShared }

// grantable reports whether txn can take mode on e right now.
func (e *lockEntry) grantable(id TxnID, mode LockMode, ignoreQueue bool) bool {
	for h, m := range e.holders {
		if h != id && !compatible(m, mode) {
			return false
		}
	}
	if ignoreQueue {
		return true
	}
	// FIFO: don't overtake earlier waiters with a conflicting request.
	for _, w := range e.queue {
		if w.txn.id != id && !compatible(w.mode, mode) {
			return false
		}
	}
	return true
}

func (lt *lockTable) grantLocked(e *lockEntry, key string, id TxnID, mode LockMode) {
	if e.holders[id] < mode {
		e.holders[id] = mode
	}
	h := lt.held[id]
	if h == nil {
		h = map[string]LockMode{}
		lt.held[id] = h
	}
	if h[key] < mode {
		h[key] = mode
	}
}

// Acquire takes mode on key for txn, waiting up to timeout. It returns a
// LockTimeout or Deadlock error when the lock can't be had.
func (lt *lockTable) Acquire(txn *Txn, key []byte, mode LockMode, timeout time.Duration) error {
	if mode == LockNone {
		return nil
	}
	k := string(key)
	lt.mu.Lock()
	e := lt.locks[k]
	if e == nil {
		e = &lockEntry{holders: map[TxnID]LockMode{}}
		lt.locks[k] = e
	}
	if cur := e.holders[txn.id]; cur >= mode {
		lt.mu.Unlock()
		return nil
	}
	// Re-entrant upgrade or a free lock is granted at once; an upgrade does
	// not queue behind waiters, which would only deadlock with itself.
	if e.grantable(txn.id, mode, e.holders[txn.id] != LockNone) {
		lt.grantLocked(e, k, txn.id, mode)
		lt.mu.Unlock()
		return nil
	}
	w := &lockWaiter{txn: txn, mode: mode, key: k, done: make(chan error, 1)}
	e.queue = append(e.queue, w)
	lt.waiting[txn.id] = w
	// Check the wait-for graph on every wait; it is small and local.
	if victim := lt.findDeadlockLocked(txn); victim != nil {
		if victim == txn {
			lt.removeWaiterLocked(w)
			lt.mu.Unlock()
			return newErr(KindDeadlock, "Deadlock found when trying to get lock; try restarting transaction")
		}
		vw := lt.waiting[victim.id]
		lt.removeWaiterLocked(vw)
		vw.done <- newErr(KindDeadlock, "Deadlock found when trying to get lock; try restarting transaction")
		lt.releaseLocked(victim.id)
		lt.mu.Unlock()
		lt.onVictim(victim)
		lt.mu.Lock()
		lt.wakeLocked(k)
	}
	lt.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-w.done:
		return err
	case <-timer.C:
	}
	lt.mu.Lock()
	defer lt.mu.Unlock()
	select {
	case err := <-w.done: // granted while the timer fired
		return err
	default:
	}
	lt.removeWaiterLocked(w)
	lt.wakeLocked(k)
	return newErr(KindLockTimeout, "Lock wait timeout exceeded; try restarting transaction")
}

func (lt *lockTable) removeWaiterLocked(w *lockWaiter) {
	if w == nil {
		return
	}
	if e := lt.locks[w.key]; e != nil {
		for i, q := range e.queue {
			if q == w {
				e.queue = append(e.queue[:i], e.queue[i+1:]...)
				break
			}
		}
		lt.gcLocked(w.key, e)
	}
	if lt.waiting[w.txn.id] == w {
		delete(lt.waiting, w.txn.id)
	}
}

func (lt *lockTable) gcLocked(key string, e *lockEntry) {
	if len(e.holders) == 0 && len(e.queue) == 0 {
		delete(lt.locks, key)
	}
}

// wakeLocked grants queued requests on key that have become compatible, in order.
func (lt *lockTable) wakeLocked(key string) {
	e := lt.locks[key]
	if e == nil {
		return
	}
	for len(e.queue) > 0 {
		w := e.queue[0]
		if !e.grantable(w.txn.id, w.mode, true) {
			break
		}
		e.queue = e.queue[1:]
		delete(lt.waiting, w.txn.id)
		lt.grantLocked(e, key, w.txn.id, w.mode)
		w.done <- nil
	}
	lt.gcLocked(key, e)
}

func (lt *lockTable) releaseLocked(id TxnID) {
	if w := lt.waiting[id]; w != nil {
		lt.removeWaiterLocked(w)
		w.done <- newErr(KindTxnNotFound, "transaction aborted while waiting for a lock")
	}
	keys := lt.held[id]
	delete(lt.held, id)
	for k := range keys {
		if e := lt.locks[k]; e != nil {
			delete(e.holders, id)
			lt.wakeLocked(k)
		}
	}
}

// ReleaseAll drops every lock txn holds or waits for.
func (lt *lockTable) ReleaseAll(id TxnID) {
	lt.mu.Lock()
	lt.releaseLocked(id)
	lt.mu.Unlock()
}

// Holds reports whether txn holds at least mode on key.
func (lt *lockTable) Holds(id TxnID, key []byte, mode LockMode) bool {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	return lt.held[id][string(key)] >= mode
}

// blockersLocked returns the transactions w waits for: conflicting holders and
// conflicting waiters ahead of it.
func (lt *lockTable) blockersLocked(w *lockWaiter) []*Txn {
	e := lt.locks[w.key]
	if e == nil {
		return nil
	}
	var out []*Txn
	seen := map[TxnID]bool{}
	for h, m := range e.holders {
		if h != w.txn.id && !compatible(m, w.mode) && !seen[h] {
			seen[h] = true
			if t := lt.txnOf(h, e); t != nil {
				out = append(out, t)
			}
		}
	}
	for _, q := range e.queue {
		if q == w {
			break
		}
		if q.txn.id != w.txn.id && !compatible(q.mode, w.mode) && !seen[q.txn.id] {
			seen[q.txn.id] = true
			out = append(out, q.txn)
		}
	}
	return out
}

// txnOf finds the Txn for a holder id: holders that wait somewhere are known
// through their waiter; others can't be part of a cycle and return nil.
func (lt *lockTable) txnOf(id TxnID, _ *lockEntry) *Txn {
	if w := lt.waiting[id]; w != nil {
		return w.txn
	}
	return nil
}

// findDeadlockLocked looks for a wait-for cycle through start and returns the
// youngest transaction on it (the victim), or nil.
func (lt *lockTable) findDeadlockLocked(start *Txn) *Txn {
	var path []*Txn
	onPath := map[TxnID]bool{}
	visited := map[TxnID]bool{}
	var dfs func(t *Txn) []*Txn
	dfs = func(t *Txn) []*Txn {
		if onPath[t.id] {
			for i, p := range path {
				if p.id == t.id {
					return append([]*Txn(nil), path[i:]...)
				}
			}
		}
		if visited[t.id] {
			return nil
		}
		visited[t.id] = true
		w := lt.waiting[t.id]
		if w == nil {
			return nil
		}
		onPath[t.id] = true
		path = append(path, t)
		for _, b := range lt.blockersLocked(w) {
			if cycle := dfs(b); cycle != nil {
				return cycle
			}
		}
		path = path[:len(path)-1]
		onPath[t.id] = false
		return nil
	}
	cycle := dfs(start)
	if cycle == nil {
		return nil
	}
	victim := cycle[0]
	for _, t := range cycle[1:] {
		if younger(t, victim) {
			victim = t
		}
	}
	return victim
}

// younger reports whether a began after b (ties broken by id).
func younger(a, b *Txn) bool {
	if !a.began.Equal(b.began) {
		return a.began.After(b.began)
	}
	return bytes.Compare(a.id[:], b.id[:]) > 0
}
