// Package sqlengine is the query engine: go-mysql-server with a database
// provider and tables backed by the KV owner, and a sql.TransactionSession
// that maps MySQL transactions onto KV transactions (D6, D7).
package sqlengine

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"sync"
	"time"

	"github.com/salesstair-01/001/internal/kv"
	"github.com/salesstair-01/001/internal/kvrpc"
)

// Router finds the KV owner of a partition. stale is an epoch the caller
// found no longer served (0 = none), so the router must not return it again
// without checking (slice 6: re-resolve on EpochChanged).
type Router interface {
	Resolve(ctx context.Context, partition, stale uint64) (*kvrpc.Client, uint64, error)
}

// StaticRouter sends every partition to one KV node, which takes a partition
// over when it is opened.
type StaticRouter struct{ Client *kvrpc.Client }

func (r StaticRouter) Resolve(ctx context.Context, partition, _ uint64) (*kvrpc.Client, uint64, error) {
	epoch, err := r.Client.Open(ctx, partition)
	return r.Client, epoch, err
}

// Store is the query engine's handle on one partition (one tenant, D1).
type Store struct {
	router    Router
	partition uint64

	mu     sync.Mutex
	client *kvrpc.Client // current owner; nil = resolve on next use
	epoch  uint64
	stale  uint64 // last epoch found gone
	open   map[*Txn]struct{}

	stop chan struct{}
}

// NewStore creates a store that talks to one KV node directly.
func NewStore(client *kvrpc.Client, partition uint64) *Store {
	return NewRoutedStore(StaticRouter{Client: client}, partition)
}

// NewRoutedStore creates a store that finds the partition's owner through a
// router (the directory), and starts its heartbeat loop.
func NewRoutedStore(r Router, partition uint64) *Store {
	s := &Store{router: r, partition: partition, open: map[*Txn]struct{}{}, stop: make(chan struct{})}
	go s.heartbeats()
	return s
}

// Close stops the heartbeat loop.
func (s *Store) Close() { close(s.stop) }

// Partition returns the partition id.
func (s *Store) Partition() uint64 { return s.partition }

// header returns the request header and the owner's client, resolving the
// owner (and so waking the partition) if needed.
func (s *Store) header(ctx context.Context) (kvrpc.Header, *kvrpc.Client, error) {
	s.mu.Lock()
	c, epoch, stale := s.client, s.epoch, s.stale
	s.mu.Unlock()
	if c == nil {
		var err error
		if c, epoch, err = s.router.Resolve(ctx, s.partition, stale); err != nil {
			return kvrpc.Header{}, nil, err
		}
		s.mu.Lock()
		if s.client == nil {
			s.client, s.epoch = c, epoch
		}
		c, epoch = s.client, s.epoch
		s.mu.Unlock()
	}
	return kvrpc.Header{Partition: s.partition, Epoch: epoch}, c, nil
}

// invalidate forgets the owner after EpochChanged, so the next call re-resolves it.
func (s *Store) invalidate(epoch uint64) {
	s.mu.Lock()
	if s.client != nil && s.epoch == epoch {
		s.client, s.stale = nil, epoch
	}
	s.mu.Unlock()
}

// heartbeats extends the lease of every open transaction every 2 s (D5).
func (s *Store) heartbeats() {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
		}
		type owner struct {
			c     *kvrpc.Client
			epoch uint64
		}
		byOwner := map[owner][]kv.TxnID{}
		s.mu.Lock()
		for txn := range s.open {
			if txn.begun {
				o := owner{txn.client, txn.epoch}
				byOwner[o] = append(byOwner[o], txn.id)
			}
		}
		s.mu.Unlock()
		for o, ids := range byOwner {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = o.c.Heartbeat(ctx, &kvrpc.HeartbeatReq{H: kvrpc.Header{Partition: s.partition, Epoch: o.epoch}, Txns: ids})
			cancel()
		}
	}
}

// newTxnID returns a UUIDv7: 48-bit millisecond timestamp, version and variant bits, random rest.
func newTxnID() kv.TxnID {
	var id kv.TxnID
	_, _ = rand.Read(id[:])
	ms := uint64(time.Now().UnixMilli())
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], ms)
	copy(id[:6], ts[2:])
	id[6] = 0x70 | id[6]&0x0F
	id[8] = 0x80 | id[8]&0x3F
	return id
}

// Txn is a query-engine transaction. Its KV transaction begins lazily on the
// first read or write, and commits or rolls back with the MySQL transaction.
type Txn struct {
	store    *Store
	id       kv.TxnID
	iso      kv.Isolation
	readOnly bool

	mu     sync.Mutex
	begun  bool
	client *kvrpc.Client // the owner the KV transaction lives on
	epoch  uint64
	ops    int  // KV operations so far
	wrote  bool // any Mutate sent
	dead   error
}

// NewTxn creates a transaction; nothing is sent to the owner yet.
func (s *Store) NewTxn(iso kv.Isolation, readOnly bool) *Txn {
	return &Txn{store: s, id: newTxnID(), iso: iso, readOnly: readOnly}
}

func (t *Txn) String() string   { return "kv txn " + t.id.String() }
func (t *Txn) IsReadOnly() bool { return t.readOnly }
func (t *Txn) ID() kv.TxnID     { return t.id }

// ensure begins the KV transaction if needed and returns its header.
func (t *Txn) ensure(ctx context.Context) (kvrpc.Header, error) {
	if t.dead != nil {
		return kvrpc.Header{}, t.dead
	}
	if t.begun {
		return kvrpc.Header{Partition: t.store.partition, Epoch: t.epoch}, nil
	}
	h, c, err := t.store.header(ctx)
	if err != nil {
		return h, err
	}
	if err := c.Begin(ctx, &kvrpc.BeginReq{H: h, Txn: t.id, Isolation: t.iso}); err != nil {
		if kv.KindOf(err) == kv.KindEpochChanged {
			t.store.invalidate(h.Epoch)
		}
		return h, err
	}
	t.begun, t.client, t.epoch = true, c, h.Epoch
	t.store.mu.Lock()
	t.store.open[t] = struct{}{}
	t.store.mu.Unlock()
	return h, nil
}

// retryable runs op. When the owner changed before this transaction did
// anything, it re-resolves the owner (waking the partition if it was
// suspended) and begins again there; once the transaction has done
// something, losing its owner aborts it (D5, slice 6).
func (t *Txn) retryable(ctx context.Context, op func(h kvrpc.Header) error) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	backoff := 20 * time.Millisecond
	for attempt := 0; ; attempt++ {
		h, err := t.ensure(ctx)
		if err == nil {
			err = op(h)
		}
		if err == nil {
			t.ops++
			return nil
		}
		if kv.KindOf(err) == kv.KindDeadlock {
			t.markDead(err) // the owner rolled the whole transaction back
			return err
		}
		if kv.KindOf(err) != kv.KindEpochChanged && kv.KindOf(err) != kv.KindTxnNotFound {
			return err
		}
		t.store.invalidate(h.Epoch)
		if t.ops > 0 || t.wrote || attempt >= 20 {
			t.markDead(err)
			return err
		}
		t.forget()
		if attempt > 0 { // e.g. the partition is still suspending
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
			if backoff < time.Second {
				backoff *= 2
			}
		}
	}
}

// forget drops the KV transaction so the next operation begins a fresh one.
func (t *Txn) forget() {
	t.store.mu.Lock()
	delete(t.store.open, t)
	t.store.mu.Unlock()
	t.begun, t.client, t.epoch = false, nil, 0
}

func (t *Txn) markDead(err error) {
	t.dead = err
	t.forget()
}

// Get reads keys; a nil value means absent.
func (t *Txn) Get(ctx context.Context, keys [][]byte, o kvrpc.ReadOpts) ([][]byte, error) {
	var out [][]byte
	err := t.retryable(ctx, func(h kvrpc.Header) error {
		var err error
		out, err = t.client.Get(ctx, &kvrpc.GetReq{H: h, Txn: t.id, Keys: keys, Opts: o})
		return err
	})
	return out, err
}

// Scan reads a key range page.
func (t *Txn) Scan(ctx context.Context, start, end []byte, limit int, o kvrpc.ReadOpts) (*kvrpc.ScanResp, error) {
	var out *kvrpc.ScanResp
	err := t.retryable(ctx, func(h kvrpc.Header) error {
		var err error
		out, err = t.client.Scan(ctx, &kvrpc.ScanReq{H: h, Txn: t.id, Start: start, End: end, Limit: limit, Opts: o})
		return err
	})
	return out, err
}

// Mutate sends writes for statement stmt, waiting up to lockWait for row locks.
func (t *Txn) Mutate(ctx context.Context, stmt uint64, muts []kv.Mutation, lockWait time.Duration) error {
	if len(muts) == 0 {
		return nil
	}
	err := t.retryable(ctx, func(h kvrpc.Header) error {
		return t.client.Mutate(ctx, &kvrpc.MutateReq{H: h, Txn: t.id, Stmt: stmt, Mutations: muts, LockWaitMs: lockWait.Milliseconds()})
	})
	if err == nil || kv.KindOf(err) == kv.KindDuplicateKey || kv.KindOf(err) == kv.KindTxnTooLarge || kv.KindOf(err) == kv.KindLockTimeout {
		t.mu.Lock()
		t.wrote = true
		t.mu.Unlock()
	}
	return err
}

// AutoIncrement reserves count values of table's counter after raising it to
// atLeast, and returns the first (D7). The counter is saved when this
// transaction commits.
func (t *Txn) AutoIncrement(ctx context.Context, table uint32, count, atLeast uint64) (uint64, error) {
	var first uint64
	err := t.retryable(ctx, func(h kvrpc.Header) error {
		var err error
		first, err = t.client.AutoIncrement(ctx, &kvrpc.AutoIncReq{H: h, Txn: t.id, Table: table, Count: count, AtLeast: atLeast})
		return err
	})
	if err == nil {
		t.mu.Lock()
		t.wrote = true // the commit carries the counter
		t.mu.Unlock()
	}
	return first, err
}

// RollbackStatement undoes the writes of statement stmt.
func (t *Txn) RollbackStatement(ctx context.Context, stmt uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.begun || t.dead != nil {
		return nil
	}
	return t.client.RollbackStatement(ctx, &kvrpc.StmtReq{H: kvrpc.Header{Partition: t.store.partition, Epoch: t.epoch}, Txn: t.id, Stmt: stmt})
}

// Commit commits the KV transaction. If the owner changed while the commit
// was in flight, it asks the new owner whether the commit landed (D5, D7).
func (t *Txn) Commit(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	defer t.forget()
	if t.dead != nil {
		return t.dead
	}
	if !t.begun {
		return nil
	}
	h := kvrpc.Header{Partition: t.store.partition, Epoch: t.epoch}
	if !t.wrote {
		// Nothing to commit; release the snapshot. If the owner changed, the
		// transaction went with it, and its reads were already consistent.
		err := t.client.Rollback(ctx, &kvrpc.TxnReq{H: h, Txn: t.id})
		if kv.KindOf(err) == kv.KindEpochChanged {
			t.store.invalidate(h.Epoch)
			return nil
		}
		return err
	}
	_, err := t.client.Commit(ctx, &kvrpc.TxnReq{H: h, Txn: t.id})
	switch kv.KindOf(err) {
	case kv.KindEpochChanged, kv.KindUnavailable:
		t.store.invalidate(h.Epoch)
		if committed, serr := t.statusOnNewOwner(ctx); serr == nil && committed {
			return nil
		}
	}
	return err
}

func (t *Txn) statusOnNewOwner(ctx context.Context) (bool, error) {
	var lastErr error
	for i := 0; i < 20; i++ {
		h, c, err := t.store.header(ctx)
		if err == nil {
			var st *kvrpc.TxnStatusResp
			st, err = c.TxnStatus(ctx, &kvrpc.TxnReq{H: h, Txn: t.id})
			if err == nil {
				return st.State == kv.TxnCommitted, nil
			}
			t.store.invalidate(h.Epoch)
		}
		lastErr = err
		select {
		case <-time.After(250 * time.Millisecond):
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	return false, lastErr
}

// Rollback aborts the KV transaction.
func (t *Txn) Rollback(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	defer t.forget()
	if !t.begun || t.dead != nil {
		return nil
	}
	err := t.client.Rollback(ctx, &kvrpc.TxnReq{H: kvrpc.Header{Partition: t.store.partition, Epoch: t.epoch}, Txn: t.id})
	if kv.KindOf(err) == kv.KindEpochChanged {
		return nil // the owner is gone, and with it the transaction
	}
	return err
}

// errIsKind reports whether err is a KV error of the given kind.
func errIsKind(err error, k kv.ErrKind) bool {
	var ke *kv.Error
	return errors.As(err, &ke) && ke.Kind == k
}
