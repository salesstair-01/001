package kv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"

	"github.com/salesstair-01/001/internal/encoding"
	"github.com/salesstair-01/001/internal/objstore"
	"github.com/salesstair-01/001/internal/wal"
)

// Config tunes a KV owner. Zero values take the defaults from D4–D7.
type Config struct {
	Store   objstore.Store
	DataDir string // local directory for Pebble instances
	OwnerID string // unique id of this process, recorded in manifests

	GroupWindow        time.Duration // group-commit window when busy (10 ms)
	GroupMaxBytes      int           // group-commit cap (1 MiB)
	CheckpointBytes    int64         // checkpoint after this much WAL (64 MiB)
	CheckpointInterval time.Duration // checkpoint at least this often when dirty (10 min)
	GCGrace            time.Duration // delay before deleting covered WAL and unreferenced SSTs (1 h)
	TxnIDWindow        time.Duration // how long committed txn ids are remembered (10 min)
	IntentCap          int           // per-transaction intent cap (64 MiB)
	CommitRetryTimeout time.Duration // how long a WAL PUT is retried before giving up (60 s)
	MVCCGCInterval     time.Duration // how often old versions are collected (1 min)
	TxnLease           time.Duration // abort transactions without a heartbeat for this long (10 s)

	Logger *slog.Logger
}

func (c *Config) withDefaults() Config {
	d := *c
	if d.GroupWindow == 0 {
		d.GroupWindow = 10 * time.Millisecond
	}
	if d.GroupMaxBytes == 0 {
		d.GroupMaxBytes = 1 << 20
	}
	if d.CheckpointBytes == 0 {
		d.CheckpointBytes = 64 << 20
	}
	if d.CheckpointInterval == 0 {
		d.CheckpointInterval = 10 * time.Minute
	}
	if d.GCGrace == 0 {
		d.GCGrace = time.Hour
	}
	if d.TxnIDWindow == 0 {
		d.TxnIDWindow = 10 * time.Minute
	}
	if d.IntentCap == 0 {
		d.IntentCap = 64 << 20
	}
	if d.CommitRetryTimeout == 0 {
		d.CommitRetryTimeout = 60 * time.Second
	}
	if d.MVCCGCInterval == 0 {
		d.MVCCGCInterval = time.Minute
	}
	if d.TxnLease == 0 {
		d.TxnLease = 10 * time.Second
	}
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	return d
}

// pebbleComparer is the default byte-wise comparer with Split marking the
// 8-byte commit_ts suffix, so bloom filters and prefix seeks ignore versions.
var pebbleComparer = func() *pebble.Comparer {
	c := *pebble.DefaultComparer
	c.Split = encoding.Split
	c.Name = "s2z.mvcc.v1"
	return &c
}()

func pebbleOptions() *pebble.Options {
	return &pebble.Options{
		Comparer:   pebbleComparer,
		DisableWAL: true, // the object-store WAL is the source of truth
		Logger:     quietLogger{},
	}
}

type quietLogger struct{}

func (quietLogger) Infof(string, ...interface{})      {}
func (quietLogger) Errorf(f string, a ...interface{}) { slog.Error(fmt.Sprintf(f, a...)) }
func (quietLogger) Fatalf(f string, a ...interface{}) { panic(fmt.Sprintf(f, a...)) }

type pendingWrite struct {
	ts    uint64
	value []byte
}

type committedTxn struct {
	ts   uint64
	wall time.Time
}

// Partition is one partition owned by this process.
type Partition struct {
	id    uint64
	epoch uint64
	cfg   Config
	store objstore.Store
	log   *slog.Logger
	db    *pebble.DB
	dir   string

	// mu protects everything below it except where noted.
	mu          sync.Mutex
	maxCommitTS uint64 // highest commit_ts assigned
	txns        map[TxnID]*Txn
	pending     map[string]pendingWrite // in-flight commits not yet applied to Pebble
	recent      map[TxnID]committedTxn  // committed ids from the last TxnIDWindow
	queue       []*commitReq            // commits waiting for the group flusher
	stopErr     error                   // set once the partition stops serving

	visibleTS atomic.Uint64 // highest commit_ts applied and readable

	// Owned by the flusher goroutine, read by checkpoint under applyMu.
	applyMu        sync.Mutex
	nextWALSeq     uint64
	lastAppliedSeq uint64
	walBytes       int64 // WAL bytes since the last checkpoint
	dirty          bool  // anything committed since the last checkpoint

	// Checkpoint state, owned by the checkpointer (ckptMu).
	ckptMu    sync.Mutex
	manifestN uint64
	manifest  Manifest
	uploaded  map[string]FileRef // local SST name -> uploaded object
	garbage   []garbageItem

	queueSignal chan struct{}
	stopCh      chan struct{}
	wg          sync.WaitGroup
	stopOnce    sync.Once
	closeOnce   sync.Once
	closeErr    error
}

// ID returns the partition id.
func (p *Partition) ID() uint64 { return p.id }

// Epoch returns the epoch this owner serves.
func (p *Partition) Epoch() uint64 { return p.epoch }

// VisibleTS returns the highest readable commit_ts.
func (p *Partition) VisibleTS() uint64 { return p.visibleTS.Load() }

// Takeover makes this process the owner of partition pid (D4, steps 1–5).
// The caller registers the owner and epoch with the directory (step 6).
func Takeover(ctx context.Context, cfg Config, pid uint64) (*Partition, error) {
	cfg = cfg.withDefaults()
	s := cfg.Store
	log := cfg.Logger.With("partition", pid)

	// 1. List the manifests and read the highest, N.
	n, m, err := latestManifest(ctx, s, pid)
	if err != nil {
		return nil, fmt.Errorf("takeover: read manifest: %w", err)
	}
	// 2. Write manifest/N+1: same content, epoch + 1, put-if-absent.
	next := *m
	next.Epoch = m.Epoch + 1
	next.Owner = cfg.OwnerID
	if err := s.PutIfAbsent(ctx, manifestKey(pid, n+1), next.encode()); err != nil {
		if errors.Is(err, objstore.ErrPreconditionFailed) {
			return nil, errTakeoverLost
		}
		// The write may have landed with its response lost; check before giving up.
		if b, gerr := s.Get(ctx, manifestKey(pid, n+1)); gerr != nil || !bytes.Equal(b, next.encode()) {
			return nil, fmt.Errorf("takeover: write manifest: %w", err)
		}
	}
	p := &Partition{
		id: pid, epoch: next.Epoch, cfg: cfg, store: s, log: log.With("epoch", next.Epoch),
		txns: map[TxnID]*Txn{}, pending: map[string]pendingWrite{}, recent: map[TxnID]committedTxn{},
		manifestN: n + 1, manifest: next, uploaded: map[string]FileRef{},
		queueSignal: make(chan struct{}, 1), stopCh: make(chan struct{}),
	}
	for _, f := range next.Files {
		p.uploaded[f.Name] = f
	}

	// 3. Download the listed files and open Pebble.
	p.dir = filepath.Join(cfg.DataDir, fmt.Sprintf("p%d-e%d-%d", pid, next.Epoch, time.Now().UnixNano()))
	if err := os.MkdirAll(p.dir, 0o755); err != nil {
		return nil, err
	}
	for _, f := range next.Files {
		b, err := s.Get(ctx, f.Object)
		if err != nil {
			return nil, fmt.Errorf("takeover: download %s: %w", f.Object, err)
		}
		if err := os.WriteFile(filepath.Join(p.dir, f.Name), b, 0o644); err != nil {
			return nil, err
		}
	}
	p.db, err = pebble.Open(p.dir, pebbleOptions())
	if err != nil {
		return nil, fmt.Errorf("takeover: open pebble: %w", err)
	}
	p.maxCommitTS = next.MaxCommitTS
	if err := p.loadTxnIDs(); err != nil {
		p.db.Close()
		return nil, err
	}

	// 4. Replay the WAL segments after replay_after_wal_seq, in order.
	seq := next.ReplayAfterWALSeq + 1
	for {
		b, err := s.Get(ctx, walKey(pid, seq))
		if errors.Is(err, objstore.ErrNotFound) {
			break
		}
		if err != nil {
			p.db.Close()
			return nil, fmt.Errorf("takeover: read wal %d: %w", seq, err)
		}
		if err := p.replaySegment(b); err != nil {
			p.db.Close()
			return nil, fmt.Errorf("takeover: replay wal %d: %w", seq, err)
		}
		seq++
	}
	// 5. Write an empty fence segment at the next sequence number. A 412 means
	// the old owner slipped in one more segment: replay it and try the next.
	for {
		fence := (&wal.Segment{Epoch: p.epoch, Seq: seq}).Encode()
		err := putWithRetry(ctx, s, walKey(pid, seq), fence, cfg.CommitRetryTimeout)
		if err == nil {
			break
		}
		if !errors.Is(err, objstore.ErrPreconditionFailed) {
			p.db.Close()
			return nil, fmt.Errorf("takeover: fence: %w", err)
		}
		b, err := s.Get(ctx, walKey(pid, seq))
		if err != nil {
			p.db.Close()
			return nil, fmt.Errorf("takeover: read raced wal %d: %w", seq, err)
		}
		if bytes.Equal(b, fence) {
			break // our own fence, with a lost response
		}
		if err := p.replaySegment(b); err != nil {
			p.db.Close()
			return nil, err
		}
		seq++
	}
	p.nextWALSeq = seq + 1
	p.lastAppliedSeq = seq
	p.visibleTS.Store(p.maxCommitTS)
	p.dirty = seq > next.ReplayAfterWALSeq+1 // anything replayed is worth a checkpoint

	p.wg.Add(2)
	go p.flusher()
	go p.background()
	p.log.Info("took over partition", "manifest", p.manifestN, "replayed_through", seq-1, "max_commit_ts", p.maxCommitTS)
	return p, nil
}

// replaySegment applies one WAL segment to Pebble during takeover.
func (p *Partition) replaySegment(b []byte) error {
	seg, err := wal.Decode(b)
	if err != nil {
		return err
	}
	batch := p.db.NewBatch()
	for _, r := range seg.Records {
		for _, w := range r.Writes {
			if err := batch.Set(encoding.MVCCKey(w.Key, r.CommitTS), w.Value, nil); err != nil {
				return err
			}
		}
		if r.CommitTS > p.maxCommitTS {
			p.maxCommitTS = r.CommitTS
		}
		p.recent[r.TxnID] = committedTxn{ts: r.CommitTS, wall: time.UnixMicro(r.WallMicros)}
	}
	return batch.Commit(pebble.NoSync)
}

// putWithRetry writes a put-if-absent object, retrying the same content on
// errors other than 412. A 412 on a retry may be our own earlier attempt
// landing; callers compare the content.
func putWithRetry(ctx context.Context, s objstore.Store, key string, data []byte, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	backoff := 10 * time.Millisecond
	for {
		err := s.PutIfAbsent(ctx, key, data)
		if err == nil || errors.Is(err, objstore.ErrPreconditionFailed) {
			return err
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return err
		}
		time.Sleep(backoff)
		if backoff < time.Second {
			backoff *= 2
		}
	}
}

// stop makes the partition stop serving: every open and queued transaction is
// aborted with err. It is safe to call more than once.
func (p *Partition) stop(err error) {
	p.stopOnce.Do(func() {
		p.mu.Lock()
		p.stopErr = err
		queue := p.queue
		p.queue = nil
		for id, t := range p.txns {
			t.abortLocked()
			delete(p.txns, id)
		}
		p.mu.Unlock()
		for _, c := range queue {
			c.done <- commitResult{err: err}
		}
		close(p.stopCh)
		p.log.Warn("partition stopped serving", "reason", err)
	})
}

// Close stops serving without a checkpoint and releases local resources.
func (p *Partition) Close() error {
	p.stop(errStopped)
	p.closeOnce.Do(func() {
		p.wg.Wait()
		p.closeErr = p.db.Close()
		_ = os.RemoveAll(p.dir)
	})
	return p.closeErr
}

// Suspend waits for open transactions to finish, writes a checkpoint and
// stops serving, so the next wake has almost no WAL to replay (D4).
func (p *Partition) Suspend(ctx context.Context) error {
	p.mu.Lock()
	open := len(p.txns)
	p.mu.Unlock()
	if open > 0 {
		return fmt.Errorf("kv: %d open transactions", open)
	}
	if err := p.Checkpoint(ctx); err != nil {
		return err
	}
	return p.Close()
}

// Stopped reports whether the partition has stopped serving and why.
func (p *Partition) Stopped() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stopErr
}

func (p *Partition) checkEpoch(epoch uint64) error {
	if epoch != 0 && epoch != p.epoch {
		return newErr(KindEpochChanged, "epoch %d requested, owner serves %d", epoch, p.epoch)
	}
	return nil
}
