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

	// IdleSuspend suspends a partition with no open transactions and no
	// requests for this long (0 = never). OnSuspend runs after each suspend,
	// and OnEmpty when the node no longer serves any partition (slice 6).
	IdleSuspend time.Duration
	OnSuspend   func(partition, epoch uint64)
	OnEmpty     func()

	// CacheDir keeps downloaded and uploaded SSTs across restarts on this host
	// (default DataDir/cache); CacheMaxBytes caps it (default 10 GiB, <0 disables).
	CacheDir      string
	CacheMaxBytes int64

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
	if d.CacheDir == "" && d.DataDir != "" {
		d.CacheDir = filepath.Join(d.DataDir, "cache")
	}
	if d.CacheMaxBytes == 0 {
		d.CacheMaxBytes = 10 << 30
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
	autoinc     map[uint32]uint64       // next AUTO_INCREMENT value per table, loaded on first use
	stopErr     error                   // set once the partition stops serving

	visibleTS  atomic.Uint64 // highest commit_ts applied and readable
	lastActive atomic.Int64  // unix nanos of the last request, for idle detection
	suspending bool          // guarded by mu: refusing new transactions while suspending

	locks *lockTable // row locks; take p.mu before locks.mu, never the reverse

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
	cache     *fileCache         // local copies of checkpoint files; may be nil
	cacheHits int                // files the takeover found in the cache
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
		autoinc:   map[uint32]uint64{},
		manifestN: n + 1, manifest: next, uploaded: map[string]FileRef{},
		queueSignal: make(chan struct{}, 1), stopCh: make(chan struct{}),
	}
	for _, f := range next.Files {
		p.uploaded[f.Name] = f
	}
	p.locks = newLockTable(p.abortTxn)

	// 3. Download the listed files and open Pebble.
	p.dir = filepath.Join(cfg.DataDir, fmt.Sprintf("p%d-e%d-%d", pid, next.Epoch, time.Now().UnixNano()))
	if err := os.MkdirAll(p.dir, 0o755); err != nil {
		return nil, err
	}
	p.cache = newFileCache(cfg.CacheDir, cfg.CacheMaxBytes)
	start := time.Now()
	hits, err := p.installFiles(ctx, next.Files)
	if err != nil {
		return nil, err
	}
	downloaded := time.Since(start)
	p.cacheHits = hits
	p.cache.evict()
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

	p.lastActive.Store(time.Now().UnixNano())
	p.wg.Add(2)
	go p.flusher()
	go p.background()
	p.log.Info("took over partition", "manifest", p.manifestN, "replayed_through", seq-1, "max_commit_ts", p.maxCommitTS,
		"files", len(next.Files), "cache_hits", hits, "download", downloaded.Round(time.Millisecond))
	return p, nil
}

// installFiles puts a checkpoint's files into the Pebble directory, from the
// local cache or object storage, several at a time.
func (p *Partition) installFiles(ctx context.Context, files []FileRef) (int, error) {
	var (
		mu    sync.Mutex
		hits  int
		first error
		wg    sync.WaitGroup
	)
	work := make(chan FileRef)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for f := range work {
				hit, err := p.cache.install(ctx, p.store, f.Object, filepath.Join(p.dir, f.Name))
				mu.Lock()
				if hit {
					hits++
				}
				if err != nil && first == nil {
					first = fmt.Errorf("takeover: download %s: %w", f.Object, err)
				}
				mu.Unlock()
			}
		}()
	}
	for _, f := range files {
		work <- f
	}
	close(work)
	wg.Wait()
	return hits, first
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
		for _, t := range p.txns {
			p.abortTxnLocked(t)
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

// Suspend checkpoints and stops serving, so the next wake has almost no WAL
// to replay (D4). It fails if transactions are open.
func (p *Partition) Suspend(ctx context.Context) error {
	ok, err := p.suspend(ctx, 0)
	if err == nil && !ok {
		err = fmt.Errorf("kv: partition %d has open transactions", p.id)
	}
	return err
}

// TrySuspend suspends the partition if it has had no open transactions and
// no requests for idle. It reports whether it suspended.
func (p *Partition) TrySuspend(ctx context.Context, idle time.Duration) (bool, error) {
	return p.suspend(ctx, idle)
}

func (p *Partition) suspend(ctx context.Context, idle time.Duration) (bool, error) {
	p.mu.Lock()
	switch {
	case p.stopErr != nil:
		p.mu.Unlock()
		return false, p.stopErr
	case len(p.txns) > 0, idle > 0 && p.IdleFor() < idle:
		p.mu.Unlock()
		return false, nil
	}
	// From here on new transactions are refused, so none can be cut off by
	// the stop below; their Begin fails with EpochChanged and is retried
	// once the directory has woken the partition again.
	p.suspending = true
	p.mu.Unlock()
	if err := p.Checkpoint(ctx); err != nil {
		p.mu.Lock()
		p.suspending = false
		p.mu.Unlock()
		return false, err
	}
	p.stop(errSuspended)
	return true, p.Close()
}

// FileCount returns how many files the partition's manifest lists.
func (p *Partition) FileCount() int {
	p.ckptMu.Lock()
	defer p.ckptMu.Unlock()
	return len(p.manifest.Files)
}

// CacheHits returns how many checkpoint files the takeover took from the local cache.
func (p *Partition) CacheHits() int { return p.cacheHits }

// IdleFor returns how long the partition has gone without a request.
func (p *Partition) IdleFor() time.Duration {
	return time.Since(time.Unix(0, p.lastActive.Load()))
}

// Stopped reports whether the partition has stopped serving and why.
func (p *Partition) Stopped() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stopErr
}

// Touch records client activity for idle detection. Background work (such as
// ANALYZE) doesn't touch, so it never keeps a tenant awake.
func (p *Partition) Touch() { p.lastActive.Store(time.Now().UnixNano()) }

func (p *Partition) checkEpoch(epoch uint64) error {
	if epoch != 0 && epoch != p.epoch {
		return newErr(KindEpochChanged, "epoch %d requested, owner serves %d", epoch, p.epoch)
	}
	return nil
}
