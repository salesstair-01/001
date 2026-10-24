package kv

import (
	"bytes"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/objstorage/remote"

	"github.com/salesstair-01/001/internal/encoding"
)

// The engine is the local LSM under a partition: today Pebble v2 with its WAL
// off, later possibly an object-store-aware LSM of our own. This file is the
// only one that imports Pebble; everything else goes through these interfaces.
//
// Every key handed to the engine, bounds and seek keys included, is an MVCC
// key (user key · inverted commit_ts). The comparer orders by user key first
// and commit_ts second, so a user key that is a prefix of another still sorts
// all of its versions before the longer key's (issue #3). lowerBound and
// pastVersions turn user-key bounds into MVCC keys.

type engine interface {
	// NewIter iterates [lower, upper); nil leaves that side unbounded.
	NewIter(lower, upper []byte) (engineIter, error)
	NewBatch() engineBatch
	// Flush writes the memtable to SSTs, so a checkpoint holds every write.
	Flush() error
	// Checkpoint hard-links a consistent copy of the engine's files into dir.
	Checkpoint(dir string) error
	Close() error
}

type engineIter interface {
	First() bool
	Next() bool
	SeekGE(key []byte) bool
	Key() []byte
	Value() []byte
	Error() error
	Close() error
}

type engineBatch interface {
	Set(key, value []byte) error
	Delete(key []byte) error
	// DeleteRange deletes [start, end); later sets in the same batch win.
	DeleteRange(start, end []byte) error
	Len() int
	Commit() error
}

// engineOptions tune one engine instance.
type engineOptions struct {
	cache         *blockCache   // nil = a private cache
	memTableBytes int64         // 0 = the engine's default
	remote        *remoteConfig // nil = every SST is local
}

// engineFormat is the on-disk format major version, pinned so that a Pebble
// upgrade never changes the format of the files we upload without us noticing.
const engineFormat = pebble.FormatValueSeparation

// mvccComparer orders keys by user key (everything but the last 8 bytes),
// then by the inverted commit_ts, so newer versions come first. Separator
// and Successor return their input: shortening a key could cut into the
// suffix and change its user key.
var mvccComparer = &pebble.Comparer{
	Name:                 "s2z.mvcc.v2",
	Split:                encoding.Split,
	ComparePointSuffixes: bytes.Compare,
	CompareRangeSuffixes: bytes.Compare,
	AbbreviatedKey: func(k []byte) uint64 {
		return pebble.DefaultComparer.AbbreviatedKey(k[:encoding.Split(k)])
	},
	Separator: func(dst, a, _ []byte) []byte { return append(dst, a...) },
	Successor: func(dst, a []byte) []byte { return append(dst, a...) },
}

// lowerBound returns the first MVCC key of user key uk (its newest possible
// version), so [lowerBound(a), lowerBound(b)) holds every version of every
// user key in [a, b). nil stays nil (unbounded).
func lowerBound(uk []byte) []byte {
	if uk == nil {
		return nil
	}
	return encoding.MVCCKey(uk, math.MaxUint64)
}

// pastVersions returns the first MVCC key after every version of uk.
func pastVersions(uk []byte) []byte {
	return lowerBound(append(append(make([]byte, 0, len(uk)+1), uk...), 0))
}

// blockCache is a block cache shared by the engines of a node.
type blockCache struct{ c *pebble.Cache }

func newBlockCache(bytes int64) *blockCache { return &blockCache{c: pebble.NewCache(bytes)} }

func (b *blockCache) unref() { b.c.Unref() }

func openEngine(dir string, o engineOptions) (engine, error) {
	opts := &pebble.Options{
		Comparer:                    mvccComparer,
		FormatMajorVersion:          engineFormat,
		DisableWAL:                  true, // the object-store WAL is the source of truth
		Logger:                      quietLogger{},
		MemTableStopWritesThreshold: 4,
		CompactionConcurrencyRange:  func() (int, int) { return 1, 1 },
	}
	if o.cache != nil {
		opts.Cache = o.cache.c
	}
	if o.memTableBytes > 0 {
		opts.MemTableSize = uint64(o.memTableBytes)
	}
	if r := o.remote; r != nil {
		if err := fixCatalogMarker(dir); err != nil {
			return nil, err
		}
		if r.sizes == nil {
			r.sizes = map[string]int64{}
		}
		opts.Experimental.RemoteStorage = r
		opts.Experimental.SecondaryCacheSizeBytes = r.cacheBytes
		if r.epoch != 0 {
			opts.Experimental.CreateOnShared = remote.CreateOnSharedAll
			opts.Experimental.CreateOnSharedLocator = remoteLocator(r.epoch)
		}
	}
	sched := newGatedScheduler()
	opts.Experimental.CompactionScheduler = sched
	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	defer sched.open()
	if o.remote != nil {
		if err := db.SetCreatorID(o.remote.creatorID); err != nil {
			db.Close()
			return nil, err
		}
	}
	return pebbleEngine{db}, nil
}

type quietLogger struct{}

func (quietLogger) Infof(string, ...interface{})      {}
func (quietLogger) Errorf(f string, a ...interface{}) { slog.Error(fmt.Sprintf(f, a...)) }
func (quietLogger) Fatalf(f string, a ...interface{}) { panic(fmt.Sprintf(f, a...)) }

type pebbleEngine struct{ db *pebble.DB }

func (e pebbleEngine) NewIter(lower, upper []byte) (engineIter, error) {
	it, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, err
	}
	return it, nil
}

func (e pebbleEngine) NewBatch() engineBatch       { return pebbleBatch{e.db.NewBatch()} }
func (e pebbleEngine) Flush() error                { return e.db.Flush() }
func (e pebbleEngine) Checkpoint(dir string) error { return e.db.Checkpoint(dir) }
func (e pebbleEngine) Close() error                { return e.db.Close() }

type pebbleBatch struct{ b *pebble.Batch }

func (b pebbleBatch) Set(k, v []byte) error         { return b.b.Set(k, v, nil) }
func (b pebbleBatch) Delete(k []byte) error         { return b.b.Delete(k, nil) }
func (b pebbleBatch) DeleteRange(s, e []byte) error { return b.b.DeleteRange(s, e, nil) }
func (b pebbleBatch) Len() int                      { return b.b.Len() }
func (b pebbleBatch) Commit() error                 { return b.b.Commit(pebble.NoSync) }

// gatedScheduler runs one compaction at a time, like Pebble's default
// scheduler, but none until open() is called. Pebble's Open waits for every
// compaction running when it scans for obsolete files, so a compaction that
// starts during Open (an L0 backlog left by the previous owner) would delay
// the wake by its whole run, which on remote SSTs is GETs and PUTs of the
// tables it rewrites. openEngine opens the gate once Open has returned.
type gatedScheduler struct {
	mu      sync.Mutex
	cond    *sync.Cond
	db      pebble.DBForCompaction
	gateOn  bool
	gone    bool
	busy    bool // granting
	running int
	poke    chan struct{}
	stop    chan struct{}
}

func newGatedScheduler() *gatedScheduler {
	s := &gatedScheduler{poke: make(chan struct{}, 1), stop: make(chan struct{})}
	s.cond = sync.NewCond(&s.mu)
	return s
}

func (s *gatedScheduler) Register(_ int, db pebble.DBForCompaction) {
	s.db = db
	go func() {
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-t.C:
			case <-s.poke:
			case <-s.stop:
				return
			}
			s.grant()
		}
	}()
}

func (s *gatedScheduler) Unregister() {
	close(s.stop)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gone = true
	for s.busy {
		s.cond.Wait()
	}
}

func (s *gatedScheduler) TrySchedule() (bool, pebble.CompactionGrantHandle) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.gateOn || s.gone || s.db.GetAllowedWithoutPermission() <= s.running {
		return false, nil
	}
	s.running++
	return true, s
}

func (s *gatedScheduler) UpdateGetAllowedWithoutPermission() { s.nudge() }

func (s *gatedScheduler) nudge() {
	select {
	case s.poke <- struct{}{}:
	default:
	}
}

// open lets compactions run.
func (s *gatedScheduler) open() {
	s.mu.Lock()
	s.gateOn = true
	s.mu.Unlock()
	s.nudge()
}

// grant starts waiting compactions up to the allowed number.
func (s *gatedScheduler) grant() {
	s.mu.Lock()
	if !s.gateOn || s.gone || s.busy {
		s.mu.Unlock()
		return
	}
	n := s.db.GetAllowedWithoutPermission() - s.running
	if n <= 0 {
		s.mu.Unlock()
		return
	}
	s.busy = true
	s.mu.Unlock()
	for ; n > 0; n-- {
		if ok, _ := s.db.GetWaitingCompaction(); !ok || !s.db.Schedule(s) {
			break
		}
		s.mu.Lock()
		s.running++
		s.mu.Unlock()
	}
	s.mu.Lock()
	s.busy = false
	s.cond.Broadcast()
	s.mu.Unlock()
}

// The scheduler is its own grant handle.
func (s *gatedScheduler) Started()                                          {}
func (s *gatedScheduler) MeasureCPU(pebble.CompactionGoroutineKind)         {}
func (s *gatedScheduler) CumulativeStats(pebble.CompactionGrantHandleStats) {}

func (s *gatedScheduler) Done() {
	s.mu.Lock()
	s.running--
	s.mu.Unlock()
	s.nudge()
}
