package kv

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"

	"github.com/salesstair-01/001/internal/encoding"
	"github.com/salesstair-01/001/internal/objstore"
)

type garbageItem struct {
	key   string
	after time.Time
}

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Checkpoint flushes Pebble, uploads new SSTs, and writes manifest N+1 with the
// new file list and replay_after_wal_seq (D4). Covered WAL segments and
// unreferenced SSTs are deleted after the grace period.
func (p *Partition) Checkpoint(ctx context.Context) error {
	p.ckptMu.Lock()
	defer p.ckptMu.Unlock()
	if err := p.Stopped(); err != nil {
		return err
	}

	// Freeze applies while Pebble is flushed and its files are linked.
	p.applyMu.Lock()
	seq := p.lastAppliedSeq
	maxTS := p.visibleTS.Load()
	walBytes, dirty := p.walBytes, p.dirty
	if err := p.saveTxnIDs(); err != nil {
		p.applyMu.Unlock()
		return err
	}
	if err := p.db.Flush(); err != nil {
		p.applyMu.Unlock()
		return err
	}
	ckptID := newID()
	tmp := p.dir + "-ckpt-" + ckptID
	if err := p.db.Checkpoint(tmp); err != nil {
		p.applyMu.Unlock()
		return err
	}
	p.walBytes, p.dirty = 0, false
	p.applyMu.Unlock()
	defer os.RemoveAll(tmp)

	restore := func() {
		p.applyMu.Lock()
		p.walBytes += walBytes
		p.dirty = p.dirty || dirty
		p.applyMu.Unlock()
	}

	// Upload the checkpoint's files. SSTs are immutable, so one already
	// uploaded under the same local name is reused.
	entries, err := os.ReadDir(tmp)
	if err != nil {
		restore()
		return err
	}
	var files []FileRef
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if ref, ok := p.uploaded[name]; ok && strings.HasSuffix(name, ".sst") {
			files = append(files, ref)
			continue
		}
		data, err := os.ReadFile(filepath.Join(tmp, name))
		if err != nil {
			restore()
			return err
		}
		obj := metaKey(p.id, ckptID, name)
		if strings.HasSuffix(name, ".sst") {
			obj = sstKey(p.id, newID())
		}
		if err := putWithRetry(ctx, p.store, obj, data, p.cfg.CommitRetryTimeout); err != nil {
			restore()
			return fmt.Errorf("checkpoint: upload %s: %w", name, err)
		}
		p.cache.add(obj, filepath.Join(tmp, name)) // the next wake on this host skips the download
		ref := FileRef{Name: name, Object: obj, Size: int64(len(data))}
		if strings.HasSuffix(name, ".sst") {
			p.uploaded[name] = ref
		}
		files = append(files, ref)
	}

	old := p.manifest
	next := Manifest{
		Epoch: p.epoch, Owner: p.cfg.OwnerID, Files: files,
		ReplayAfterWALSeq: seq, MaxCommitTS: maxTS, CheckpointID: ckptID,
	}
	data := next.encode()
	key := manifestKey(p.id, p.manifestN+1)
	err = putWithRetry(ctx, p.store, key, data, p.cfg.CommitRetryTimeout)
	if errors.Is(err, objstore.ErrPreconditionFailed) {
		if existing, gerr := getWithRetry(ctx, p.store, key, p.cfg.CommitRetryTimeout); gerr == nil && bytes.Equal(existing, data) {
			err = nil
		} else {
			p.stop(errFenced)
			return errFenced
		}
	}
	if err != nil {
		restore()
		return fmt.Errorf("checkpoint: write manifest: %w", err)
	}
	p.manifestN++
	p.manifest = next

	// Schedule garbage: covered WAL segments, files the new manifest no longer
	// references, and manifests older than the previous one.
	after := time.Now().Add(p.cfg.GCGrace)
	for s := old.ReplayAfterWALSeq + 1; s <= seq; s++ {
		p.garbage = append(p.garbage, garbageItem{walKey(p.id, s), after})
	}
	live := map[string]bool{}
	for _, f := range files {
		live[f.Object] = true
	}
	for _, f := range old.Files {
		if !live[f.Object] {
			p.garbage = append(p.garbage, garbageItem{f.Object, after})
		}
	}
	for name, ref := range p.uploaded {
		if !live[ref.Object] {
			delete(p.uploaded, name)
		}
	}
	if p.manifestN >= 3 {
		p.garbage = append(p.garbage, garbageItem{manifestKey(p.id, p.manifestN-2), after})
	}
	p.log.Debug("checkpoint", "manifest", p.manifestN, "replay_after", seq, "files", len(files))
	return nil
}

// collectGarbage deletes scheduled objects whose grace period has passed.
func (p *Partition) collectGarbage(ctx context.Context) {
	p.ckptMu.Lock()
	defer p.ckptMu.Unlock()
	now := time.Now()
	kept := p.garbage[:0]
	for _, g := range p.garbage {
		if now.After(g.after) {
			if err := p.store.Delete(ctx, g.key); err == nil {
				continue
			}
		}
		kept = append(kept, g)
	}
	p.garbage = kept
}

// ---- committed transaction ids (system table 4) ----------------------------

func (p *Partition) txnIDKey(id TxnID) []byte {
	k := encoding.AppendBytes(encoding.Prefix(p.id, encoding.TablePartitionMeta, 0), []byte("txn"), false)
	k = encoding.AppendBytes(k, id[:], false)
	return encoding.MVCCKey(k, 0)
}

func (p *Partition) txnIDPrefix() []byte {
	return encoding.AppendBytes(encoding.Prefix(p.id, encoding.TablePartitionMeta, 0), []byte("txn"), false)
}

// saveTxnIDs writes the recent committed ids into system table 4 so that they
// survive in the checkpoint. Called with applyMu held.
func (p *Partition) saveTxnIDs() error {
	p.mu.Lock()
	cutoff := time.Now().Add(-p.cfg.TxnIDWindow)
	b := p.db.NewBatch()
	// Rewrite the table: the range delete drops expired ids, and the sets that
	// follow it in the same batch win over it.
	prefix := p.txnIDPrefix()
	_ = b.DeleteRange(prefix, encoding.PrefixEnd(prefix), nil)
	for id, c := range p.recent {
		k := p.txnIDKey(id)
		if c.wall.Before(cutoff) {
			delete(p.recent, id)
			continue
		}
		v := binary.BigEndian.AppendUint64(nil, c.ts)
		v = binary.BigEndian.AppendUint64(v, uint64(c.wall.UnixMicro()))
		_ = b.Set(k, v, nil)
	}
	p.mu.Unlock()
	return b.Commit(pebble.NoSync)
}

// loadTxnIDs reads system table 4 during takeover, dropping expired ids.
func (p *Partition) loadTxnIDs() error {
	prefix := p.txnIDPrefix()
	it, err := p.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: encoding.PrefixEnd(prefix)})
	if err != nil {
		return err
	}
	defer it.Close()
	cutoff := time.Now().Add(-p.cfg.TxnIDWindow)
	for ok := it.First(); ok; ok = it.Next() {
		uk, _, err := encoding.SplitMVCC(it.Key())
		if err != nil {
			return err
		}
		_, rest, _ := encoding.DecodeBytes(uk[encoding.PrefixLen:], false)
		raw, _, err := encoding.DecodeBytes(rest, false)
		if err != nil || len(raw) != 16 || len(it.Value()) != 16 {
			continue
		}
		wall := time.UnixMicro(int64(binary.BigEndian.Uint64(it.Value()[8:])))
		if wall.Before(cutoff) {
			continue
		}
		p.recent[TxnID(raw)] = committedTxn{ts: binary.BigEndian.Uint64(it.Value()), wall: wall}
	}
	return it.Error()
}

// ---- MVCC garbage collection -------------------------------------------------

// gcVersions drops versions no snapshot can read: for each user key, every
// version older than the newest one at or below the safe point, and that one
// too if it is a delete marker. These deletes are local; the next checkpoint
// carries them, and a WAL replay only re-adds versions that are equally invisible.
func (p *Partition) gcVersions() (int, error) {
	p.mu.Lock()
	safe := p.oldestSnapshotLocked()
	p.mu.Unlock()
	if safe == 0 {
		return 0, nil
	}
	lower := encoding.PartitionPrefix(p.id)
	it, err := p.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: encoding.PrefixEnd(lower)})
	if err != nil {
		return 0, err
	}
	defer it.Close()
	b := p.db.NewBatch()
	var cur []byte
	keptBelowSafe := false
	dropped := 0
	for ok := it.First(); ok; ok = it.Next() {
		uk, ts, err := encoding.SplitMVCC(it.Key())
		if err != nil {
			return dropped, err
		}
		if ts == 0 {
			continue // system rows without versions
		}
		if !bytes.Equal(uk, cur) {
			cur = append(cur[:0], uk...)
			keptBelowSafe = false
		}
		if ts > safe {
			continue
		}
		if !keptBelowSafe {
			keptBelowSafe = true
			if !encoding.IsDelete(it.Value()) {
				continue
			}
		}
		_ = b.Delete(it.Key(), nil)
		dropped++
		if b.Len() > 4<<20 {
			if err := b.Commit(pebble.NoSync); err != nil {
				return dropped, err
			}
			b = p.db.NewBatch()
		}
	}
	if err := it.Error(); err != nil {
		return dropped, err
	}
	return dropped, b.Commit(pebble.NoSync)
}

// background runs periodic work: checkpoints, lease expiry, id expiry,
// MVCC GC and object garbage collection.
func (p *Partition) background() {
	defer p.wg.Done()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	lastCkpt, lastGC := time.Now(), time.Now()
	var gcAtTS uint64
	for {
		select {
		case <-p.stopCh:
			return
		case <-tick.C:
		}
		ctx := context.Background()
		p.expireTxns()
		p.applyMu.Lock()
		walBytes, dirty := p.walBytes, p.dirty
		p.applyMu.Unlock()
		if walBytes >= p.cfg.CheckpointBytes || (dirty && time.Since(lastCkpt) >= p.cfg.CheckpointInterval) {
			if err := p.Checkpoint(ctx); err != nil {
				p.log.Warn("checkpoint failed", "err", err)
			} else {
				lastCkpt = time.Now()
			}
		}
		if vis := p.visibleTS.Load(); vis != gcAtTS && time.Since(lastGC) >= p.cfg.MVCCGCInterval {
			if _, err := p.gcVersions(); err != nil {
				p.log.Warn("mvcc gc failed", "err", err)
			}
			lastGC, gcAtTS = time.Now(), vis
		}
		p.collectGarbage(ctx)
	}
}

// expireTxns aborts transactions whose lease ran out (a dead query engine)
// and forgets committed ids older than the window.
func (p *Partition) expireTxns() {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	for id, t := range p.txns {
		if !t.committing && now.Sub(t.lastSeen) > p.cfg.TxnLease {
			p.log.Info("transaction lease expired", "txn", id)
			p.abortTxnLocked(t)
		}
	}
	cutoff := now.Add(-p.cfg.TxnIDWindow)
	for id, c := range p.recent {
		if c.wall.Before(cutoff) {
			delete(p.recent, id)
		}
	}
}
