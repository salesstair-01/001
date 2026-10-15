package kv

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/cockroachdb/pebble"

	"github.com/salesstair-01/001/internal/encoding"
	"github.com/salesstair-01/001/internal/objstore"
	"github.com/salesstair-01/001/internal/wal"
)

type commitResult struct {
	ts  uint64
	err error
}

type commitReq struct {
	txn     *Txn
	rec     wal.Record
	size    int
	arrived time.Time
	done    chan commitResult
}

// Commit commits the transaction (D4): check it still holds its locks (an
// aborted transaction no longer exists), assign commit_ts = max + 1, join the
// open group, and return once the group's WAL segment is durable in object
// storage and applied. A transaction without writes commits at 0.
func (p *Partition) Commit(epoch uint64, id TxnID) (uint64, error) {
	if err := p.checkEpoch(epoch); err != nil {
		return 0, err
	}
	p.mu.Lock()
	if c, ok := p.recent[id]; ok { // a retried Commit returns the original commit_ts
		p.mu.Unlock()
		return c.ts, nil
	}
	t, err := p.txnLocked(id)
	if err != nil {
		p.mu.Unlock()
		return 0, err
	}
	if t.committing {
		p.mu.Unlock()
		return 0, newErr(KindInternal, "transaction %s is already committing", id)
	}
	keys, writes := t.merged(0)
	if len(keys) == 0 {
		p.abortTxnLocked(t) // nothing to write: just free the locks
		p.mu.Unlock()
		return 0, nil
	}
	// The transaction holds an exclusive lock on every key it wrote, so no
	// other commit can have touched them since; re-check insert-only keys anyway.
	for _, k := range keys {
		in := writes[k]
		if !in.insertOnly {
			continue
		}
		latest, _, err := p.latestLocked([]byte(k))
		if err != nil {
			p.mu.Unlock()
			return 0, err
		}
		if latest != nil && !encoding.IsDelete(latest) {
			p.abortTxnLocked(t)
			p.mu.Unlock()
			return 0, &Error{Kind: KindDuplicateKey, Msg: "duplicate key at commit", Key: []byte(k)}
		}
	}
	ts := p.maxCommitTS + 1
	p.maxCommitTS = ts
	rec := wal.Record{TxnID: id, CommitTS: ts, WallMicros: time.Now().UnixMicro()}
	for _, k := range keys {
		v := writes[k].value
		rec.Writes = append(rec.Writes, wal.Write{Key: []byte(k), Value: v})
		p.pending[k] = pendingWrite{ts: ts, value: v}
	}
	t.committing = true
	req := &commitReq{txn: t, rec: rec, size: rec.Size(), arrived: time.Now(), done: make(chan commitResult, 1)}
	p.queue = append(p.queue, req)
	p.mu.Unlock()
	select {
	case p.queueSignal <- struct{}{}:
	default:
	}
	res := <-req.done
	return res.ts, res.err
}

// flusher is the single goroutine that writes WAL segments, in sequence order.
func (p *Partition) flusher() {
	defer p.wg.Done()
	var lastFlushEnd time.Time
	for {
		select {
		case <-p.queueSignal:
		case <-p.stopCh:
			return
		}
		for {
			p.mu.Lock()
			if len(p.queue) == 0 || p.stopErr != nil {
				p.mu.Unlock()
				break
			}
			first := p.queue[0].arrived
			queued := 0
			for _, r := range p.queue {
				queued += r.size
			}
			p.mu.Unlock()
			// Flush immediately when idle; when busy, wait up to the window
			// after the first record so more commits share the PUT.
			busy := time.Since(lastFlushEnd) < p.cfg.GroupWindow
			if wait := time.Until(first.Add(p.cfg.GroupWindow)); busy && wait > 0 && queued < p.cfg.GroupMaxBytes {
				select {
				case <-time.After(wait):
				case <-p.stopCh:
					return
				}
			}
			p.mu.Lock()
			var batch []*commitReq
			size := 0
			for len(p.queue) > 0 && (len(batch) == 0 || size+p.queue[0].size <= p.cfg.GroupMaxBytes) {
				size += p.queue[0].size
				batch = append(batch, p.queue[0])
				p.queue = p.queue[1:]
			}
			p.mu.Unlock()
			if len(batch) == 0 {
				break
			}
			err := p.flush(batch)
			lastFlushEnd = time.Now()
			if err != nil {
				p.stop(err)
				return
			}
		}
	}
}

// flush writes one commit group as a WAL segment and applies it.
func (p *Partition) flush(batch []*commitReq) error {
	ctx := context.Background()
	seq := p.nextWALSeq
	seg := &wal.Segment{Epoch: p.epoch, Seq: seq}
	for _, r := range batch {
		seg.Records = append(seg.Records, r.rec)
	}
	data := seg.Encode()
	key := walKey(p.id, seq)
	err := putWithRetry(ctx, p.store, key, data, p.cfg.CommitRetryTimeout)
	if errors.Is(err, objstore.ErrPreconditionFailed) {
		// GET the object and compare: if it is ours, a timed-out first attempt
		// landed. If not, a newer owner fenced us.
		existing, gerr := getWithRetry(ctx, p.store, key, p.cfg.CommitRetryTimeout)
		switch {
		case gerr == nil && bytes.Equal(existing, data):
			err = nil
		case gerr == nil:
			err = errFenced
		default:
			err = errCommitAbandoned
		}
	} else if err != nil {
		err = errCommitAbandoned
	}
	if err != nil {
		for _, r := range batch {
			r.done <- commitResult{err: err}
		}
		return err
	}

	// Durable: apply to Pebble, make the writes visible, acknowledge.
	p.applyMu.Lock()
	pb := p.db.NewBatch()
	for _, r := range batch {
		for _, w := range r.rec.Writes {
			_ = pb.Set(encoding.MVCCKey(w.Key, r.rec.CommitTS), w.Value, nil)
		}
	}
	if aerr := pb.Commit(pebble.NoSync); aerr != nil {
		p.applyMu.Unlock()
		// The commit is durable but this owner can no longer serve it; a new
		// owner will replay it. Report the outcome as unknown.
		for _, r := range batch {
			r.done <- commitResult{err: errCommitAbandoned}
		}
		return aerr
	}
	p.lastAppliedSeq = seq
	p.nextWALSeq = seq + 1
	p.walBytes += int64(len(data))
	p.dirty = true
	p.visibleTS.Store(batch[len(batch)-1].rec.CommitTS)
	p.applyMu.Unlock()

	p.mu.Lock()
	now := time.Now()
	for _, r := range batch {
		for _, w := range r.rec.Writes {
			if pw, ok := p.pending[string(w.Key)]; ok && pw.ts == r.rec.CommitTS {
				delete(p.pending, string(w.Key))
			}
		}
		p.recent[r.rec.TxnID] = committedTxn{ts: r.rec.CommitTS, wall: now}
		delete(p.txns, r.rec.TxnID)
	}
	p.mu.Unlock()
	// Release locks only once the writes are visible (D4).
	for _, r := range batch {
		p.locks.ReleaseAll(r.rec.TxnID)
	}
	for _, r := range batch {
		r.done <- commitResult{ts: r.rec.CommitTS}
	}
	return nil
}

func getWithRetry(ctx context.Context, s objstore.Store, key string, timeout time.Duration) ([]byte, error) {
	deadline := time.Now().Add(timeout)
	backoff := 10 * time.Millisecond
	for {
		b, err := s.Get(ctx, key)
		if err == nil || errors.Is(err, objstore.ErrNotFound) || time.Now().After(deadline) {
			return b, err
		}
		time.Sleep(backoff)
		if backoff < time.Second {
			backoff *= 2
		}
	}
}
