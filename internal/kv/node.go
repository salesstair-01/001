package kv

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Node is one KV process. It can own many partitions, each with its own
// Pebble instance and durability layer (D1).
type Node struct {
	cfg Config

	mu         sync.Mutex
	partitions map[uint64]*Partition
	opening    map[uint64]chan struct{}
}

// NewNode creates a KV node.
func NewNode(cfg Config) *Node {
	cfg = cfg.withDefaults()
	if cfg.OwnerID == "" {
		cfg.OwnerID = "kv-" + newID()[:12]
	}
	return &Node{cfg: cfg, partitions: map[uint64]*Partition{}, opening: map[uint64]chan struct{}{}}
}

// OwnerID returns this node's owner id.
func (n *Node) OwnerID() string { return n.cfg.OwnerID }

// Partition returns the served partition pid, or an EpochChanged error.
func (n *Node) Partition(pid uint64) (*Partition, error) {
	n.mu.Lock()
	p, ok := n.partitions[pid]
	n.mu.Unlock()
	if !ok {
		return nil, newErr(KindEpochChanged, "partition %d is not served by this node", pid)
	}
	if err := p.Stopped(); err != nil {
		n.forget(p)
		return nil, err
	}
	return p, nil
}

func (n *Node) forget(p *Partition) {
	n.mu.Lock()
	if n.partitions[p.id] == p {
		delete(n.partitions, p.id)
	}
	n.mu.Unlock()
	go p.Close()
}

// Open returns the partition, taking it over first if this node does not
// serve it. Concurrent opens of one partition share a single takeover.
func (n *Node) Open(ctx context.Context, pid uint64) (*Partition, error) {
	for {
		n.mu.Lock()
		if p, ok := n.partitions[pid]; ok {
			n.mu.Unlock()
			if p.Stopped() == nil {
				return p, nil
			}
			n.forget(p)
			continue
		}
		if ch, ok := n.opening[pid]; ok {
			n.mu.Unlock()
			select {
			case <-ch:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		ch := make(chan struct{})
		n.opening[pid] = ch
		n.mu.Unlock()

		p, err := n.takeover(ctx, pid)
		n.mu.Lock()
		delete(n.opening, pid)
		if err == nil {
			n.partitions[pid] = p
		}
		n.mu.Unlock()
		close(ch)
		return p, err
	}
}

func (n *Node) takeover(ctx context.Context, pid uint64) (*Partition, error) {
	backoff := 50 * time.Millisecond
	for attempt := 0; ; attempt++ {
		p, err := Takeover(ctx, n.cfg, pid)
		if !errors.Is(err, errTakeoverLost) || attempt >= 5 {
			return p, err
		}
		// Someone else won manifest N+1; read the new highest and try again.
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		backoff *= 2
	}
}

// Suspend checkpoints and releases partition pid if it has no open transactions.
func (n *Node) Suspend(ctx context.Context, pid uint64) error {
	n.mu.Lock()
	p, ok := n.partitions[pid]
	n.mu.Unlock()
	if !ok {
		return nil
	}
	if err := p.Suspend(ctx); err != nil {
		return err
	}
	n.mu.Lock()
	if n.partitions[pid] == p {
		delete(n.partitions, pid)
	}
	n.mu.Unlock()
	return nil
}

// Close stops serving every partition without a checkpoint.
func (n *Node) Close() error {
	n.mu.Lock()
	ps := make([]*Partition, 0, len(n.partitions))
	for _, p := range n.partitions {
		ps = append(ps, p)
	}
	n.partitions = map[uint64]*Partition{}
	n.mu.Unlock()
	var errs []error
	for _, p := range ps {
		if err := p.Close(); err != nil {
			errs = append(errs, fmt.Errorf("partition %d: %w", p.id, err))
		}
	}
	return errors.Join(errs...)
}
