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
	stop       chan struct{}
	stopOnce   sync.Once
}

// NewNode creates a KV node. With Config.IdleSuspend set, it suspends idle
// partitions in the background.
func NewNode(cfg Config) *Node {
	cfg = cfg.withDefaults()
	cfg.res = newNodeResources(&cfg)
	if cfg.OwnerID == "" {
		cfg.OwnerID = "kv-" + newID()[:12]
	}
	n := &Node{cfg: cfg, partitions: map[uint64]*Partition{}, opening: map[uint64]chan struct{}{}, stop: make(chan struct{})}
	if cfg.IdleSuspend > 0 {
		go n.idleLoop()
	}
	return n
}

// PartitionEpoch is a served partition and its epoch.
type PartitionEpoch struct {
	Partition, Epoch uint64
}

// Partitions lists the partitions this node serves.
func (n *Node) Partitions() []PartitionEpoch {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]PartitionEpoch, 0, len(n.partitions))
	for id, p := range n.partitions {
		if p.Stopped() == nil {
			out = append(out, PartitionEpoch{id, p.epoch})
		}
	}
	return out
}

// idleLoop suspends partitions with no open transactions and no requests for
// IdleSuspend: checkpoint, release, and (via OnEmpty) let the process exit.
func (n *Node) idleLoop() {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	hadPartitions := false
	for {
		select {
		case <-n.stop:
			return
		case <-tick.C:
		}
		n.mu.Lock()
		ps := make([]*Partition, 0, len(n.partitions))
		var stopped []*Partition
		for id, p := range n.partitions {
			if p.Stopped() != nil { // fenced by a newer owner, or failed
				delete(n.partitions, id)
				stopped = append(stopped, p)
				hadPartitions = true
				continue
			}
			ps = append(ps, p)
		}
		busy := len(n.opening) > 0
		n.mu.Unlock()
		for _, p := range stopped {
			go p.Close()
		}
		for _, p := range ps {
			if p.IdleFor() < n.cfg.IdleSuspend {
				continue
			}
			ok, err := p.TrySuspend(context.Background(), n.cfg.IdleSuspend)
			if err != nil {
				n.cfg.Logger.Warn("idle suspend failed", "partition", p.id, "err", err)
				continue
			}
			if !ok {
				continue
			}
			n.mu.Lock()
			if n.partitions[p.id] == p {
				delete(n.partitions, p.id)
			}
			n.mu.Unlock()
			hadPartitions = true
			n.cfg.Logger.Info("suspended idle partition", "partition", p.id, "epoch", p.epoch)
			if n.cfg.OnSuspend != nil {
				n.cfg.OnSuspend(p.id, p.epoch)
			}
		}
		n.mu.Lock()
		empty := len(n.partitions) == 0 && len(n.opening) == 0
		if len(n.partitions) > 0 || busy {
			hadPartitions = true
		}
		n.mu.Unlock()
		if empty && hadPartitions && n.cfg.OnEmpty != nil {
			hadPartitions = false
			n.cfg.OnEmpty()
		}
	}
}

// Get returns a served partition, or nil.
func (n *Node) Get(pid uint64) *Partition {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.partitions[pid]
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
	if n.cfg.OnSuspend != nil {
		n.cfg.OnSuspend(pid, p.epoch)
	}
	return nil
}

// Close stops serving every partition without a checkpoint.
func (n *Node) Close() error {
	n.stopOnce.Do(func() { close(n.stop) })
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
	n.cfg.res.close()
	return errors.Join(errs...)
}
