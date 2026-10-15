package directory

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/salesstair-01/001/internal/kv"
)

// Agent connects a KV node to the directory: it registers the node,
// heartbeats every 2 s with the partitions it serves, releases partitions the
// node suspends, and, for a dedicated per-tenant process, asks the process to
// exit once it serves nothing.
type Agent struct {
	Dir          *Client
	NodeID, Addr string
	ExitWhenIdle bool
	Exit         func() // called once when an idle dedicated node should exit
	Logger       *slog.Logger

	mu   sync.Mutex
	node *kv.Node
	once sync.Once
}

// Attach sets the node; call it before Run.
func (a *Agent) Attach(n *kv.Node) {
	a.mu.Lock()
	a.node = n
	a.mu.Unlock()
}

func (a *Agent) log() *slog.Logger {
	if a.Logger != nil {
		return a.Logger
	}
	return slog.Default()
}

// Run registers and heartbeats until ctx ends. A dedicated node that has not
// been given a partition within idleExit exits too.
func (a *Agent) Run(ctx context.Context, idleExit time.Duration) {
	for {
		rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := a.Dir.RegisterNode(rctx, a.NodeID, a.Addr)
		cancel()
		if err == nil || ctx.Err() != nil {
			break
		}
		a.log().Warn("register with directory", "err", err)
		time.Sleep(time.Second)
	}
	started := time.Now()
	served := false
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		a.mu.Lock()
		n := a.node
		a.mu.Unlock()
		var parts []kv.PartitionEpoch
		if n != nil {
			parts = n.Partitions()
		}
		served = served || len(parts) > 0
		hctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		if err := a.Dir.Heartbeat(hctx, a.NodeID, a.Addr, parts); err != nil {
			a.log().Warn("directory heartbeat", "err", err)
		}
		cancel()
		if a.ExitWhenIdle && !served && idleExit > 0 && time.Since(started) > idleExit {
			a.exit()
		}
	}
}

// OnSuspend is the node's kv.Config.OnSuspend.
func (a *Agent) OnSuspend(partition, epoch uint64) {
	a.mu.Lock()
	n := a.node
	a.mu.Unlock()
	exiting := a.ExitWhenIdle && n != nil && len(n.Partitions()) == 0
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.Dir.Release(ctx, a.NodeID, partition, epoch, exiting); err != nil {
		a.log().Warn("release partition in directory", "partition", partition, "err", err)
	}
}

// OnEmpty is the node's kv.Config.OnEmpty.
func (a *Agent) OnEmpty() {
	if a.ExitWhenIdle {
		a.exit()
	}
}

func (a *Agent) exit() {
	a.once.Do(func() {
		a.log().Info("no partitions left; exiting", "node", a.NodeID)
		if a.Exit != nil {
			a.Exit()
		}
	})
}
