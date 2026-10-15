package directory

import (
	"context"
	"sync"

	"github.com/salesstair-01/001/internal/kvrpc"
)

// Router resolves partitions through the directory and keeps one client per
// KV node. It implements sqlengine.Router.
type Router struct {
	Dir  *Client
	Dial func(addr string) (*kvrpc.Client, error)

	mu      sync.Mutex
	clients map[string]*kvrpc.Client
}

func (r *Router) Resolve(ctx context.Context, partition, stale uint64) (*kvrpc.Client, uint64, error) {
	route, err := r.Dir.Resolve(ctx, partition, stale)
	if err != nil {
		return nil, 0, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.clients == nil {
		r.clients = map[string]*kvrpc.Client{}
	}
	c, ok := r.clients[route.Addr]
	if !ok {
		if c, err = r.Dial(route.Addr); err != nil {
			return nil, 0, err
		}
		r.clients[route.Addr] = c
	}
	return c, route.Epoch, nil
}
