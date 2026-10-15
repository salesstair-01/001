// Package directory is the always-on directory service (slice 6): it maps
// each partition to its current owner and epoch, tracks KV nodes by their
// heartbeats, and wakes a suspended partition on demand by asking a node to
// take it over, starting a new KV process when none is available.
//
// The partition table lives in SQLite; node liveness is in memory and is
// rebuilt from heartbeats after a restart.
package directory

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"github.com/salesstair-01/001/internal/kv"
	"github.com/salesstair-01/001/internal/kvrpc"
)

// Spawner starts a KV process that registers itself as nodeID.
type Spawner func(ctx context.Context, nodeID string) (exited <-chan struct{}, err error)

// Config configures a Directory.
type Config struct {
	DBPath  string        // SQLite file (":memory:" for tests)
	NodeTTL time.Duration // a node without a heartbeat for this long is dead (10 s)
	// Spawn starts KV processes on demand; nil means only registered nodes are used.
	Spawn Spawner
	// DedicatedNodes gives every woken partition its own KV process, so the
	// process can exit when its tenant goes idle. Requires Spawn.
	DedicatedNodes bool
	// Dial connects to a KV node.
	Dial   func(addr string) (*kvrpc.Client, error)
	Logger *slog.Logger
}

type node struct {
	id, addr string
	lastSeen time.Time
	exiting  bool
	parts    map[uint64]uint64 // partition -> epoch, as last reported
}

// Directory is the service state.
type Directory struct {
	cfg Config
	db  *sql.DB
	log *slog.Logger

	mu        sync.Mutex
	nodes     map[string]*node
	waking    map[uint64]chan struct{}
	waitingOn map[string]chan struct{} // spawned node id -> closed on registration
	clients   map[string]*kvrpc.Client
	seq       int
}

// Open opens the directory and its SQLite table.
func Open(cfg Config) (*Directory, error) {
	if cfg.NodeTTL == 0 {
		cfg.NodeTTL = 10 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	db, err := sql.Open("sqlite", cfg.DBPath)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS partitions (
		id INTEGER PRIMARY KEY,
		node TEXT NOT NULL DEFAULT '',
		addr TEXT NOT NULL DEFAULT '',
		epoch INTEGER NOT NULL DEFAULT 0,
		updated INTEGER NOT NULL DEFAULT 0)`); err != nil {
		db.Close()
		return nil, err
	}
	return &Directory{
		cfg: cfg, db: db, log: cfg.Logger,
		nodes: map[string]*node{}, waking: map[uint64]chan struct{}{},
		waitingOn: map[string]chan struct{}{}, clients: map[string]*kvrpc.Client{},
	}, nil
}

// Close closes the database.
func (d *Directory) Close() error { return d.db.Close() }

// Route is where a partition is served.
type Route struct {
	Node  string
	Addr  string
	Epoch uint64
}

func (d *Directory) row(pid uint64) (Route, error) {
	var r Route
	err := d.db.QueryRow(`SELECT node, addr, epoch FROM partitions WHERE id = ?`, pid).Scan(&r.Node, &r.Addr, &r.Epoch)
	if errors.Is(err, sql.ErrNoRows) {
		return Route{}, nil
	}
	return r, err
}

func (d *Directory) setRow(pid uint64, r Route) error {
	_, err := d.db.Exec(`INSERT INTO partitions (id, node, addr, epoch, updated) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET node = excluded.node, addr = excluded.addr, epoch = excluded.epoch, updated = excluded.updated`,
		pid, r.Node, r.Addr, r.Epoch, time.Now().Unix())
	return err
}

// Partitions lists every known partition and its route (for tools and tests).
func (d *Directory) Partitions() (map[uint64]Route, error) {
	rows, err := d.db.Query(`SELECT id, node, addr, epoch FROM partitions`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[uint64]Route{}
	for rows.Next() {
		var id uint64
		var r Route
		if err := rows.Scan(&id, &r.Node, &r.Addr, &r.Epoch); err != nil {
			return nil, err
		}
		out[id] = r
	}
	return out, rows.Err()
}

func (d *Directory) liveLocked(id string) *node {
	n := d.nodes[id]
	if n == nil || n.exiting || time.Since(n.lastSeen) > d.cfg.NodeTTL {
		return nil
	}
	return n
}

// RegisterNode records a KV node and its address.
func (d *Directory) RegisterNode(id, addr string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := d.nodes[id]
	if n == nil || n.addr != addr {
		n = &node{id: id, addr: addr, parts: map[uint64]uint64{}}
		d.nodes[id] = n
		d.log.Info("node registered", "node", id, "addr", addr)
	}
	n.lastSeen = time.Now()
	if ch, ok := d.waitingOn[id]; ok {
		close(ch)
		delete(d.waitingOn, id)
	}
}

// Heartbeat records a node's liveness and the partitions it serves. A node
// reporting a newer epoch than the table has (after a directory restart, or a
// takeover started by hand) becomes the partition's route.
func (d *Directory) Heartbeat(id, addr string, parts []kv.PartitionEpoch) error {
	d.RegisterNode(id, addr)
	d.mu.Lock()
	n := d.nodes[id]
	n.parts = map[uint64]uint64{}
	for _, p := range parts {
		n.parts[p.Partition] = p.Epoch
	}
	d.mu.Unlock()
	for _, p := range parts {
		r, err := d.row(p.Partition)
		if err != nil {
			return err
		}
		if p.Epoch > r.Epoch || (r.Node == "" && p.Epoch >= r.Epoch) {
			if err := d.setRow(p.Partition, Route{Node: id, Addr: addr, Epoch: p.Epoch}); err != nil {
				return err
			}
		}
	}
	return nil
}

// Release records that a node suspended a partition. exiting marks the node
// as shutting down, so no partition is placed on it again.
func (d *Directory) Release(id string, pid, epoch uint64, exiting bool) error {
	d.mu.Lock()
	if n := d.nodes[id]; n != nil {
		delete(n.parts, pid)
		n.exiting = n.exiting || exiting
	}
	d.mu.Unlock()
	r, err := d.row(pid)
	if err != nil {
		return err
	}
	if r.Node == id && r.Epoch == epoch {
		d.log.Info("partition released", "partition", pid, "node", id, "epoch", epoch)
		return d.setRow(pid, Route{Epoch: epoch})
	}
	return nil
}

// NodeExited marks a node dead at once (its process was seen to exit).
func (d *Directory) NodeExited(id string) {
	d.mu.Lock()
	delete(d.nodes, id)
	d.mu.Unlock()
}

func (d *Directory) client(addr string) (*kvrpc.Client, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if c, ok := d.clients[addr]; ok {
		return c, nil
	}
	c, err := d.cfg.Dial(addr)
	if err != nil {
		return nil, err
	}
	d.clients[addr] = c
	return c, nil
}

// Resolve returns the route of a partition, waking it if no live node serves
// it. stale is the epoch a caller found no longer served (or unreachable);
// a route at that epoch is checked with its node before it is returned again.
func (d *Directory) Resolve(ctx context.Context, pid, stale uint64) (Route, error) {
	waitUntil := time.Now().Add(5 * time.Second)
	for attempt := 0; ; attempt++ {
		r, err := d.row(pid)
		if err != nil {
			return Route{}, err
		}
		d.mu.Lock()
		live := r.Node != "" && d.liveLocked(r.Node) != nil
		d.mu.Unlock()
		if live && (stale == 0 || r.Epoch != stale) {
			return r, nil
		}
		if live { // the caller says this epoch is gone: ask the node
			epoch, err := d.open(ctx, r.Addr, pid)
			if err == nil && epoch != stale {
				r.Epoch = epoch
				return r, d.setRow(pid, r)
			}
			if err == nil && time.Now().Before(waitUntil) {
				// The node still serves the epoch the caller was refused: it is
				// suspending. Wait for its release rather than start a second
				// owner that would fence it mid-checkpoint.
				select {
				case <-time.After(50 * time.Millisecond):
				case <-ctx.Done():
					return Route{}, ctx.Err()
				}
				attempt--
				continue
			}
			d.log.Info("route is stale", "partition", pid, "node", r.Node, "epoch", r.Epoch)
			d.NodeExitedIfUnreachable(ctx, r)
			if err := d.setRow(pid, Route{Epoch: r.Epoch}); err != nil {
				return Route{}, err
			}
		}
		r, err = d.wake(ctx, pid)
		if err == nil || attempt >= 3 || ctx.Err() != nil {
			return r, err
		}
		d.log.Warn("wake failed; retrying", "partition", pid, "err", err)
	}
}

// NodeExitedIfUnreachable forgets a node that no longer answers.
func (d *Directory) NodeExitedIfUnreachable(ctx context.Context, r Route) {
	c, err := d.client(r.Addr)
	if err == nil {
		pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, err = c.Ping(pctx)
		cancel()
	}
	if err != nil {
		d.log.Info("node unreachable", "node", r.Node, "err", err)
		d.NodeExited(r.Node)
	}
}

func (d *Directory) open(ctx context.Context, addr string, pid uint64) (uint64, error) {
	c, err := d.client(addr)
	if err != nil {
		return 0, err
	}
	return c.Open(ctx, pid)
}

// wake places a partition on a node and has it take the partition over. One
// wake per partition runs at a time; concurrent callers share it.
func (d *Directory) wake(ctx context.Context, pid uint64) (Route, error) {
	d.mu.Lock()
	if ch, ok := d.waking[pid]; ok {
		d.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return Route{}, ctx.Err()
		}
		r, err := d.row(pid)
		if err == nil && r.Node == "" {
			err = fmt.Errorf("directory: wake of partition %d failed", pid)
		}
		return r, err
	}
	ch := make(chan struct{})
	d.waking[pid] = ch
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		delete(d.waking, pid)
		d.mu.Unlock()
		close(ch)
	}()

	start := time.Now()
	n, err := d.place(ctx)
	if err != nil {
		return Route{}, err
	}
	epoch, err := d.open(ctx, n.addr, pid)
	if err != nil {
		d.NodeExitedIfUnreachable(ctx, Route{Node: n.id, Addr: n.addr})
		return Route{}, fmt.Errorf("directory: node %s could not take partition %d over: %w", n.id, pid, err)
	}
	r := Route{Node: n.id, Addr: n.addr, Epoch: epoch}
	if err := d.setRow(pid, r); err != nil {
		return Route{}, err
	}
	d.mu.Lock()
	n.parts[pid] = epoch
	d.mu.Unlock()
	d.log.Info("woke partition", "partition", pid, "node", n.id, "epoch", epoch, "took", time.Since(start).Round(time.Millisecond))
	return r, nil
}

// place picks a node for a partition: the least loaded live node, or a newly
// spawned one when there is none or every partition gets its own process.
func (d *Directory) place(ctx context.Context) (*node, error) {
	d.mu.Lock()
	if !(d.cfg.DedicatedNodes && d.cfg.Spawn != nil) {
		var live []*node
		for id := range d.nodes {
			if n := d.liveLocked(id); n != nil {
				live = append(live, n)
			}
		}
		if len(live) > 0 {
			sort.Slice(live, func(i, j int) bool {
				if len(live[i].parts) != len(live[j].parts) {
					return len(live[i].parts) < len(live[j].parts)
				}
				return live[i].id < live[j].id
			})
			d.mu.Unlock()
			return live[0], nil
		}
	}
	if d.cfg.Spawn == nil {
		d.mu.Unlock()
		return nil, errors.New("directory: no live KV node and no way to start one")
	}
	d.seq++
	id := fmt.Sprintf("kv-%d-%d", time.Now().UnixNano(), d.seq)
	registered := make(chan struct{})
	d.waitingOn[id] = registered
	d.mu.Unlock()

	exited, err := d.cfg.Spawn(ctx, id)
	if err != nil {
		d.mu.Lock()
		delete(d.waitingOn, id)
		d.mu.Unlock()
		return nil, fmt.Errorf("directory: start KV process: %w", err)
	}
	go func() {
		<-exited
		d.NodeExited(id)
	}()
	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()
	select {
	case <-registered:
	case <-exited:
		return nil, fmt.Errorf("directory: KV process %s exited before registering", id)
	case <-timeout.C:
		return nil, fmt.Errorf("directory: KV process %s did not register", id)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.nodes[id], nil
}
