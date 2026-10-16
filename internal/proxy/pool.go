package proxy

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sort"
	"sync"
	"time"
)

// Engine is one generic query-engine process the proxy can send any tenant's
// session to.
type Engine struct {
	Addr string
	// Stop ends the process; nil for engines the proxy did not start.
	Stop func()
	// Exited is closed when the process ends (nil = never).
	Exited <-chan struct{}

	conns    int
	lastUsed time.Time
	dead     bool
}

// Spawner starts a generic query engine and returns once it accepts connections.
type Spawner func(ctx context.Context) (*Engine, error)

// PoolConfig configures the query-engine pool.
type PoolConfig struct {
	Static []string // engines that always run (not started or stopped by the proxy)
	Spawn  Spawner  // starts engines on demand; nil = static only
	// Warm is how many spawned engines to keep running even with no
	// sessions, so a cold tenant pays only for its KV wake (0 = every compute
	// layer can reach zero). While Warm > 0 the pool also starts an engine
	// ahead of need once every engine is at 80% of MaxSessions.
	Warm int
	// MaxSessions is how many backend connections one engine takes before
	// the pool starts another (default 256). Engines serve any tenant, so
	// many tenants share one.
	MaxSessions int
	// Idle stops a spawned engine with no sessions for this long, beyond Warm.
	Idle   time.Duration
	Logger *slog.Logger
}

// Pool hands out query engines and scales spawned ones to zero.
type Pool struct {
	cfg PoolConfig

	mu       sync.Mutex
	engines  []*Engine
	spawning int
	spawned  chan struct{} // closed when the spawns in flight end; nil = none
	wake     chan struct{}
	stop     chan struct{}
}

// NewPool creates a pool and starts its warm engines and reaper.
func NewPool(cfg PoolConfig) *Pool {
	if cfg.Idle == 0 {
		cfg.Idle = 5 * time.Minute
	}
	if cfg.MaxSessions == 0 {
		cfg.MaxSessions = 256
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	p := &Pool{cfg: cfg, wake: make(chan struct{}, 1), stop: make(chan struct{})}
	for _, a := range cfg.Static {
		p.engines = append(p.engines, &Engine{Addr: a, lastUsed: time.Now()})
	}
	go p.loop()
	p.kick()
	return p
}

// Close stops the reaper and every spawned engine.
func (p *Pool) Close() {
	close(p.stop)
	p.mu.Lock()
	es := p.engines
	p.engines = nil
	p.mu.Unlock()
	for _, e := range es {
		if e.Stop != nil {
			e.Stop()
		}
	}
}

func (p *Pool) kick() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// Running returns how many engines are up (spawned and static).
func (p *Pool) Running() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, e := range p.engines {
		if !e.dead {
			n++
		}
	}
	return n
}

// Acquire returns the least-loaded engine with room, starting one if there
// is none. Concurrent callers share an engine being started.
func (p *Pool) Acquire(ctx context.Context) (*Engine, error) {
	for {
		p.mu.Lock()
		var best *Engine
		for _, e := range p.engines {
			if !e.dead && e.conns < p.cfg.MaxSessions && (best == nil || e.conns < best.conns) {
				best = e
			}
		}
		if best != nil {
			best.conns++
			best.lastUsed = time.Now()
			p.mu.Unlock()
			p.kick() // start one ahead if engines are getting full
			return best, nil
		}
		if p.cfg.Spawn == nil {
			p.mu.Unlock()
			return nil, errors.New("proxy: no query engine available")
		}
		if ch := p.spawned; ch != nil { // wait for the engine being started
			p.mu.Unlock()
			select {
			case <-ch:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			continue
		}
		p.beginSpawnLocked()
		p.mu.Unlock()
		if err := p.spawnOne(ctx); err != nil {
			return nil, err
		}
	}
}

// beginSpawnLocked counts a spawn in flight. Requires p.mu.
func (p *Pool) beginSpawnLocked() {
	p.spawning++
	if p.spawned == nil {
		p.spawned = make(chan struct{})
	}
}

// Release returns an engine after a session's backend connection closes.
func (p *Pool) Release(e *Engine) {
	p.mu.Lock()
	if e.conns > 0 {
		e.conns--
	}
	e.lastUsed = time.Now()
	p.mu.Unlock()
}

// Failed marks an engine unreachable.
func (p *Pool) Failed(e *Engine) {
	p.mu.Lock()
	e.dead = true
	p.mu.Unlock()
	if e.Stop != nil {
		e.Stop()
	}
	p.kick()
}

// spawnOne starts an engine; the caller has already counted it in spawning.
func (p *Pool) spawnOne(ctx context.Context) error {
	e, err := p.cfg.Spawn(ctx)
	p.mu.Lock()
	p.spawning--
	if err == nil {
		e.lastUsed = time.Now()
		p.engines = append(p.engines, e)
	}
	if p.spawning == 0 && p.spawned != nil {
		close(p.spawned)
		p.spawned = nil
	}
	p.mu.Unlock()
	if err != nil {
		return err
	}
	p.cfg.Logger.Info("query engine started", "addr", e.Addr)
	if e.Exited != nil {
		go func() {
			<-e.Exited
			p.mu.Lock()
			e.dead = true
			p.mu.Unlock()
		}()
	}
	return nil
}

// loop keeps Warm engines running, starts one ahead when every engine is
// nearly full, and stops engines idle past Idle beyond the Warm ones.
func (p *Pool) loop() {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-tick.C:
		case <-p.wake:
		}
		p.mu.Lock()
		// Busiest and most recently used first, so the longest-idle engines are
		// the ones stopped.
		sort.SliceStable(p.engines, func(i, j int) bool {
			a, b := p.engines[i], p.engines[j]
			if a.conns != b.conns {
				return a.conns > b.conns
			}
			return a.lastUsed.After(b.lastUsed)
		})
		kept := p.engines[:0]
		running, roomy := 0, 0
		var stop []*Engine
		for _, e := range p.engines {
			if e.dead {
				continue
			}
			if e.Stop != nil {
				if e.conns == 0 && running >= p.cfg.Warm && time.Since(e.lastUsed) > p.cfg.Idle {
					stop = append(stop, e)
					continue
				}
				running++
			}
			if e.conns < p.cfg.MaxSessions*8/10 {
				roomy++
			}
			kept = append(kept, e)
		}
		p.engines = kept
		need := 0
		if p.cfg.Spawn != nil && p.spawning == 0 {
			need = p.cfg.Warm - running
			if need < 1 && p.cfg.Warm > 0 && roomy == 0 {
				need = 1
			}
		}
		for i := 0; i < need; i++ {
			p.beginSpawnLocked()
		}
		p.mu.Unlock()
		for _, e := range stop {
			p.cfg.Logger.Info("stopping idle query engine", "addr", e.Addr)
			e.Stop()
		}
		for i := 0; i < need; i++ {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				if err := p.spawnOne(ctx); err != nil {
					p.cfg.Logger.Warn("start warm query engine", "err", err)
				}
			}()
		}
	}
}

// WaitReady dials addr until it accepts connections.
func WaitReady(ctx context.Context, addr string) error {
	for {
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			c.Close()
			return nil
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(20 * time.Millisecond):
		}
	}
}
