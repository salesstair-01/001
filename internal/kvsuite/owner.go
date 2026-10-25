// Package kvsuite holds the KV owner's semantic test suites (the
// metamorphic test, transactions and locks), run over gRPC against a kvd
// binary: the contract between the query engine's client (internal/kvrpc)
// and the owner (kvd-rs/, whose own tests cover it from the inside).
//
//	go test ./internal/kvsuite                     # kvd-rs on a local store
//	MINIO_ENDPOINT=... go test ./internal/kvsuite  # kvd-rs on an S3 store
//	KVSUITE_KVD=<binary> go test ./internal/kvsuite  # another kvd binary
package kvsuite

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/salesstair-01/001/internal/kv"
	"github.com/salesstair-01/001/internal/kvrpc"
)

// Owner is the API of a partition's owner: *kv.Partition, or a kvd over gRPC.
type Owner interface {
	Epoch() uint64
	Begin(epoch uint64, id kv.TxnID, o kv.BeginOptions) error
	Get(epoch uint64, id kv.TxnID, keys [][]byte, o kv.ReadOptions) ([][]byte, error)
	Scan(epoch uint64, id kv.TxnID, start, end []byte, limit int, o kv.ReadOptions) ([]kv.KV, []byte, error)
	Mutate(epoch uint64, id kv.TxnID, stmt uint64, muts []kv.Mutation, wait time.Duration) error
	RollbackStatement(epoch uint64, id kv.TxnID, stmt uint64) error
	CommitGuarded(epoch uint64, id kv.TxnID, guards []kv.Guard) (uint64, error)
	Rollback(epoch uint64, id kv.TxnID) error
	Heartbeat(epoch uint64, ids []kv.TxnID) error
	TxnStatus(epoch uint64, id kv.TxnID) (kv.TxnState, uint64, error)
	AutoIncrement(epoch uint64, id kv.TxnID, table uint32, count, atLeast uint64) (uint64, error)
}

// Instance is one partition under test and the owner serving it.
type Instance interface {
	Pid() uint64
	Owner() Owner
	// Restart ends the current owner (open transactions are lost) and takes
	// the partition over again.
	Restart(t testing.TB)
	// Maintain runs storage work that must not change any result
	// ("checkpoint" or "gc"); owners without it do nothing.
	Maintain(t testing.TB, what string) error
}

// Target creates a fresh partition with an owner serving it, cleaned up
// with t.
type Target func(t testing.TB) Instance

// ---- an owner over gRPC -------------------------------------------------------

type rpcOwner struct {
	c     *kvrpc.Client
	pid   uint64
	epoch uint64
}

func (o *rpcOwner) h(epoch uint64) kvrpc.Header { return kvrpc.Header{Partition: o.pid, Epoch: epoch} }

func ropts(r kv.ReadOptions) kvrpc.ReadOpts {
	return kvrpc.ReadOpts{Stmt: r.Stmt, IncludeStmt: r.IncludeStmt, Lock: r.Lock, LockWaitMs: r.LockWait.Milliseconds()}
}

func (o *rpcOwner) Epoch() uint64 { return o.epoch }

func (o *rpcOwner) Begin(epoch uint64, id kv.TxnID, b kv.BeginOptions) error {
	return o.c.Begin(context.Background(), &kvrpc.BeginReq{H: o.h(epoch), Txn: id, Isolation: b.Isolation, ConsistentSnapshot: b.ConsistentSnapshot})
}

func (o *rpcOwner) Get(epoch uint64, id kv.TxnID, keys [][]byte, r kv.ReadOptions) ([][]byte, error) {
	return o.c.Get(context.Background(), &kvrpc.GetReq{H: o.h(epoch), Txn: id, Keys: keys, Opts: ropts(r)})
}

func (o *rpcOwner) Scan(epoch uint64, id kv.TxnID, start, end []byte, limit int, r kv.ReadOptions) ([]kv.KV, []byte, error) {
	resp, err := o.c.Scan(context.Background(), &kvrpc.ScanReq{H: o.h(epoch), Txn: id, Start: start, End: end, Limit: limit, Opts: ropts(r)})
	if err != nil {
		return nil, nil, err
	}
	rows := make([]kv.KV, len(resp.Keys))
	for i := range resp.Keys {
		rows[i] = kv.KV{Key: resp.Keys[i], Value: resp.Values[i]}
		if rows[i].Value == nil {
			rows[i].Value = []byte{}
		}
	}
	var resume []byte
	if len(resp.Resume) > 0 {
		resume = resp.Resume
	}
	return rows, resume, nil
}

func (o *rpcOwner) Mutate(epoch uint64, id kv.TxnID, stmt uint64, muts []kv.Mutation, wait time.Duration) error {
	return o.c.Mutate(context.Background(), &kvrpc.MutateReq{H: o.h(epoch), Txn: id, Stmt: stmt, Mutations: muts, LockWaitMs: wait.Milliseconds()})
}

func (o *rpcOwner) RollbackStatement(epoch uint64, id kv.TxnID, stmt uint64) error {
	return o.c.RollbackStatement(context.Background(), &kvrpc.StmtReq{H: o.h(epoch), Txn: id, Stmt: stmt})
}

func (o *rpcOwner) CommitGuarded(epoch uint64, id kv.TxnID, guards []kv.Guard) (uint64, error) {
	return o.c.Commit(context.Background(), &kvrpc.TxnReq{H: o.h(epoch), Txn: id, Guards: guards})
}

func (o *rpcOwner) Rollback(epoch uint64, id kv.TxnID) error {
	return o.c.Rollback(context.Background(), &kvrpc.TxnReq{H: o.h(epoch), Txn: id})
}

func (o *rpcOwner) Heartbeat(epoch uint64, ids []kv.TxnID) error {
	return o.c.Heartbeat(context.Background(), &kvrpc.HeartbeatReq{H: o.h(epoch), Txns: ids})
}

func (o *rpcOwner) TxnStatus(epoch uint64, id kv.TxnID) (kv.TxnState, uint64, error) {
	resp, err := o.c.TxnStatus(context.Background(), &kvrpc.TxnReq{H: o.h(epoch), Txn: id})
	if err != nil {
		return kv.TxnUnknown, 0, err
	}
	return resp.State, resp.CommitTS, nil
}

func (o *rpcOwner) AutoIncrement(epoch uint64, id kv.TxnID, table uint32, count, atLeast uint64) (uint64, error) {
	return o.c.AutoIncrement(context.Background(), &kvrpc.AutoIncReq{H: o.h(epoch), Txn: id, Table: table, Count: count, AtLeast: atLeast})
}

// ---- a kvd process ------------------------------------------------------------

var nextPid atomic.Uint64

func init() { nextPid.Store(uint64(time.Now().UnixNano()%(1<<40)) + 1<<20) }

// ProcessTarget runs each instance on its own kvd process started from bin,
// with its data in storeDir (a local store), or in the object store named
// by MINIO_* when storeDir is empty. Restart kills it with SIGKILL and
// starts another on the same address.
func ProcessTarget(bin, storeDir string) Target {
	return func(t testing.TB) Instance {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := l.Addr().String()
		l.Close()
		c, err := kvrpc.Dial(addr)
		if err != nil {
			t.Fatal(err)
		}
		p := &process{bin: bin, addr: addr, dir: t.TempDir(), store: storeDir, pid: nextPid.Add(1), c: c}
		p.start(t)
		t.Cleanup(func() {
			if p.cmd != nil {
				_ = p.cmd.Process.Signal(syscall.SIGTERM)
				_ = p.cmd.Wait()
			}
		})
		return p
	}
}

type process struct {
	bin, addr, dir string
	store          string // local store directory; empty = MINIO_*
	pid            uint64
	n              int
	cmd            *exec.Cmd
	c              *kvrpc.Client
	owner          *rpcOwner
}

func (p *process) Pid() uint64                       { return p.pid }
func (p *process) Owner() Owner                      { return p.owner }
func (p *process) Maintain(testing.TB, string) error { return nil }

func (p *process) start(t testing.TB) {
	t.Helper()
	p.n++
	log, err := os.Create(filepath.Join(p.dir, fmt.Sprintf("kvd%d.log", p.n)))
	if err != nil {
		t.Fatal(err)
	}
	args := []string{"-listen", p.addr, "-owner", fmt.Sprintf("suite-%d-%d", p.pid, p.n)}
	if p.store != "" {
		args = append(args, "-store-dir", p.store)
	}
	p.cmd = exec.Command(p.bin, args...)
	p.cmd.Stdout, p.cmd.Stderr = log, log
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		epoch, err := p.c.Open(ctx, p.pid)
		cancel()
		if err == nil {
			p.owner = &rpcOwner{c: p.c, pid: p.pid, epoch: epoch}
			return
		}
		if time.Now().After(deadline) {
			b, _ := os.ReadFile(log.Name())
			t.Fatalf("kvd did not open partition %d: %v\n%s", p.pid, err, b)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (p *process) Restart(t testing.TB) {
	t.Helper()
	_ = p.cmd.Process.Kill()
	_ = p.cmd.Wait()
	p.start(t)
}
