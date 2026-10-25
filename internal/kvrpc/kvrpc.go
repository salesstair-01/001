// Package kvrpc is the query engine's client for the KV owner (kvd-rs/),
// over the gRPC API of proto/s2z/kv/kv.proto (D7). Go code uses the plain
// structs below, which codec.go converts to and from the protobuf messages.
// Every request carries the partition id and the epoch the caller expects;
// a mismatch returns EpochChanged.
package kvrpc

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/salesstair-01/001/internal/kv"
)

// Header is carried by every request.
type Header struct {
	Partition uint64
	Epoch     uint64
	// Background requests (statistics, maintenance) don't count as activity,
	// so they don't keep an idle partition awake.
	Background bool
}

type (
	OpenReq  struct{ Partition uint64 }
	OpenResp struct{ Epoch uint64 }

	BeginReq struct {
		H                  Header
		Txn                kv.TxnID
		Isolation          kv.Isolation
		ConsistentSnapshot bool
	}
	Empty struct{}

	// ReadOpts mirror kv.ReadOptions.
	ReadOpts struct {
		Stmt        uint64
		IncludeStmt bool // see the statement's own writes (duplicate-key checks)
		Lock        kv.LockMode
		LockWaitMs  int64
	}

	GetReq struct {
		H    Header
		Txn  kv.TxnID
		Keys [][]byte
		Opts ReadOpts
	}
	GetResp struct {
		Values [][]byte
		Found  []bool // the wire does not keep nil apart from empty
	}

	ScanReq struct {
		H          Header
		Txn        kv.TxnID
		Start, End []byte
		Limit      int
		Opts       ReadOpts
	}
	ScanResp struct {
		Keys, Values [][]byte
		Resume       []byte // where the next page starts; empty at the end
	}

	MutateReq struct {
		H          Header
		Txn        kv.TxnID
		Stmt       uint64
		Mutations  []kv.Mutation
		LockWaitMs int64
	}

	StmtReq struct {
		H    Header
		Txn  kv.TxnID
		Stmt uint64
	}

	TxnReq struct {
		H      Header
		Txn    kv.TxnID
		Guards []kv.Guard // Commit only
	}
	CommitResp struct{ CommitTS uint64 }

	TxnStatusResp struct {
		State    kv.TxnState
		CommitTS uint64
	}

	PingResp struct{ Owner string }

	AutoIncReq struct {
		H       Header
		Txn     kv.TxnID // zero = peek
		Table   uint32
		Count   uint64
		AtLeast uint64
	}
	AutoIncResp struct{ First uint64 }

	HeartbeatReq struct {
		H    Header
		Txns []kv.TxnID
	}
)

func lockWaitOf(o ReadOpts) int64 {
	if o.Lock == kv.LockNone {
		return 0
	}
	if o.LockWaitMs <= 0 {
		return kv.DefaultLockWait.Milliseconds()
	}
	return o.LockWaitMs
}

// ---- codec -------------------------------------------------------------------

// ServerOption makes a grpc.Server use the protobuf codec over the Go
// structs (the directory's server, internal/directory).
func ServerOption() grpc.ServerOption { return grpc.ForceServerCodec(protoCodec{}) }

// DialOptions make a client connection use the protobuf codec and reconnect
// quickly. Calls fail fast when the owner is unreachable: the query engine
// then re-resolves the owner (which wakes or moves the partition) and retries.
func DialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.ForceCodec(protoCodec{})),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.Config{BaseDelay: 50 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: time.Second},
			MinConnectTimeout: time.Second,
		}),
	}
}

// CallTimeout bounds every KV call whose context has no deadline.
var CallTimeout = 30 * time.Second

// ---- errors ------------------------------------------------------------------

const errPrefix = "kv|"

// FromStatus turns a gRPC error back into a *kv.Error where possible.
// Transport failures (owner unreachable) become EpochChanged, as in D7.
func FromStatus(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	if msg := st.Message(); strings.HasPrefix(msg, errPrefix) {
		parts := strings.SplitN(msg[len(errPrefix):], "|", 3)
		if len(parts) == 3 {
			key, _ := hex.DecodeString(parts[1])
			return &kv.Error{Kind: kv.KindFromString(parts[0]), Key: key, Msg: parts[2]}
		}
	}
	switch st.Code() {
	case codes.Unavailable, codes.Canceled, codes.DeadlineExceeded:
		return &kv.Error{Kind: kv.KindEpochChanged, Msg: "owner unreachable: " + st.Message()}
	}
	return fmt.Errorf("kv rpc: %s", st.Message())
}

const serviceName = "s2z.kv.KV"

// ---- client ------------------------------------------------------------------

// Client calls a KV owner.
type Client struct {
	cc *grpc.ClientConn
}

func NewClient(cc *grpc.ClientConn) *Client { return &Client{cc: cc} }

// Close closes the connection.
func (c *Client) Close() error { return c.cc.Close() }

func (c *Client) call(ctx context.Context, method string, req, resp any) error {
	return c.callWait(ctx, method, req, resp, 0)
}

// callWait is call for requests that may wait up to lockWaitMs for locks.
func (c *Client) callWait(ctx context.Context, method string, req, resp any, lockWaitMs int64) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, CallTimeout+time.Duration(lockWaitMs)*time.Millisecond)
		defer cancel()
	}
	return FromStatus(c.cc.Invoke(ctx, "/"+serviceName+"/"+method, req, resp))
}

func (c *Client) Open(ctx context.Context, partition uint64) (uint64, error) {
	var resp OpenResp
	err := c.call(ctx, "Open", &OpenReq{Partition: partition}, &resp)
	return resp.Epoch, err
}

func (c *Client) Begin(ctx context.Context, r *BeginReq) error {
	return c.call(ctx, "Begin", r, &Empty{})
}

func (c *Client) Get(ctx context.Context, r *GetReq) ([][]byte, error) {
	var resp GetResp
	if err := c.callWait(ctx, "Get", r, &resp, lockWaitOf(r.Opts)); err != nil {
		return nil, err
	}
	out := make([][]byte, len(r.Keys))
	for i := range out {
		if i < len(resp.Found) && resp.Found[i] {
			out[i] = resp.Values[i]
			if out[i] == nil {
				out[i] = []byte{}
			}
		}
	}
	return out, nil
}

func (c *Client) Scan(ctx context.Context, r *ScanReq) (*ScanResp, error) {
	var resp ScanResp
	err := c.callWait(ctx, "Scan", r, &resp, lockWaitOf(r.Opts))
	return &resp, err
}

func (c *Client) Mutate(ctx context.Context, r *MutateReq) error {
	wait := r.LockWaitMs
	if wait <= 0 {
		wait = kv.DefaultLockWait.Milliseconds()
	}
	return c.callWait(ctx, "Mutate", r, &Empty{}, wait)
}

func (c *Client) RollbackStatement(ctx context.Context, r *StmtReq) error {
	return c.call(ctx, "RollbackStatement", r, &Empty{})
}

func (c *Client) Commit(ctx context.Context, r *TxnReq) (uint64, error) {
	var resp CommitResp
	err := c.call(ctx, "Commit", r, &resp)
	return resp.CommitTS, err
}

func (c *Client) Rollback(ctx context.Context, r *TxnReq) error {
	return c.call(ctx, "Rollback", r, &Empty{})
}

func (c *Client) TxnStatus(ctx context.Context, r *TxnReq) (*TxnStatusResp, error) {
	var resp TxnStatusResp
	err := c.call(ctx, "TxnStatus", r, &resp)
	return &resp, err
}

// Ping checks that the node answers.
func (c *Client) Ping(ctx context.Context) (string, error) {
	var resp PingResp
	err := c.call(ctx, "Ping", &Empty{}, &resp)
	return resp.Owner, err
}

func (c *Client) AutoIncrement(ctx context.Context, r *AutoIncReq) (uint64, error) {
	var resp AutoIncResp
	err := c.call(ctx, "AutoIncrement", r, &resp)
	return resp.First, err
}

func (c *Client) Heartbeat(ctx context.Context, r *HeartbeatReq) error {
	return c.call(ctx, "Heartbeat", r, &Empty{})
}
