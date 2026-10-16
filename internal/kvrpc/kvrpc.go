// Package kvrpc is the gRPC API between the query engine and the KV owner
// (D7). Messages are plain Go structs carried with a gob codec, so the API
// needs no code generation. Every request carries the partition id and the
// epoch the caller expects; a mismatch returns EpochChanged.
package kvrpc

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/hex"
	"errors"
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
		Found  []bool // gob does not keep nil apart from empty
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

func (o ReadOpts) kv() kv.ReadOptions {
	return kv.ReadOptions{Stmt: o.Stmt, IncludeStmt: o.IncludeStmt, Lock: o.Lock, LockWait: time.Duration(o.LockWaitMs) * time.Millisecond}
}

// ---- codec -------------------------------------------------------------------

type gobCodec struct{}

func (gobCodec) Name() string { return "gob" }

func (gobCodec) Marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (gobCodec) Unmarshal(data []byte, v any) error {
	return gob.NewDecoder(bytes.NewReader(data)).Decode(v)
}

// ServerOption makes a grpc.Server use the gob codec.
func ServerOption() grpc.ServerOption { return grpc.ForceServerCodec(gobCodec{}) }

// DialOptions make a client connection use the gob codec and reconnect
// quickly. Calls fail fast when the owner is unreachable: the query engine
// then re-resolves the owner (which wakes or moves the partition) and retries.
func DialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.ForceCodec(gobCodec{})),
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

// toStatus turns a KV error into a gRPC status that keeps its kind and key.
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	var ke *kv.Error
	if errors.As(err, &ke) {
		return status.Error(codes.Aborted, errPrefix+ke.Kind.String()+"|"+hex.EncodeToString(ke.Key)+"|"+ke.Msg)
	}
	return status.Error(codes.Internal, err.Error())
}

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

// ---- service -----------------------------------------------------------------

const serviceName = "s2z.kv.KV"

// Server serves a kv.Node over gRPC.
type Server struct {
	Node *kv.Node
	// AutoOpen takes a partition over on Open when this node doesn't serve it.
	// The directory (slice 6) replaces this with explicit placement.
	AutoOpen bool
}

func (s *Server) part(h Header) (*kv.Partition, error) {
	p, err := s.Node.Partition(h.Partition)
	if err == nil && !h.Background {
		p.Touch()
	}
	return p, err
}

type handler func(s *Server, ctx context.Context, dec func(any) error) (any, error)

func unary[Req any, Resp any](f func(s *Server, ctx context.Context, req *Req) (*Resp, error)) handler {
	return func(s *Server, ctx context.Context, dec func(any) error) (any, error) {
		req := new(Req)
		if err := dec(req); err != nil {
			return nil, err
		}
		resp, err := f(s, ctx, req)
		if err != nil {
			return nil, toStatus(err)
		}
		return resp, nil
	}
}

var methods = map[string]handler{
	"Open": unary(func(s *Server, ctx context.Context, r *OpenReq) (*OpenResp, error) {
		if !s.AutoOpen {
			p, err := s.Node.Partition(r.Partition)
			if err != nil {
				return nil, err
			}
			return &OpenResp{Epoch: p.Epoch()}, nil
		}
		p, err := s.Node.Open(ctx, r.Partition)
		if err != nil {
			return nil, err
		}
		return &OpenResp{Epoch: p.Epoch()}, nil
	}),
	"Begin": unary(func(s *Server, _ context.Context, r *BeginReq) (*Empty, error) {
		p, err := s.part(r.H)
		if err != nil {
			return nil, err
		}
		return &Empty{}, p.Begin(r.H.Epoch, r.Txn, kv.BeginOptions{Isolation: r.Isolation, ConsistentSnapshot: r.ConsistentSnapshot})
	}),
	"Get": unary(func(s *Server, _ context.Context, r *GetReq) (*GetResp, error) {
		p, err := s.part(r.H)
		if err != nil {
			return nil, err
		}
		vs, err := p.Get(r.H.Epoch, r.Txn, r.Keys, r.Opts.kv())
		resp := &GetResp{Values: vs, Found: make([]bool, len(vs))}
		for i, v := range vs {
			resp.Found[i] = v != nil
		}
		return resp, err
	}),
	"Scan": unary(func(s *Server, _ context.Context, r *ScanReq) (*ScanResp, error) {
		p, err := s.part(r.H)
		if err != nil {
			return nil, err
		}
		rows, resume, err := p.Scan(r.H.Epoch, r.Txn, r.Start, r.End, r.Limit, r.Opts.kv())
		if err != nil {
			return nil, err
		}
		resp := &ScanResp{Resume: resume, Keys: make([][]byte, len(rows)), Values: make([][]byte, len(rows))}
		for i, kvp := range rows {
			resp.Keys[i], resp.Values[i] = kvp.Key, kvp.Value
		}
		return resp, nil
	}),
	"Mutate": unary(func(s *Server, _ context.Context, r *MutateReq) (*Empty, error) {
		p, err := s.part(r.H)
		if err != nil {
			return nil, err
		}
		return &Empty{}, p.Mutate(r.H.Epoch, r.Txn, r.Stmt, r.Mutations, time.Duration(r.LockWaitMs)*time.Millisecond)
	}),
	"RollbackStatement": unary(func(s *Server, _ context.Context, r *StmtReq) (*Empty, error) {
		p, err := s.part(r.H)
		if err != nil {
			return nil, err
		}
		return &Empty{}, p.RollbackStatement(r.H.Epoch, r.Txn, r.Stmt)
	}),
	"Commit": unary(func(s *Server, _ context.Context, r *TxnReq) (*CommitResp, error) {
		p, err := s.part(r.H)
		if err != nil {
			return nil, err
		}
		ts, err := p.CommitGuarded(r.H.Epoch, r.Txn, r.Guards)
		return &CommitResp{CommitTS: ts}, err
	}),
	"Rollback": unary(func(s *Server, _ context.Context, r *TxnReq) (*Empty, error) {
		p, err := s.part(r.H)
		if err != nil {
			return nil, err
		}
		return &Empty{}, p.Rollback(r.H.Epoch, r.Txn)
	}),
	"TxnStatus": unary(func(s *Server, _ context.Context, r *TxnReq) (*TxnStatusResp, error) {
		p, err := s.part(r.H)
		if err != nil {
			return nil, err
		}
		st, ts, err := p.TxnStatus(r.H.Epoch, r.Txn)
		return &TxnStatusResp{State: st, CommitTS: ts}, err
	}),
	"Ping": unary(func(s *Server, _ context.Context, _ *Empty) (*PingResp, error) {
		return &PingResp{Owner: s.Node.OwnerID()}, nil
	}),
	"AutoIncrement": unary(func(s *Server, _ context.Context, r *AutoIncReq) (*AutoIncResp, error) {
		p, err := s.part(r.H)
		if err != nil {
			return nil, err
		}
		first, err := p.AutoIncrement(r.H.Epoch, r.Txn, r.Table, r.Count, r.AtLeast)
		return &AutoIncResp{First: first}, err
	}),
	"Heartbeat": unary(func(s *Server, _ context.Context, r *HeartbeatReq) (*Empty, error) {
		p, err := s.part(r.H)
		if err != nil {
			return nil, err
		}
		return &Empty{}, p.Heartbeat(r.H.Epoch, r.Txns)
	}),
}

// Register adds the KV service to a gRPC server.
func Register(g *grpc.Server, s *Server) {
	desc := grpc.ServiceDesc{ServiceName: serviceName, HandlerType: (*any)(nil)}
	for name, h := range methods {
		h := h
		desc.Methods = append(desc.Methods, grpc.MethodDesc{
			MethodName: name,
			Handler: func(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				return h(srv.(*Server), ctx, dec)
			},
		})
	}
	g.RegisterService(&desc, s)
}

// ---- client ------------------------------------------------------------------

// Client calls a KV owner.
type Client struct {
	cc *grpc.ClientConn
}

func NewClient(cc *grpc.ClientConn) *Client { return &Client{cc: cc} }

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
