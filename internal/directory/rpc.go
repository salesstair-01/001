package directory

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/salesstair-01/001/internal/kv"
)

// The directory API uses the same gob codec as the KV API (kvrpc.ServerOption,
// kvrpc.DialOptions).

type (
	RegisterReq  struct{ Node, Addr string }
	HeartbeatReq struct {
		Node, Addr string
		Partitions []kv.PartitionEpoch
	}
	ReleaseReq struct {
		Node             string
		Partition, Epoch uint64
		Exiting          bool
	}
	ResolveReq struct{ Partition, Stale uint64 }
	Empty      struct{}
)

const serviceName = "s2z.directory.Directory"

type method func(d *Directory, ctx context.Context, dec func(any) error) (any, error)

func unary[Req any, Resp any](f func(d *Directory, ctx context.Context, r *Req) (*Resp, error)) method {
	return func(d *Directory, ctx context.Context, dec func(any) error) (any, error) {
		req := new(Req)
		if err := dec(req); err != nil {
			return nil, err
		}
		resp, err := f(d, ctx, req)
		if err != nil {
			return nil, status.Error(codes.Unavailable, err.Error())
		}
		return resp, nil
	}
}

var methods = map[string]method{
	"RegisterNode": unary(func(d *Directory, _ context.Context, r *RegisterReq) (*Empty, error) {
		d.RegisterNode(r.Node, r.Addr)
		return &Empty{}, nil
	}),
	"Heartbeat": unary(func(d *Directory, _ context.Context, r *HeartbeatReq) (*Empty, error) {
		return &Empty{}, d.Heartbeat(r.Node, r.Addr, r.Partitions)
	}),
	"Release": unary(func(d *Directory, _ context.Context, r *ReleaseReq) (*Empty, error) {
		return &Empty{}, d.Release(r.Node, r.Partition, r.Epoch, r.Exiting)
	}),
	"Peek": unary(func(d *Directory, _ context.Context, r *ResolveReq) (*Route, error) {
		route, err := d.row(r.Partition)
		return &route, err
	}),
	"Resolve": unary(func(d *Directory, ctx context.Context, r *ResolveReq) (*Route, error) {
		route, err := d.Resolve(ctx, r.Partition, r.Stale)
		return &route, err
	}),
}

// Register adds the directory service to a gRPC server.
func Register(g *grpc.Server, d *Directory) {
	desc := grpc.ServiceDesc{ServiceName: serviceName, HandlerType: (*any)(nil)}
	for name, m := range methods {
		m := m
		desc.Methods = append(desc.Methods, grpc.MethodDesc{
			MethodName: name,
			Handler: func(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				return m(srv.(*Directory), ctx, dec)
			},
		})
	}
	g.RegisterService(&desc, d)
}

// Client calls the directory.
type Client struct{ cc *grpc.ClientConn }

func NewClient(cc *grpc.ClientConn) *Client { return &Client{cc: cc} }

func (c *Client) call(ctx context.Context, m string, req, resp any) error {
	return c.cc.Invoke(ctx, "/"+serviceName+"/"+m, req, resp)
}

func (c *Client) RegisterNode(ctx context.Context, node, addr string) error {
	return c.call(ctx, "RegisterNode", &RegisterReq{Node: node, Addr: addr}, &Empty{})
}

func (c *Client) Heartbeat(ctx context.Context, node, addr string, parts []kv.PartitionEpoch) error {
	return c.call(ctx, "Heartbeat", &HeartbeatReq{Node: node, Addr: addr, Partitions: parts}, &Empty{})
}

func (c *Client) Release(ctx context.Context, node string, partition, epoch uint64, exiting bool) error {
	return c.call(ctx, "Release", &ReleaseReq{Node: node, Partition: partition, Epoch: epoch, Exiting: exiting}, &Empty{})
}

// Resolve returns the route to a partition, waking it if needed. stale is an
// epoch the caller found no longer served (0 = none).
func (c *Client) Resolve(ctx context.Context, partition, stale uint64) (Route, error) {
	var r Route
	err := c.call(ctx, "Resolve", &ResolveReq{Partition: partition, Stale: stale}, &r)
	return r, err
}

// Peek returns a partition's route without waking it (Node is empty while suspended).
func (c *Client) Peek(ctx context.Context, partition uint64) (Route, error) {
	var r Route
	err := c.call(ctx, "Peek", &ResolveReq{Partition: partition}, &r)
	return r, err
}
