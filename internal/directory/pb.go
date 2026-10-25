package directory

import (
	"github.com/salesstair-01/001/internal/directory/dirpb"
	"github.com/salesstair-01/001/internal/kv"
	"github.com/salesstair-01/001/internal/kvrpc"
)

// Converters between the directory's Go messages and dirpb (proto/s2z/directory).

func init() {
	kvrpc.Message(func() *dirpb.Empty { return &dirpb.Empty{} },
		func(*Empty) *dirpb.Empty { return &dirpb.Empty{} },
		func(*dirpb.Empty, *Empty) {})
	kvrpc.Message(func() *dirpb.RegisterReq { return &dirpb.RegisterReq{} },
		func(r *RegisterReq) *dirpb.RegisterReq { return &dirpb.RegisterReq{Node: r.Node, Addr: r.Addr} },
		func(m *dirpb.RegisterReq, r *RegisterReq) { *r = RegisterReq{Node: m.Node, Addr: m.Addr} })
	kvrpc.Message(func() *dirpb.HeartbeatReq { return &dirpb.HeartbeatReq{} },
		func(r *HeartbeatReq) *dirpb.HeartbeatReq {
			ps := make([]*dirpb.PartitionEpoch, len(r.Partitions))
			for i, p := range r.Partitions {
				ps[i] = &dirpb.PartitionEpoch{Partition: p.Partition, Epoch: p.Epoch}
			}
			return &dirpb.HeartbeatReq{Node: r.Node, Addr: r.Addr, Partitions: ps}
		},
		func(m *dirpb.HeartbeatReq, r *HeartbeatReq) {
			ps := make([]kv.PartitionEpoch, len(m.Partitions))
			for i, p := range m.Partitions {
				ps[i] = kv.PartitionEpoch{Partition: p.Partition, Epoch: p.Epoch}
			}
			*r = HeartbeatReq{Node: m.Node, Addr: m.Addr, Partitions: ps}
		})
	kvrpc.Message(func() *dirpb.ReleaseReq { return &dirpb.ReleaseReq{} },
		func(r *ReleaseReq) *dirpb.ReleaseReq {
			return &dirpb.ReleaseReq{Node: r.Node, Partition: r.Partition, Epoch: r.Epoch, Exiting: r.Exiting}
		},
		func(m *dirpb.ReleaseReq, r *ReleaseReq) {
			*r = ReleaseReq{Node: m.Node, Partition: m.Partition, Epoch: m.Epoch, Exiting: m.Exiting}
		})
	kvrpc.Message(func() *dirpb.ResolveReq { return &dirpb.ResolveReq{} },
		func(r *ResolveReq) *dirpb.ResolveReq {
			return &dirpb.ResolveReq{Partition: r.Partition, Stale: r.Stale}
		},
		func(m *dirpb.ResolveReq, r *ResolveReq) { *r = ResolveReq{Partition: m.Partition, Stale: m.Stale} })
	kvrpc.Message(func() *dirpb.Route { return &dirpb.Route{} },
		func(r *Route) *dirpb.Route { return &dirpb.Route{Node: r.Node, Addr: r.Addr, Epoch: r.Epoch} },
		func(m *dirpb.Route, r *Route) { *r = Route{Node: m.Node, Addr: m.Addr, Epoch: m.Epoch} })
}
