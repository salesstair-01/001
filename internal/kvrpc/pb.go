package kvrpc

import (
	"github.com/salesstair-01/001/internal/kv"
	"github.com/salesstair-01/001/internal/kvrpc/kvpb"
)

// Converters between the Go messages and kvpb. An absent and an empty byte
// field are the same in proto3, as they were with gob.

func init() {
	Message(func() *kvpb.Empty { return &kvpb.Empty{} },
		func(*Empty) *kvpb.Empty { return &kvpb.Empty{} },
		func(*kvpb.Empty, *Empty) {})
	Message(func() *kvpb.OpenReq { return &kvpb.OpenReq{} },
		func(r *OpenReq) *kvpb.OpenReq { return &kvpb.OpenReq{Partition: r.Partition} },
		func(m *kvpb.OpenReq, r *OpenReq) { r.Partition = m.Partition })
	Message(func() *kvpb.OpenResp { return &kvpb.OpenResp{} },
		func(r *OpenResp) *kvpb.OpenResp { return &kvpb.OpenResp{Epoch: r.Epoch} },
		func(m *kvpb.OpenResp, r *OpenResp) { r.Epoch = m.Epoch })
	Message(func() *kvpb.BeginReq { return &kvpb.BeginReq{} },
		func(r *BeginReq) *kvpb.BeginReq {
			return &kvpb.BeginReq{H: headerPB(r.H), Txn: r.Txn[:], Isolation: kvpb.Isolation(r.Isolation), ConsistentSnapshot: r.ConsistentSnapshot}
		},
		func(m *kvpb.BeginReq, r *BeginReq) {
			*r = BeginReq{H: header(m.H), Txn: txnID(m.Txn), Isolation: kv.Isolation(m.Isolation), ConsistentSnapshot: m.ConsistentSnapshot}
		})
	Message(func() *kvpb.GetReq { return &kvpb.GetReq{} },
		func(r *GetReq) *kvpb.GetReq {
			return &kvpb.GetReq{H: headerPB(r.H), Txn: r.Txn[:], Keys: r.Keys, Opts: readOptsPB(r.Opts)}
		},
		func(m *kvpb.GetReq, r *GetReq) {
			*r = GetReq{H: header(m.H), Txn: txnID(m.Txn), Keys: m.Keys, Opts: readOpts(m.Opts)}
		})
	Message(func() *kvpb.GetResp { return &kvpb.GetResp{} },
		func(r *GetResp) *kvpb.GetResp { return &kvpb.GetResp{Values: r.Values, Found: r.Found} },
		func(m *kvpb.GetResp, r *GetResp) { *r = GetResp{Values: m.Values, Found: m.Found} })
	Message(func() *kvpb.ScanReq { return &kvpb.ScanReq{} },
		func(r *ScanReq) *kvpb.ScanReq {
			return &kvpb.ScanReq{H: headerPB(r.H), Txn: r.Txn[:], Start: r.Start, End: r.End, Limit: int64(r.Limit), Opts: readOptsPB(r.Opts)}
		},
		func(m *kvpb.ScanReq, r *ScanReq) {
			*r = ScanReq{H: header(m.H), Txn: txnID(m.Txn), Start: m.Start, End: m.End, Limit: int(m.Limit), Opts: readOpts(m.Opts)}
		})
	Message(func() *kvpb.ScanResp { return &kvpb.ScanResp{} },
		func(r *ScanResp) *kvpb.ScanResp {
			return &kvpb.ScanResp{Keys: r.Keys, Values: r.Values, Resume: r.Resume}
		},
		func(m *kvpb.ScanResp, r *ScanResp) { *r = ScanResp{Keys: m.Keys, Values: m.Values, Resume: m.Resume} })
	Message(func() *kvpb.MutateReq { return &kvpb.MutateReq{} },
		func(r *MutateReq) *kvpb.MutateReq {
			ms := make([]*kvpb.Mutation, len(r.Mutations))
			for i, mu := range r.Mutations {
				ms[i] = &kvpb.Mutation{Op: kvpb.MutationOp(mu.Op), Key: mu.Key, Value: mu.Value}
			}
			return &kvpb.MutateReq{H: headerPB(r.H), Txn: r.Txn[:], Stmt: r.Stmt, Mutations: ms, LockWaitMs: r.LockWaitMs}
		},
		func(m *kvpb.MutateReq, r *MutateReq) {
			ms := make([]kv.Mutation, len(m.Mutations))
			for i, mu := range m.Mutations {
				ms[i] = kv.Mutation{Op: kv.MutationOp(mu.Op), Key: mu.Key, Value: mu.Value}
				if ms[i].Op != kv.OpDelete && ms[i].Value == nil {
					// An empty value (a secondary-index entry) is a row; nil would read as absent.
					ms[i].Value = []byte{}
				}
			}
			*r = MutateReq{H: header(m.H), Txn: txnID(m.Txn), Stmt: m.Stmt, Mutations: ms, LockWaitMs: m.LockWaitMs}
		})
	Message(func() *kvpb.StmtReq { return &kvpb.StmtReq{} },
		func(r *StmtReq) *kvpb.StmtReq { return &kvpb.StmtReq{H: headerPB(r.H), Txn: r.Txn[:], Stmt: r.Stmt} },
		func(m *kvpb.StmtReq, r *StmtReq) { *r = StmtReq{H: header(m.H), Txn: txnID(m.Txn), Stmt: m.Stmt} })
	Message(func() *kvpb.TxnReq { return &kvpb.TxnReq{} },
		func(r *TxnReq) *kvpb.TxnReq {
			gs := make([]*kvpb.Guard, len(r.Guards))
			for i, g := range r.Guards {
				gs[i] = &kvpb.Guard{Key: g.Key, Value: g.Value}
			}
			return &kvpb.TxnReq{H: headerPB(r.H), Txn: r.Txn[:], Guards: gs}
		},
		func(m *kvpb.TxnReq, r *TxnReq) {
			var gs []kv.Guard
			for _, g := range m.Guards {
				gs = append(gs, kv.Guard{Key: g.Key, Value: g.Value})
			}
			*r = TxnReq{H: header(m.H), Txn: txnID(m.Txn), Guards: gs}
		})
	Message(func() *kvpb.CommitResp { return &kvpb.CommitResp{} },
		func(r *CommitResp) *kvpb.CommitResp { return &kvpb.CommitResp{CommitTs: r.CommitTS} },
		func(m *kvpb.CommitResp, r *CommitResp) { r.CommitTS = m.CommitTs })
	Message(func() *kvpb.TxnStatusResp { return &kvpb.TxnStatusResp{} },
		func(r *TxnStatusResp) *kvpb.TxnStatusResp {
			return &kvpb.TxnStatusResp{State: kvpb.TxnState(r.State), CommitTs: r.CommitTS}
		},
		func(m *kvpb.TxnStatusResp, r *TxnStatusResp) {
			*r = TxnStatusResp{State: kv.TxnState(m.State), CommitTS: m.CommitTs}
		})
	Message(func() *kvpb.PingResp { return &kvpb.PingResp{} },
		func(r *PingResp) *kvpb.PingResp { return &kvpb.PingResp{Owner: r.Owner} },
		func(m *kvpb.PingResp, r *PingResp) { r.Owner = m.Owner })
	Message(func() *kvpb.AutoIncReq { return &kvpb.AutoIncReq{} },
		func(r *AutoIncReq) *kvpb.AutoIncReq {
			var txn []byte
			if r.Txn != (kv.TxnID{}) {
				txn = r.Txn[:]
			}
			return &kvpb.AutoIncReq{H: headerPB(r.H), Txn: txn, Table: r.Table, Count: r.Count, AtLeast: r.AtLeast}
		},
		func(m *kvpb.AutoIncReq, r *AutoIncReq) {
			*r = AutoIncReq{H: header(m.H), Txn: txnID(m.Txn), Table: m.Table, Count: m.Count, AtLeast: m.AtLeast}
		})
	Message(func() *kvpb.AutoIncResp { return &kvpb.AutoIncResp{} },
		func(r *AutoIncResp) *kvpb.AutoIncResp { return &kvpb.AutoIncResp{First: r.First} },
		func(m *kvpb.AutoIncResp, r *AutoIncResp) { r.First = m.First })
	Message(func() *kvpb.HeartbeatReq { return &kvpb.HeartbeatReq{} },
		func(r *HeartbeatReq) *kvpb.HeartbeatReq {
			ids := make([][]byte, len(r.Txns))
			for i := range r.Txns {
				ids[i] = r.Txns[i][:]
			}
			return &kvpb.HeartbeatReq{H: headerPB(r.H), Txns: ids}
		},
		func(m *kvpb.HeartbeatReq, r *HeartbeatReq) {
			ids := make([]kv.TxnID, len(m.Txns))
			for i, b := range m.Txns {
				ids[i] = txnID(b)
			}
			*r = HeartbeatReq{H: header(m.H), Txns: ids}
		})
}

func headerPB(h Header) *kvpb.Header {
	return &kvpb.Header{Partition: h.Partition, Epoch: h.Epoch, Background: h.Background}
}

func header(m *kvpb.Header) Header {
	return Header{Partition: m.GetPartition(), Epoch: m.GetEpoch(), Background: m.GetBackground()}
}

func readOptsPB(o ReadOpts) *kvpb.ReadOpts {
	return &kvpb.ReadOpts{Stmt: o.Stmt, IncludeStmt: o.IncludeStmt, Lock: kvpb.LockMode(o.Lock), LockWaitMs: o.LockWaitMs}
}

func readOpts(m *kvpb.ReadOpts) ReadOpts {
	return ReadOpts{Stmt: m.GetStmt(), IncludeStmt: m.GetIncludeStmt(), Lock: kv.LockMode(m.GetLock()), LockWaitMs: m.GetLockWaitMs()}
}

func txnID(b []byte) kv.TxnID {
	var id kv.TxnID
	copy(id[:], b)
	return id
}
