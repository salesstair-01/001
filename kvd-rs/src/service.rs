//! The gRPC service `s2z.kv.KV` (proto/s2z/kv/kv.proto), as served by the
//! Go owner's `internal/kvrpc.Server`.

use std::sync::Arc;
use std::time::Duration;

use tonic::{Request, Response, Status};

use crate::locks::{LockMode, TxnId};
use crate::node::Node;
use crate::partition::{Guard, Isolation, Mutation, Op, Partition, ReadOptions};
use crate::pb;

pub struct Service {
    pub node: Arc<Node>,
    /// Take a partition over on Open when this node doesn't serve it.
    pub auto_open: bool,
}

type R<T> = Result<Response<T>, Status>;

fn txn_id(b: &[u8]) -> TxnId {
    let mut id = [0u8; 16];
    let n = b.len().min(16);
    id[..n].copy_from_slice(&b[..n]);
    id
}

fn read_opts(o: Option<pb::ReadOpts>) -> ReadOptions {
    let o = o.unwrap_or_default();
    ReadOptions {
        stmt: o.stmt,
        include_stmt: o.include_stmt,
        lock: LockMode::from_i32(o.lock),
        lock_wait: Duration::from_millis(o.lock_wait_ms.max(0) as u64),
    }
}

impl Service {
    fn part(&self, h: &Option<pb::Header>) -> Result<(Arc<Partition>, u64), Status> {
        let h = (*h).unwrap_or_default();
        let p = self.node.partition(h.partition)?;
        if !h.background {
            p.touch();
        }
        Ok((p, h.epoch))
    }
}

#[tonic::async_trait]
impl pb::kv_server::Kv for Service {
    async fn open(&self, r: Request<pb::OpenReq>) -> R<pb::OpenResp> {
        let _t = Timer("open", std::time::Instant::now());
        let pid = r.into_inner().partition;
        let p = if self.auto_open {
            self.node.open(pid).await?
        } else {
            self.node.partition(pid)?
        };
        Ok(Response::new(pb::OpenResp { epoch: p.epoch }))
    }

    async fn begin(&self, r: Request<pb::BeginReq>) -> R<pb::Empty> {
        let _t = Timer("begin", std::time::Instant::now());
        let r = r.into_inner();
        let (p, epoch) = self.part(&r.h)?;
        let iso = if r.isolation == pb::Isolation::ReadCommitted as i32 {
            Isolation::ReadCommitted
        } else {
            Isolation::RepeatableRead
        };
        p.begin(epoch, txn_id(&r.txn), iso, r.consistent_snapshot)?;
        Ok(Response::new(pb::Empty {}))
    }

    async fn get(&self, r: Request<pb::GetReq>) -> R<pb::GetResp> {
        let _t = Timer("get", std::time::Instant::now());
        let r = r.into_inner();
        let (p, epoch) = self.part(&r.h)?;
        let vs = p.get(epoch, txn_id(&r.txn), &r.keys, read_opts(r.opts)).await?;
        let found = vs.iter().map(Option::is_some).collect();
        let values = vs.into_iter().map(Option::unwrap_or_default).collect();
        Ok(Response::new(pb::GetResp { values, found }))
    }

    async fn scan(&self, r: Request<pb::ScanReq>) -> R<pb::ScanResp> {
        let _t = Timer("scan", std::time::Instant::now());
        let r = r.into_inner();
        let (p, epoch) = self.part(&r.h)?;
        let limit = r.limit.max(0) as usize;
        let (rows, resume) = p
            .scan(epoch, txn_id(&r.txn), &r.start, &r.end, limit, read_opts(r.opts))
            .await?;
        let (keys, values) = rows.into_iter().unzip();
        Ok(Response::new(pb::ScanResp {
            keys,
            values,
            resume: resume.unwrap_or_default(),
        }))
    }

    async fn mutate(&self, r: Request<pb::MutateReq>) -> R<pb::Empty> {
        let _t = Timer("mutate", std::time::Instant::now());
        let r = r.into_inner();
        let (p, epoch) = self.part(&r.h)?;
        let muts = r
            .mutations
            .into_iter()
            .map(|m| Mutation {
                op: match m.op {
                    1 => Op::Delete,
                    2 => Op::InsertOnly,
                    _ => Op::Put,
                },
                key: m.key,
                value: m.value,
            })
            .collect();
        let wait = Duration::from_millis(r.lock_wait_ms.max(0) as u64);
        p.mutate(epoch, txn_id(&r.txn), r.stmt, muts, wait).await?;
        Ok(Response::new(pb::Empty {}))
    }

    async fn rollback_statement(&self, r: Request<pb::StmtReq>) -> R<pb::Empty> {
        let _t = Timer("rollback_statement", std::time::Instant::now());
        let r = r.into_inner();
        let (p, epoch) = self.part(&r.h)?;
        p.rollback_statement(epoch, txn_id(&r.txn), r.stmt)?;
        Ok(Response::new(pb::Empty {}))
    }

    async fn commit(&self, r: Request<pb::TxnReq>) -> R<pb::CommitResp> {
        let _t = Timer("commit", std::time::Instant::now());
        let r = r.into_inner();
        let (p, epoch) = self.part(&r.h)?;
        let guards = r
            .guards
            .into_iter()
            .map(|g| Guard {
                key: g.key,
                value: g.value,
            })
            .collect();
        let ts = p.commit(epoch, txn_id(&r.txn), guards).await?;
        Ok(Response::new(pb::CommitResp { commit_ts: ts }))
    }

    async fn rollback(&self, r: Request<pb::TxnReq>) -> R<pb::Empty> {
        let _t = Timer("rollback", std::time::Instant::now());
        let r = r.into_inner();
        let (p, epoch) = self.part(&r.h)?;
        p.rollback(epoch, txn_id(&r.txn))?;
        Ok(Response::new(pb::Empty {}))
    }

    async fn txn_status(&self, r: Request<pb::TxnReq>) -> R<pb::TxnStatusResp> {
        let _t = Timer("txn_status", std::time::Instant::now());
        let r = r.into_inner();
        let (p, epoch) = self.part(&r.h)?;
        let (state, ts) = p.txn_status(epoch, txn_id(&r.txn))?;
        Ok(Response::new(pb::TxnStatusResp {
            state: state as i32,
            commit_ts: ts,
        }))
    }

    async fn ping(&self, _: Request<pb::Empty>) -> R<pb::PingResp> {
        let _t = Timer("ping", std::time::Instant::now());
        Ok(Response::new(pb::PingResp {
            owner: self.node.owner_id().to_string(),
        }))
    }

    async fn auto_increment(&self, r: Request<pb::AutoIncReq>) -> R<pb::AutoIncResp> {
        let _t = Timer("auto_increment", std::time::Instant::now());
        let r = r.into_inner();
        let (p, epoch) = self.part(&r.h)?;
        let first = p
            .auto_increment(epoch, txn_id(&r.txn), r.table, r.count, r.at_least)
            .await?;
        Ok(Response::new(pb::AutoIncResp { first }))
    }

    async fn heartbeat(&self, r: Request<pb::HeartbeatReq>) -> R<pb::Empty> {
        let _t = Timer("heartbeat", std::time::Instant::now());
        let r = r.into_inner();
        let (p, epoch) = self.part(&r.h)?;
        let ids: Vec<TxnId> = r.txns.iter().map(|b| txn_id(b)).collect();
        p.heartbeat(epoch, &ids)?;
        Ok(Response::new(pb::Empty {}))
    }
}

/// Logs each call's duration at debug level.
struct Timer(&'static str, std::time::Instant);

impl Drop for Timer {
    fn drop(&mut self) {
        tracing::debug!(method = self.0, took = ?self.1.elapsed(), "rpc");
    }
}
