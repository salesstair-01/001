//! A KV process serving many partitions, each its own SlateDB database
//! (`internal/kv/node.go`). Concurrent opens of one partition share a
//! single takeover.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;
use parking_lot::Mutex;

use crate::error::{Kind, KvError, Result};
use crate::partition::{Config, Partition};

pub struct Node {
    cfg: Config,
    store: Arc<dyn ObjectStore>,
    partitions: Mutex<HashMap<u64, Arc<Partition>>>,
    opening: Mutex<HashMap<u64, Arc<tokio::sync::Mutex<()>>>>,
}

impl Node {
    pub fn new(cfg: Config, store: Arc<dyn ObjectStore>) -> Arc<Node> {
        Arc::new(Node {
            cfg,
            store,
            partitions: Mutex::default(),
            opening: Mutex::default(),
        })
    }

    pub fn owner_id(&self) -> &str {
        &self.cfg.owner_id
    }

    /// The served partition pid, or EpochChanged.
    pub fn partition(&self, pid: u64) -> Result<Arc<Partition>> {
        let p = self.partitions.lock().get(&pid).cloned().ok_or_else(|| {
            KvError::new(
                Kind::EpochChanged,
                format!("partition {pid} is not served by this node"),
            )
        })?;
        if let Some(e) = p.stopped() {
            self.forget(&p);
            return Err(e);
        }
        Ok(p)
    }

    fn forget(&self, p: &Arc<Partition>) {
        let mut ps = self.partitions.lock();
        if ps.get(&p.id).is_some_and(|q| Arc::ptr_eq(q, p)) {
            ps.remove(&p.id);
        }
        drop(ps);
        let p = p.clone();
        tokio::spawn(async move { p.close().await });
    }

    /// The partition, taken over first if this node does not serve it.
    pub async fn open(&self, pid: u64) -> Result<Arc<Partition>> {
        let gate = self.opening.lock().entry(pid).or_default().clone();
        let _g = gate.lock().await;
        if let Some(p) = self.partitions.lock().get(&pid).cloned() {
            if p.stopped().is_none() {
                return Ok(p);
            }
            self.forget(&p);
        }
        let mut backoff = Duration::from_millis(50);
        let mut attempt = 0;
        let p = loop {
            match Partition::takeover(self.cfg.clone(), self.store.clone(), pid).await {
                Ok(p) => break p,
                // Another owner fenced this one while it opened: try again.
                Err(e) if e.kind == Kind::EpochChanged && attempt < 5 => {
                    attempt += 1;
                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                }
                Err(e) => return Err(e),
            }
        };
        self.partitions.lock().insert(pid, p.clone());
        Ok(p)
    }

    /// Closes every partition, flushing each memtable.
    pub async fn close(&self) {
        let ps: Vec<_> = self.partitions.lock().drain().map(|(_, p)| p).collect();
        for p in ps {
            p.close().await;
        }
    }
}
