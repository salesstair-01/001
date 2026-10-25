//! A KV process serving many partitions, each its own SlateDB database
//! (`internal/kv/node.go`). Concurrent opens of one partition share a
//! single takeover. With an idle-suspend time, partitions without open
//! transactions or requests for that long are suspended; the node reports
//! each suspension, and that it serves nothing any more, as events (the
//! directory agent releases the partition and may exit the process).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;
use parking_lot::Mutex;
use tokio::sync::mpsc;

use crate::error::{Kind, KvError, Result};
use crate::partition::{Config, Partition};

#[derive(Debug, PartialEq, Eq)]
pub enum Event {
    /// The node suspended a partition and no longer serves it.
    Suspended { partition: u64, epoch: u64 },
    /// The node served partitions and now serves none.
    Empty,
}

pub struct Node {
    cfg: Config,
    store: Arc<dyn ObjectStore>,
    partitions: Mutex<HashMap<u64, Arc<Partition>>>,
    opening: Mutex<HashMap<u64, Arc<tokio::sync::Mutex<()>>>>,
    events: Option<mpsc::UnboundedSender<Event>>,
}

impl Node {
    /// A node; events, if given, receive suspensions and emptiness.
    pub fn new(cfg: Config, store: Arc<dyn ObjectStore>, events: Option<mpsc::UnboundedSender<Event>>) -> Arc<Node> {
        Arc::new(Node {
            cfg,
            store,
            partitions: Mutex::default(),
            opening: Mutex::default(),
            events,
        })
    }

    pub fn owner_id(&self) -> &str {
        &self.cfg.owner_id
    }

    /// The partitions this node serves, with their epochs.
    pub fn partitions(&self) -> Vec<(u64, u64)> {
        self.partitions
            .lock()
            .values()
            .filter(|p| p.stopped().is_none())
            .map(|p| (p.id, p.epoch))
            .collect()
    }

    fn opening_count(&self) -> usize {
        self.opening.lock().values().filter(|g| g.try_lock().is_err()).count()
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

    fn remove(&self, p: &Arc<Partition>) -> bool {
        let mut ps = self.partitions.lock();
        if ps.get(&p.id).is_some_and(|q| Arc::ptr_eq(q, p)) {
            ps.remove(&p.id);
            return true;
        }
        false
    }

    fn forget(&self, p: &Arc<Partition>) {
        self.remove(p);
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

    /// Suspends idle partitions every second until the process exits.
    pub fn spawn_idle_loop(self: &Arc<Self>, idle: Duration) {
        let node = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            let mut had_partitions = false;
            loop {
                tick.tick().await;
                let Some(n) = node.upgrade() else { return };
                had_partitions |= n.idle_pass(idle).await;
                let empty = n.partitions.lock().is_empty() && n.opening_count() == 0;
                if empty && had_partitions {
                    had_partitions = false;
                    n.emit(Event::Empty);
                }
            }
        });
    }

    /// One pass of the idle loop; reports whether the node served anything.
    async fn idle_pass(&self, idle: Duration) -> bool {
        let ps: Vec<Arc<Partition>> = self.partitions.lock().values().cloned().collect();
        let mut served = !ps.is_empty() || self.opening_count() > 0;
        for p in ps {
            if p.stopped().is_some() {
                // fenced by a newer owner, or failed
                self.forget(&p);
                continue;
            }
            if p.idle_for() < idle {
                continue;
            }
            match p.try_suspend(idle).await {
                Ok(false) => {}
                Ok(true) => {
                    self.remove(&p);
                    served = true;
                    tracing::info!(partition = p.id, epoch = p.epoch, "suspended idle partition");
                    self.emit(Event::Suspended {
                        partition: p.id,
                        epoch: p.epoch,
                    });
                }
                Err(e) => {
                    // The database is closed either way: stop serving it.
                    tracing::warn!(partition = p.id, err = %e, "idle suspend failed");
                    self.remove(&p);
                }
            }
        }
        served
    }

    fn emit(&self, e: Event) {
        if let Some(tx) = &self.events {
            let _ = tx.send(e);
        }
    }

    /// Closes every partition, flushing each memtable.
    pub async fn close(&self) {
        let ps: Vec<_> = self.partitions.lock().drain().map(|(_, p)| p).collect();
        for p in ps {
            p.close().await;
        }
    }
}
