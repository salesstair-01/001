//! Connects the node to the directory, as `internal/directory/agent.go` does
//! for the Go owner: register, heartbeat every 2 s with the partitions
//! served, release each partition the node suspends, and, for a dedicated
//! per-tenant process, exit once it serves nothing.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{Notify, mpsc};
use tonic::transport::{Channel, Endpoint};

use crate::node::{Event, Node};
use crate::pb::dir;

pub struct Agent {
    pub node_id: String,
    pub addr: String,
    pub exit_when_idle: bool,
    /// A dedicated node never given a partition exits after this (zero = never).
    pub idle_exit: Duration,
    /// Notified once when the process should exit.
    pub exit: Arc<Notify>,
}

impl Agent {
    pub async fn run(self, dir_addr: String, node: Arc<Node>, mut events: mpsc::UnboundedReceiver<Event>) {
        let ch = match Endpoint::from_shared(format!("http://{dir_addr}")) {
            Ok(e) => e
                .connect_timeout(Duration::from_secs(2))
                .timeout(Duration::from_secs(5))
                .connect_lazy(),
            Err(e) => {
                tracing::error!(err = %e, "bad -directory address");
                self.exit.notify_one();
                return;
            }
        };
        let mut c = dir::directory_client::DirectoryClient::new(ch);
        loop {
            let req = dir::RegisterReq {
                node: self.node_id.clone(),
                addr: self.addr.clone(),
            };
            match c.register_node(req).await {
                Ok(_) => break,
                Err(e) => {
                    tracing::warn!(err = %e.message(), "register with directory");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
        tracing::info!(
            node = self.node_id,
            addr = self.addr,
            directory = dir_addr,
            "registered with directory"
        );
        let started = Instant::now();
        let mut served = false;
        let mut tick = tokio::time::interval(Duration::from_secs(2));
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    let parts = node.partitions();
                    served |= !parts.is_empty();
                    self.heartbeat(&mut c, parts).await;
                    if self.exit_when_idle && !served && !self.idle_exit.is_zero() && started.elapsed() > self.idle_exit {
                        self.exit("never given a partition");
                    }
                }
                ev = events.recv() => match ev {
                    Some(Event::Suspended { partition, epoch }) => {
                        served = true;
                        let exiting = self.exit_when_idle && node.partitions().is_empty();
                        self.release(&mut c, partition, epoch, exiting).await;
                    }
                    Some(Event::Empty) => {
                        if self.exit_when_idle {
                            self.exit("no partitions left");
                        }
                    }
                    None => return,
                },
            }
        }
    }

    async fn heartbeat(&self, c: &mut dir::directory_client::DirectoryClient<Channel>, parts: Vec<(u64, u64)>) {
        let req = dir::HeartbeatReq {
            node: self.node_id.clone(),
            addr: self.addr.clone(),
            partitions: parts
                .into_iter()
                .map(|(partition, epoch)| dir::PartitionEpoch { partition, epoch })
                .collect(),
        };
        if let Err(e) = c.heartbeat(req).await {
            tracing::warn!(err = %e.message(), "directory heartbeat");
        }
    }

    async fn release(
        &self,
        c: &mut dir::directory_client::DirectoryClient<Channel>,
        partition: u64,
        epoch: u64,
        exiting: bool,
    ) {
        let req = dir::ReleaseReq {
            node: self.node_id.clone(),
            partition,
            epoch,
            exiting,
        };
        if let Err(e) = c.release(req).await {
            tracing::warn!(partition, err = %e.message(), "release partition in directory");
        }
    }

    fn exit(&self, why: &str) {
        tracing::info!(node = self.node_id, why, "exiting");
        self.exit.notify_one();
    }
}
