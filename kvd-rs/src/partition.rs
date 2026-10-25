//! One partition owned by this process: a SlateDB database at `p/<id>` with
//! the transaction layer of `internal/kv` (txn.go, commit.go, autoinc.go)
//! on top. SlateDB is the ordered store, its WAL and manifest fencing replace
//! the Go owner's WAL segments, checkpoints and manifests.
//!
//! Commit: under `commit_lock`, check guards and insert-only keys, assign
//! commit_ts = max + 1 and write the batch (it is readable at once, like the
//! Go owner's pending writes); then, outside the lock, wait for durability.
//! Only then are the versions visible to snapshots (`visible`), the locks
//! released and the commit acknowledged.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use object_store::ObjectStore;
use parking_lot::Mutex;
use slatedb::config::{DurabilityLevel, ScanOptions, Settings};
use slatedb::db_cache::DbCache;
use slatedb::{Db, WriteBatch};

use crate::error::{Kind, KvError, Result};
use crate::keys;
use crate::locks::{DEFAULT_LOCK_WAIT, LockMode, LockTable, TxnId, TxnRef};

/// A delete marker: the value of a deleted row's newest version.
pub const DELETE_MARKER: u8 = 0x81;
pub const MAX_ROW_SIZE: usize = 1 << 20;
const TABLE_AUTO_INCREMENT: u32 = 3;

pub fn is_delete(v: &[u8]) -> bool {
    v.len() == 1 && v[0] == DELETE_MARKER
}

#[derive(Clone)]
pub struct Config {
    pub owner_id: String,
    /// The block cache shared by the node's partitions (None = SlateDB's own per database).
    pub cache: Option<Arc<dyn DbCache>>,
    pub flush_interval: Duration,
    pub l0_sst_bytes: usize,
    pub max_unflushed_bytes: usize,
    pub txn_lease: Duration,
    pub txn_id_window: Duration,
    pub intent_cap: usize,
    pub mvcc_gc_interval: Duration,
    /// How far back a point-in-time restore can go: MVCC GC keeps every
    /// version newer than this (zero = only what open snapshots need).
    pub restore_window: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            owner_id: String::new(),
            cache: None,
            flush_interval: Duration::from_millis(10),
            l0_sst_bytes: 16 << 20,
            max_unflushed_bytes: 256 << 20,
            txn_lease: Duration::from_secs(10),
            txn_id_window: Duration::from_secs(600),
            intent_cap: 64 << 20,
            mvcc_gc_interval: Duration::from_secs(60),
            restore_window: Duration::from_secs(3600),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Isolation {
    RepeatableRead,
    ReadCommitted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    Put,
    Delete,
    InsertOnly,
}

pub struct Mutation {
    pub op: Op,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

pub struct Guard {
    pub key: Vec<u8>,
    pub value: Vec<u8>, // empty = absent
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ReadOptions {
    pub stmt: u64,
    pub include_stmt: bool,
    pub lock: LockMode,
    pub lock_wait: Duration, // zero = DEFAULT_LOCK_WAIT
}

impl ReadOptions {
    fn skip(&self) -> u64 {
        if self.include_stmt { 0 } else { self.stmt }
    }
}

fn lock_wait(d: Duration) -> Duration {
    if d.is_zero() { DEFAULT_LOCK_WAIT } else { d }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxnState {
    Unknown = 0,
    Active = 1,
    Committed = 2,
}

#[derive(Clone)]
struct Intent {
    value: Vec<u8>, // row value or delete marker
    insert_only: bool,
}

struct Layer {
    id: u64,
    writes: HashMap<Vec<u8>, Intent>,
}

struct Txn {
    r: TxnRef,
    iso: Isolation,
    snapshot_ts: u64,
    has_snap: bool,
    snap_stmt: u64,
    layers: Vec<Layer>,
    bytes: usize,
    last_seen: Instant,
    committing: bool,
    autoinc: HashSet<u32>,
}

impl Txn {
    fn layer(&mut self, stmt: u64) -> &mut Layer {
        if self.layers.last().is_none_or(|l| l.id != stmt) {
            self.layers.push(Layer {
                id: stmt,
                writes: HashMap::new(),
            });
        }
        self.layers.last_mut().unwrap()
    }

    /// The transaction's own newest intent for key, ignoring statement skip.
    fn lookup(&self, key: &[u8], skip: u64) -> Option<&Intent> {
        self.layers
            .iter()
            .rev()
            .filter(|l| skip == 0 || l.id != skip)
            .find_map(|l| l.writes.get(key))
    }

    /// The final write per key, in key order, ignoring statement skip.
    fn merged(&self, skip: u64) -> Vec<(Vec<u8>, Intent)> {
        let mut out: HashMap<&[u8], Intent> = HashMap::new();
        for l in self.layers.iter().filter(|l| skip == 0 || l.id != skip) {
            for (k, i) in &l.writes {
                let mut i = i.clone();
                if out.get(k.as_slice()).is_some_and(|p| p.insert_only) {
                    // An update of a row this transaction inserted still must not exist outside it.
                    i.insert_only = true;
                }
                out.insert(k, i);
            }
        }
        let mut v: Vec<_> = out.into_iter().map(|(k, i)| (k.to_vec(), i)).collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    }
}

struct State {
    max_commit_ts: u64,
    txns: HashMap<TxnId, Txn>,
    recent: HashMap<TxnId, (u64, u64)>, // committed ids: commit_ts, wall micros
    autoinc: HashMap<u32, u64>,
    stop_err: Option<KvError>,
}

pub struct Partition {
    pub id: u64,
    pub epoch: u64,
    cfg: Config,
    db: Db,
    state: Mutex<State>,
    locks: LockTable,
    commit_lock: tokio::sync::Mutex<()>,
    visible: AtomicU64,
    last_active: Mutex<Instant>,
}

fn wall_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

fn be64(v: &Option<Bytes>) -> u64 {
    v.as_deref()
        .and_then(|b| b.try_into().ok())
        .map(u64::from_be_bytes)
        .unwrap_or(0)
}

const META_EPOCH: &[u8] = b"epoch";
const META_MAXTS: &[u8] = b"maxts";
const META_TXN: &[u8] = b"txn/";
/// time/<wall micros><commit_ts>, both big-endian: when each commit happened,
/// for point-in-time restore (restore.rs).
pub(crate) const META_TIME: &[u8] = b"time/";

pub(crate) fn time_key(wall: u64, ts: u64) -> Vec<u8> {
    let mut k = keys::meta_key(META_TIME);
    k.extend_from_slice(&wall.to_be_bytes());
    k.extend_from_slice(&ts.to_be_bytes());
    k
}

/// Splits a time record's key into its wall time and commit_ts.
pub(crate) fn split_time_key(k: &[u8]) -> Option<(u64, u64)> {
    let rest = k.strip_prefix(keys::meta_key(META_TIME).as_slice())?;
    let wall = u64::from_be_bytes(rest.get(..8)?.try_into().ok()?);
    let ts = u64::from_be_bytes(rest.get(8..16)?.try_into().ok()?);
    Some((wall, ts))
}

fn txn_key(id: &TxnId) -> Vec<u8> {
    let mut k = keys::meta_key(META_TXN);
    k.extend_from_slice(id);
    k
}

fn scan_opts() -> ScanOptions {
    ScanOptions {
        durability_filter: DurabilityLevel::Memory,
        read_ahead_bytes: 1 << 20,
        max_fetch_tasks: 4,
        ..ScanOptions::default()
    }
}

fn point_opts() -> ScanOptions {
    ScanOptions {
        durability_filter: DurabilityLevel::Memory,
        ..ScanOptions::default()
    }
}

fn not_open(id: &TxnId) -> KvError {
    KvError::new(
        Kind::TxnNotFound,
        format!("transaction {} is not open on this owner (aborted)", hex::encode(id)),
    )
}

impl Partition {
    /// Takes partition pid over: opening its database fences the previous
    /// writer; the epoch is then raised durably.
    pub async fn takeover(cfg: Config, store: Arc<dyn ObjectStore>, pid: u64) -> Result<Arc<Partition>> {
        let t0 = Instant::now();
        let settings = Settings {
            flush_interval: Some(cfg.flush_interval),
            l0_sst_size_bytes: cfg.l0_sst_bytes,
            max_unflushed_bytes: cfg.max_unflushed_bytes,
            manifest_poll_interval: Duration::from_secs(1),
            ..Settings::default()
        };
        let mut builder = Db::builder(format!("p/{pid}"), store).with_settings(settings);
        if let Some(cache) = &cfg.cache {
            builder = builder.with_db_cache(cache.clone(), pid);
        }
        let db = builder.build().await?;
        let opened = t0.elapsed();
        let epoch = be64(&db.get(keys::meta_key(META_EPOCH)).await?) + 1;
        let max_ts = be64(&db.get(keys::meta_key(META_MAXTS)).await?);
        let mut b = WriteBatch::new();
        b.put(keys::meta_key(META_EPOCH), epoch.to_be_bytes());
        db.write(b).await?.await_durable().await?;
        let epoch_written = t0.elapsed();

        // Committed ids from the window, so a commit whose response was lost is recognised.
        let mut recent = HashMap::new();
        let cutoff = wall_micros().saturating_sub(cfg.txn_id_window.as_micros() as u64);
        let prefix = keys::meta_key(META_TXN);
        let mut end = prefix.clone();
        *end.last_mut().unwrap() += 1;
        let mut it = db.scan_with_options(prefix.clone()..end, &scan_opts()).await?;
        while let Some(kv) = it.next().await? {
            let (Ok(id), Ok(v)) = (
                <TxnId>::try_from(&kv.key[prefix.len()..]),
                <[u8; 16]>::try_from(&kv.value[..]),
            ) else {
                continue;
            };
            let ts = u64::from_be_bytes(v[..8].try_into().unwrap());
            let wall = u64::from_be_bytes(v[8..].try_into().unwrap());
            if wall >= cutoff {
                recent.insert(id, (ts, wall));
            }
        }

        let p = Arc::new(Partition {
            id: pid,
            epoch,
            cfg,
            db,
            state: Mutex::new(State {
                max_commit_ts: max_ts,
                txns: HashMap::new(),
                recent,
                autoinc: HashMap::new(),
                stop_err: None,
            }),
            locks: LockTable::default(),
            commit_lock: tokio::sync::Mutex::new(()),
            visible: AtomicU64::new(max_ts),
            last_active: Mutex::new(Instant::now()),
        });
        tokio::spawn(background(Arc::downgrade(&p)));
        crate::metrics::TAKEOVER.observe(t0.elapsed());
        tracing::info!(partition = pid, epoch, max_commit_ts = max_ts, took = ?t0.elapsed(), ?opened, ?epoch_written, "took over partition");
        Ok(p)
    }

    pub fn visible_ts(&self) -> u64 {
        self.visible.load(Ordering::Acquire)
    }

    pub fn touch(&self) {
        *self.last_active.lock() = Instant::now();
    }

    pub fn idle_for(&self) -> Duration {
        self.last_active.lock().elapsed()
    }

    /// Suspends the partition if it has had no open transactions and no
    /// requests for idle: new requests get EpochChanged from here on, and
    /// the database is closed with a memtable flush, so the next wake has
    /// little WAL to replay. Reports whether it suspended.
    pub async fn try_suspend(&self, idle: Duration) -> Result<bool> {
        {
            let mut st = self.state.lock();
            if st.stop_err.is_some() || !st.txns.is_empty() || self.idle_for() < idle {
                return Ok(false);
            }
            st.stop_err = Some(KvError::suspended());
        }
        let _g = self.commit_lock.lock().await;
        self.db.close().await?;
        Ok(true)
    }

    pub fn stopped(&self) -> Option<KvError> {
        self.state.lock().stop_err.clone()
    }

    fn check_epoch(&self, epoch: u64) -> Result<()> {
        if epoch != 0 && epoch != self.epoch {
            return Err(KvError::new(
                Kind::EpochChanged,
                format!("epoch {epoch} requested, owner serves {}", self.epoch),
            ));
        }
        Ok(())
    }

    /// Makes the partition stop serving: every open transaction is aborted.
    pub fn stop(&self, err: KvError) {
        let mut st = self.state.lock();
        if st.stop_err.is_some() {
            return;
        }
        tracing::warn!(partition = self.id, reason = %err, "partition stopped serving");
        st.stop_err = Some(err);
        let ids: Vec<TxnId> = st.txns.keys().copied().collect();
        for id in ids {
            st.txns.remove(&id);
            self.locks.release_all(&id);
        }
    }

    /// Stops serving and closes the database, flushing its memtable so the
    /// next owner opens quickly.
    pub async fn close(&self) {
        self.stop(KvError::stopped());
        let _g = self.commit_lock.lock().await;
        if let Err(e) = self.db.close().await {
            tracing::debug!(partition = self.id, err = %e, "close");
        }
    }

    fn txn_locked<'a>(&self, st: &'a mut State, id: &TxnId) -> Result<&'a mut Txn> {
        if let Some(e) = &st.stop_err {
            return Err(e.clone());
        }
        let t = st.txns.get_mut(id).ok_or_else(|| not_open(id))?;
        t.last_seen = Instant::now();
        Ok(t)
    }

    fn abort_locked(&self, st: &mut State, id: &TxnId) {
        st.txns.remove(id);
        self.locks.release_all(id);
    }

    fn abort(&self, id: &TxnId) {
        let mut st = self.state.lock();
        if st.txns.get(id).is_some_and(|t| !t.committing) {
            self.abort_locked(&mut st, id);
        }
    }

    async fn lock(&self, t: TxnRef, key: &[u8], mode: LockMode, wait: Duration) -> Result<()> {
        let res = self
            .locks
            .acquire(t, key, mode, lock_wait(wait), |victim| self.abort(&victim))
            .await;
        if let Err(e) = &res
            && e.kind == Kind::Deadlock
        {
            self.abort(&t.id);
        }
        res
    }

    fn snapshot_locked(&self, t: &mut Txn, stmt: u64) -> u64 {
        if (t.iso == Isolation::ReadCommitted && (!t.has_snap || t.snap_stmt != stmt)) || !t.has_snap {
            t.snapshot_ts = self.visible_ts();
            t.snap_stmt = stmt;
            t.has_snap = true;
        }
        t.snapshot_ts
    }

    pub fn begin(&self, epoch: u64, id: TxnId, iso: Isolation, consistent_snapshot: bool) -> Result<()> {
        self.check_epoch(epoch)?;
        let mut st = self.state.lock();
        if let Some(e) = &st.stop_err {
            return Err(e.clone());
        }
        if st.txns.contains_key(&id) {
            return Ok(());
        }
        if let Some((ts, _)) = st.recent.get(&id) {
            return Err(KvError::new(
                Kind::Internal,
                format!("transaction {} already committed at {ts}", hex::encode(id)),
            ));
        }
        let now = Instant::now();
        let mut t = Txn {
            r: TxnRef { id, began: now },
            iso,
            snapshot_ts: 0,
            has_snap: false,
            snap_stmt: 0,
            layers: vec![],
            bytes: 0,
            last_seen: now,
            committing: false,
            autoinc: HashSet::new(),
        };
        if consistent_snapshot {
            t.snapshot_ts = self.visible_ts();
            t.has_snap = true;
        }
        st.txns.insert(id, t);
        Ok(())
    }

    /// The newest version of uk at or below ts (maybe a delete marker).
    async fn read_version(&self, uk: &[u8], ts: u64) -> Result<Option<Bytes>> {
        let mut it = self
            .db
            .scan_with_options(keys::version_key(uk, ts)..keys::versions_end(uk), &point_opts())
            .await?;
        Ok(it.next().await?.map(|kv| kv.value))
    }

    /// The newest written version of key, durable or not.
    async fn latest(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.read_version(key, u64::MAX).await
    }

    /// Reads keys in the transaction. None means the key does not exist.
    pub async fn get(&self, epoch: u64, id: TxnId, keys: &[Vec<u8>], o: ReadOptions) -> Result<Vec<Option<Vec<u8>>>> {
        self.check_epoch(epoch)?;
        let mut out = vec![None; keys.len()];
        let mut misses = vec![];
        let (tref, ts) = {
            let mut st = self.state.lock();
            let t = self.txn_locked(&mut st, &id)?;
            let ts = if o.lock == LockMode::None {
                self.snapshot_locked(t, o.stmt)
            } else {
                u64::MAX
            };
            for (i, k) in keys.iter().enumerate() {
                match t.lookup(k, o.skip()) {
                    Some(intent) => {
                        if !is_delete(&intent.value) {
                            out[i] = Some(intent.value.clone());
                        }
                    }
                    None => misses.push(i),
                }
            }
            (t.r, ts)
        };
        for i in misses {
            if o.lock != LockMode::None {
                // Lock first: once held, no other transaction can have a
                // commit in flight for this key.
                self.lock(tref, &keys[i], o.lock, o.lock_wait).await?;
            }
            if let Some(v) = self.read_version(&keys[i], ts).await?
                && !is_delete(&v)
            {
                out[i] = Some(v.to_vec());
            }
        }
        Ok(out)
    }

    /// Reads [start, end) in key order, up to limit rows (0 = no limit).
    /// The resume key is where the next page starts, or None at the end.
    pub async fn scan(
        &self,
        epoch: u64,
        id: TxnId,
        start: &[u8],
        end: &[u8],
        limit: usize,
        o: ReadOptions,
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, Option<Vec<u8>>)> {
        self.check_epoch(epoch)?;
        let (tref, ts, intents) = {
            let mut st = self.state.lock();
            let t = self.txn_locked(&mut st, &id)?;
            let ts = if o.lock == LockMode::None {
                self.snapshot_locked(t, o.stmt)
            } else {
                u64::MAX
            };
            let intents: Vec<_> = if t.layers.is_empty() {
                vec![]
            } else {
                t.merged(o.skip())
                    .into_iter()
                    .filter(|(k, _)| k.as_slice() >= start && (end.is_empty() || k.as_slice() < end))
                    .collect()
            };
            (t.r, ts, intents)
        };
        let (rows, own, resume) = self.scan_merged(start, end, limit, ts, intents).await?;
        if o.lock == LockMode::None {
            return Ok((rows, resume));
        }
        // Locking read: lock each row found, then read its latest version; a
        // row deleted while we waited drops out. No gap locks.
        let mut out = Vec::with_capacity(rows.len());
        for (i, (k, v)) in rows.into_iter().enumerate() {
            if own[i] {
                out.push((k, v));
                continue;
            }
            self.lock(tref, &k, o.lock, o.lock_wait).await?;
            if let Some(v) = self.latest(&k).await?
                && !is_delete(&v)
            {
                out.push((k, v.to_vec()));
            }
        }
        Ok((out, resume))
    }

    /// Merges the committed rows visible at ts with the transaction's
    /// intents; own[i] reports whether row i is an intent.
    async fn scan_merged(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
        ts: u64,
        intents: Vec<(Vec<u8>, Intent)>,
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, Vec<bool>, Option<Vec<u8>>)> {
        let lower = keys::versions_start(start).max(keys::DATA_START.to_vec());
        let it = if end.is_empty() {
            self.db.scan_with_options(lower.., &scan_opts()).await?
        } else {
            self.db
                .scan_with_options(lower..keys::versions_start(end), &scan_opts())
                .await?
        };
        let mut committed = Committed { it, ts, decided: None };
        let mut out = vec![];
        let mut own = vec![];
        let mut c = committed.next().await?;
        let mut ii = intents.into_iter().peekable();
        loop {
            let take_intent = match (ii.peek(), &c) {
                (None, None) => break,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (Some((ik, _)), Some((ck, _))) => ik <= ck,
            };
            let (k, v, from_intent) = if take_intent {
                let (k, intent) = ii.next().unwrap();
                if c.as_ref().is_some_and(|(ck, _)| *ck == k) {
                    // our own write replaces the committed row
                    c = committed.next().await?;
                }
                if is_delete(&intent.value) {
                    continue;
                }
                (k, intent.value, true)
            } else {
                let (k, v) = c.take().unwrap();
                c = committed.next().await?;
                (k, v, false)
            };
            out.push((k, v));
            own.push(from_intent);
            if limit > 0 && out.len() >= limit {
                if ii.peek().is_some() || c.is_some() {
                    let mut resume = out.last().unwrap().0.clone();
                    resume.push(0);
                    return Ok((out, own, Some(resume)));
                }
                break;
            }
        }
        Ok((out, own, None))
    }

    /// Adds writes to statement stmt's layer. Every written key is locked
    /// exclusively first; insert-only keys are then checked against the
    /// transaction's own writes and the latest committed version.
    pub async fn mutate(&self, epoch: u64, id: TxnId, stmt: u64, muts: Vec<Mutation>, wait: Duration) -> Result<()> {
        self.check_epoch(epoch)?;
        let tref = {
            let mut st = self.state.lock();
            let t = self.txn_locked(&mut st, &id)?;
            if t.committing {
                return Err(KvError::new(
                    Kind::Internal,
                    format!("transaction {} is committing", hex::encode(id)),
                ));
            }
            t.r
        };
        for m in &muts {
            self.lock(tref, &m.key, LockMode::Exclusive, wait).await?;
        }
        // The latest committed version of each insert-only key; it can't
        // change now that the key is locked.
        let mut latest = HashMap::new();
        for m in muts.iter().filter(|m| m.op == Op::InsertOnly) {
            let v = self.latest(&m.key).await?;
            latest.insert(m.key.clone(), v.is_some_and(|v| !is_delete(&v)));
        }
        let mut st = self.state.lock();
        let cap = self.cfg.intent_cap;
        let t = self.txn_locked(&mut st, &id)?;
        for m in muts {
            let mut intent = Intent {
                value: m.value,
                insert_only: false,
            };
            match m.op {
                Op::Put => {}
                Op::Delete => intent.value = vec![DELETE_MARKER],
                Op::InsertOnly => {
                    intent.insert_only = true;
                    match t.lookup(&m.key, 0) {
                        Some(own) if !is_delete(&own.value) => return Err(KvError::duplicate(&m.key, "duplicate key")),
                        // Re-inserting a row this transaction deleted
                        // overwrites a committed version, if any.
                        Some(_) => intent.insert_only = false,
                        None if latest[&m.key] => return Err(KvError::duplicate(&m.key, "duplicate key")),
                        None => {}
                    }
                }
            }
            if intent.value.len() > MAX_ROW_SIZE {
                return Err(KvError::new(Kind::Internal, "row too large"));
            }
            let l = t.layer(stmt);
            let delta_minus = match l.writes.get(&m.key) {
                Some(prev) => prev.value.len(),
                None => 0,
            };
            let delta_plus = intent.value.len() + if l.writes.contains_key(&m.key) { 0 } else { m.key.len() };
            l.writes.insert(m.key, intent);
            t.bytes = t.bytes + delta_plus - delta_minus;
            if t.bytes > cap {
                return Err(KvError::new(
                    Kind::TxnTooLarge,
                    format!("transaction exceeds {cap} bytes of intents"),
                ));
            }
        }
        Ok(())
    }

    /// Drops every write of statement stmt; its locks stay, as in InnoDB.
    pub fn rollback_statement(&self, epoch: u64, id: TxnId, stmt: u64) -> Result<()> {
        self.check_epoch(epoch)?;
        let mut st = self.state.lock();
        let t = self.txn_locked(&mut st, &id)?;
        if let Some(i) = t.layers.iter().position(|l| l.id == stmt) {
            let l = t.layers.remove(i);
            for (k, intent) in l.writes {
                t.bytes = t.bytes.saturating_sub(k.len() + intent.value.len());
            }
        }
        Ok(())
    }

    /// Aborts the transaction; an unknown id is a no-op.
    pub fn rollback(&self, epoch: u64, id: TxnId) -> Result<()> {
        self.check_epoch(epoch)?;
        self.abort(&id);
        Ok(())
    }

    pub fn heartbeat(&self, epoch: u64, ids: &[TxnId]) -> Result<()> {
        self.check_epoch(epoch)?;
        let mut st = self.state.lock();
        if let Some(e) = &st.stop_err {
            return Err(e.clone());
        }
        let now = Instant::now();
        for id in ids {
            if let Some(t) = st.txns.get_mut(id) {
                t.last_seen = now;
            }
        }
        Ok(())
    }

    /// What happened to a transaction. After a takeover an unknown id is
    /// definitive: SlateDB recovered everything the old owner made durable.
    pub fn txn_status(&self, epoch: u64, id: TxnId) -> Result<(TxnState, u64)> {
        self.check_epoch(epoch)?;
        let st = self.state.lock();
        if let Some((ts, _)) = st.recent.get(&id) {
            return Ok((TxnState::Committed, *ts));
        }
        if st.txns.contains_key(&id) {
            return Ok((TxnState::Active, 0));
        }
        Ok((TxnState::Unknown, 0))
    }

    fn auto_inc_key(&self, table: u32) -> Vec<u8> {
        let mut k = Vec::with_capacity(25);
        k.extend_from_slice(&self.id.to_be_bytes());
        k.extend_from_slice(&TABLE_AUTO_INCREMENT.to_be_bytes());
        k.extend_from_slice(&0u32.to_be_bytes());
        k.push(0x01);
        k.extend_from_slice(&(table as u64).to_be_bytes());
        k
    }

    /// Reserves count values of table's AUTO_INCREMENT counter and returns
    /// the first; at_least raises the counter first. A zero id peeks.
    pub async fn auto_increment(&self, epoch: u64, id: TxnId, table: u32, count: u64, at_least: u64) -> Result<u64> {
        self.check_epoch(epoch)?;
        let loaded = self.state.lock().autoinc.contains_key(&table);
        if !loaded {
            let v = self.latest(&self.auto_inc_key(table)).await?;
            let n = match v.as_deref() {
                Some(b) if b.len() == 8 => u64::from_be_bytes(b.try_into().unwrap()),
                _ => 1,
            };
            self.state.lock().autoinc.entry(table).or_insert(n);
        }
        let mut st = self.state.lock();
        if let Some(e) = &st.stop_err {
            return Err(e.clone());
        }
        let mut next = st.autoinc[&table];
        if id == [0u8; 16] {
            return Ok(next);
        }
        self.txn_locked(&mut st, &id)?.autoinc.insert(table);
        next = next.max(at_least);
        st.autoinc.insert(table, next + count);
        Ok(next)
    }

    /// Commits the transaction with preconditions checked in commit order.
    /// A transaction without writes commits at 0.
    pub async fn commit(&self, epoch: u64, id: TxnId, guards: Vec<Guard>) -> Result<u64> {
        self.check_epoch(epoch)?;
        let t0 = Instant::now();
        let (writes, autoinc) = {
            let mut st = self.state.lock();
            if let Some((ts, _)) = st.recent.get(&id) {
                return Ok(*ts); // a retried Commit returns the original commit_ts
            }
            let t = self.txn_locked(&mut st, &id)?;
            if t.committing {
                return Err(KvError::new(
                    Kind::Internal,
                    format!("transaction {} is already committing", hex::encode(id)),
                ));
            }
            let writes = t.merged(0);
            if writes.is_empty() && t.autoinc.is_empty() {
                self.abort_locked(&mut st, &id); // nothing to write: just free the locks
                return Ok(0);
            }
            t.committing = true;
            (writes, t.autoinc.clone())
        };
        let fail = |e: KvError| {
            let mut st = self.state.lock();
            self.abort_locked(&mut st, &id);
            e
        };

        let ordered = self.commit_lock.lock().await;
        if let Some(e) = self.stopped() {
            return Err(fail(e));
        }
        for g in &guards {
            let cur = self.latest(&g.key).await.map_err(fail)?;
            let cur = cur.filter(|v| !is_delete(v)).unwrap_or_default();
            if cur.as_ref() != g.value.as_slice() {
                return Err(fail(KvError::new(
                    Kind::SchemaChanged,
                    "the table's schema changed during the transaction; try restarting transaction",
                )));
            }
        }
        // Every written key is locked exclusively, so no other commit touched
        // them; re-check insert-only keys anyway.
        for (k, intent) in writes.iter().filter(|(_, i)| i.insert_only) {
            if self.latest(k).await.map_err(fail)?.is_some_and(|v| !is_delete(&v)) {
                return Err(fail(KvError::duplicate(k, "duplicate key at commit")));
            }
            let _ = intent;
        }
        let wall = wall_micros();
        let mut b = WriteBatch::new();
        let ts = {
            let mut st = self.state.lock();
            st.max_commit_ts += 1;
            let ts = st.max_commit_ts;
            for table in &autoinc {
                // saved with the commit, so values are never reused
                b.put(
                    keys::version_key(&self.auto_inc_key(*table), ts),
                    st.autoinc[table].to_be_bytes(),
                );
            }
            ts
        };
        for (k, intent) in &writes {
            b.put(keys::version_key(k, ts), &intent.value);
        }
        let mut rec = ts.to_be_bytes().to_vec();
        rec.extend_from_slice(&wall.to_be_bytes());
        b.put(txn_key(&id), rec);
        b.put(time_key(wall, ts), b"");
        b.put(keys::meta_key(META_MAXTS), ts.to_be_bytes());
        let written = self.db.write(b).await;
        drop(ordered);
        let durable = match written {
            Ok(h) => h.await_durable().await,
            Err(e) => Err(e),
        };
        if let Err(e) = durable {
            // The outcome is unknown to the client (the batch may be durable);
            // this owner can't go on either way.
            let err: KvError = e.into();
            let err = if err.kind == Kind::EpochChanged {
                err
            } else {
                KvError::abandoned()
            };
            self.stop(err.clone());
            return Err(err);
        }
        crate::metrics::COMMIT.observe(t0.elapsed());
        self.visible.fetch_max(ts, Ordering::AcqRel);
        {
            let mut st = self.state.lock();
            st.recent.insert(id, (ts, wall));
            st.txns.remove(&id);
        }
        // Release locks only once the writes are visible.
        self.locks.release_all(&id);
        Ok(ts)
    }

    /// The GC safe point: the oldest snapshot an open transaction may read.
    fn oldest_snapshot(&self) -> u64 {
        let st = self.state.lock();
        st.txns
            .values()
            .filter(|t| t.has_snap)
            .map(|t| t.snapshot_ts)
            .fold(self.visible_ts(), u64::min)
    }

    /// Aborts transactions whose lease ran out and forgets old committed ids.
    fn expire_txns(&self) -> Vec<TxnId> {
        let mut st = self.state.lock();
        let lease = self.cfg.txn_lease;
        let expired: Vec<TxnId> = st
            .txns
            .values()
            .filter(|t| !t.committing && t.last_seen.elapsed() > lease)
            .map(|t| t.r.id)
            .collect();
        for id in &expired {
            tracing::info!(partition = self.id, txn = hex::encode(id), "transaction lease expired");
            self.abort_locked(&mut st, id);
        }
        let cutoff = wall_micros().saturating_sub(self.cfg.txn_id_window.as_micros() as u64);
        let old: Vec<TxnId> = st
            .recent
            .iter()
            .filter(|(_, (_, w))| *w < cutoff)
            .map(|(id, _)| *id)
            .collect();
        for id in &old {
            st.recent.remove(id);
        }
        old
    }

    /// Drops versions no snapshot can read: for each user key, every version
    /// older than the newest one at or below the safe point, and that one too
    /// if it is a delete marker. The deletes need not be durable: versions
    /// that come back after a crash are equally invisible.
    /// Runs MVCC GC now and reports how many versions it dropped.
    pub async fn collect_garbage(&self) -> Result<usize> {
        self.gc_versions(&[]).await
    }

    async fn gc_versions(&self, forget: &[TxnId]) -> Result<usize> {
        let mut b = WriteBatch::new();
        for id in forget {
            b.delete(txn_key(id));
        }
        let safe = self.oldest_snapshot().min(self.restore_safe_point(&mut b).await?);
        let mut dropped = 0;
        if safe > 0 {
            let mut it = self
                .db
                .scan_with_options(keys::DATA_START.to_vec().., &scan_opts())
                .await?;
            let mut cur: Option<Vec<u8>> = None;
            let mut kept_below_safe = false;
            while let Some(kv) = it.next().await? {
                let Some((uk, ts)) = keys::split(&kv.key) else { continue };
                if cur.as_ref() != Some(&uk) {
                    cur = Some(uk);
                    kept_below_safe = false;
                }
                if ts > safe {
                    continue;
                }
                if !kept_below_safe {
                    kept_below_safe = true;
                    if !is_delete(&kv.value) {
                        continue;
                    }
                }
                b.delete(&kv.key);
                dropped += 1;
                if dropped % 10_000 == 0 {
                    self.db.write(std::mem::take(&mut b)).await?;
                }
            }
        }
        if !b.is_empty() {
            self.db.write(b).await?;
        }
        Ok(dropped)
    }
}

impl Partition {
    /// The newest commit_ts a restore no longer needs history before: that
    /// of the last commit at least restore_window old (0 when there is
    /// none). Time records older than that commit are deleted into b.
    async fn restore_safe_point(&self, b: &mut WriteBatch) -> Result<u64> {
        let Some(cutoff) = wall_micros().checked_sub(self.cfg.restore_window.as_micros() as u64) else {
            return Ok(0);
        };
        let start = keys::meta_key(META_TIME);
        let end = time_key(cutoff, u64::MAX);
        let mut it = self.db.scan_with_options(start..=end, &point_opts()).await?;
        let mut last: Option<(Vec<u8>, u64)> = None;
        while let Some(kv) = it.next().await? {
            let Some((_, ts)) = split_time_key(&kv.key) else {
                continue;
            };
            if let Some((k, _)) = last.replace((kv.key.to_vec(), ts)) {
                b.delete(k);
            }
        }
        Ok(last.map_or(0, |(_, ts)| ts))
    }
}

/// Lease expiry and MVCC GC, until the partition stops.
async fn background(p: Weak<Partition>) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let mut last_gc = Instant::now();
    let mut gc_at_ts = 0;
    let mut last_change = Instant::now();
    let mut forget = vec![];
    loop {
        tick.tick().await;
        let Some(p) = p.upgrade() else { return };
        if p.stopped().is_some() {
            return;
        }
        forget.extend(p.expire_txns());
        let vis = p.visible_ts();
        if vis != gc_at_ts {
            last_change = Instant::now();
        }
        // Versions age out of the restore window after the last commit too.
        let aging = last_change.elapsed() < p.cfg.restore_window + 2 * p.cfg.mvcc_gc_interval;
        if (vis != gc_at_ts || aging) && last_gc.elapsed() >= p.cfg.mvcc_gc_interval {
            match p.gc_versions(&forget).await {
                Ok(n) => {
                    forget.clear();
                    tracing::debug!(partition = p.id, dropped = n, "mvcc gc");
                }
                Err(e) => tracing::warn!(partition = p.id, err = %e, "mvcc gc failed"),
            }
            last_gc = Instant::now();
            gc_at_ts = vis;
        }
    }
}

/// Committed rows visible at a timestamp, one per user key.
struct Committed {
    it: slatedb::DbIterator,
    ts: u64,
    decided: Option<Vec<u8>>, // the user key whose visible version was already taken
}

impl Committed {
    async fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        while let Some(kv) = self.it.next().await? {
            let Some((uk, vts)) = keys::split(&kv.key) else {
                continue;
            };
            if self.decided.as_ref() == Some(&uk) || vts > self.ts {
                continue;
            }
            self.decided = Some(uk.clone());
            if !is_delete(&kv.value) {
                return Ok(Some((uk, kv.value.to_vec())));
            }
        }
        Ok(None)
    }
}
