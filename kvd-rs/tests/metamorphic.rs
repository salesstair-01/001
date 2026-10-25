//! A metamorphic test in the style of Pebble's, ported from the Go owner's
//! (internal/kvsuite/meta.go): a seeded generator produces operations with
//! the result each must have, computed by a small model of the transaction
//! semantics, and replays them against a partition under several
//! configurations that must not change any result. Storage events (MVCC GC,
//! and Restart: close the owner and take the partition over again) must be
//! invisible, except that a restart loses open transactions.
//!
//!     KVD_META_SEEDS=200 KVD_META_OPS=1000 cargo test --release --test metamorphic

mod common;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use common::*;
use kvd::locks::{LockMode, TxnId};
use kvd::partition::{Config, Isolation, Mutation, Op, Partition, ReadOptions};
use object_store::ObjectStore;
use rand::Rng;
use rand::rngs::StdRng;

const KEYS: usize = 12;
const LOCK_WAIT: Duration = Duration::from_millis(1); // nothing waits in this single-threaded test

#[derive(Clone, Debug)]
enum OpKind {
    Begin {
        iso: Isolation,
        snap: bool,
    },
    Get {
        keys: Vec<usize>,
        lock: LockMode,
        include: bool,
    },
    Scan {
        lo: usize,
        hi: usize,
        limit: usize,
        lock: LockMode,
        include: bool,
    },
    Mutate(Vec<(Op, usize, String)>),
    NextStmt,
    RollbackStmt,
    Rollback,
    Commit,
    Gc,
    Restart,
}

#[derive(Clone, Debug)]
struct MetaOp {
    txn: usize,
    kind: OpKind,
}

// ---- the model ----------------------------------------------------------------

struct Layer {
    stmt: u64,
    writes: HashMap<usize, Option<String>>, // None = delete
}

struct ModelTxn {
    iso: Isolation,
    stmt: u64,
    snap: Option<BTreeMap<usize, String>>,
    layers: Vec<Layer>,
}

impl ModelTxn {
    fn own(&self, k: usize, include: bool) -> Option<&Option<String>> {
        self.layers
            .iter()
            .rev()
            .filter(|l| include || l.stmt != self.stmt)
            .find_map(|l| l.writes.get(&k))
    }

    fn layer(&mut self) -> &mut Layer {
        if self.layers.last().is_none_or(|l| l.stmt != self.stmt) {
            self.layers.push(Layer {
                stmt: self.stmt,
                writes: HashMap::new(),
            });
        }
        self.layers.last_mut().unwrap()
    }
}

#[derive(Default)]
struct Lock {
    x: Option<usize>,
    s: BTreeSet<usize>,
}

#[derive(Default)]
struct Model {
    committed: BTreeMap<usize, String>,
    txns: BTreeMap<usize, ModelTxn>,
    locks: HashMap<usize, Lock>,
}

fn fmt_val(v: Option<&str>) -> String {
    v.map_or("-".into(), |v| format!("{v:?}"))
}

impl Model {
    fn lock(&mut self, txn: usize, k: usize, mode: LockMode) -> bool {
        let l = self.locks.entry(k).or_default();
        if l.x.is_some_and(|x| x != txn) {
            return false;
        }
        if mode == LockMode::Exclusive {
            if l.s.iter().any(|h| *h != txn) {
                return false;
            }
            l.x = Some(txn);
            return true;
        }
        if l.x != Some(txn) {
            l.s.insert(txn);
        }
        true
    }

    fn unlock_all(&mut self, txn: usize) {
        for l in self.locks.values_mut() {
            if l.x == Some(txn) {
                l.x = None;
            }
            l.s.remove(&txn);
        }
    }

    /// The committed state a read sees: the snapshot, or the latest for a locking read.
    fn base(&mut self, txn: usize, lock: LockMode) -> BTreeMap<usize, String> {
        if lock != LockMode::None {
            return self.committed.clone();
        }
        let committed = self.committed.clone();
        self.txns.get_mut(&txn).unwrap().snap.get_or_insert(committed).clone()
    }

    fn apply(&mut self, op: &MetaOp) -> String {
        let t = op.txn;
        match &op.kind {
            OpKind::Begin { iso, snap } => {
                let mut mt = ModelTxn {
                    iso: *iso,
                    stmt: 1,
                    snap: None,
                    layers: vec![],
                };
                if *snap && *iso == Isolation::RepeatableRead {
                    mt.snap = Some(self.committed.clone());
                }
                self.txns.insert(t, mt);
                "ok".into()
            }
            OpKind::NextStmt => {
                let mt = self.txns.get_mut(&t).unwrap();
                mt.stmt += 1;
                if mt.iso == Isolation::ReadCommitted {
                    mt.snap = None;
                }
                "ok".into()
            }
            OpKind::Get { keys, lock, include } => {
                let mut base = if *lock == LockMode::None {
                    Some(self.base(t, *lock))
                } else {
                    None
                };
                let mut out = vec![];
                for &k in keys {
                    if let Some(v) = self.txns[&t].own(k, *include) {
                        out.push(fmt_val(v.as_deref()));
                        continue;
                    }
                    if *lock != LockMode::None {
                        if !self.lock(t, k, *lock) {
                            return "err:LockTimeout".into();
                        }
                        base = Some(self.committed.clone());
                    }
                    out.push(fmt_val(base.as_ref().unwrap().get(&k).map(String::as_str)));
                }
                out.join(" ")
            }
            OpKind::Scan {
                lo, hi, lock, include, ..
            } => {
                let base = self.base(t, *lock);
                let mut rows = vec![];
                for k in *lo..*hi {
                    if let Some(v) = self.txns[&t].own(k, *include).cloned() {
                        if let Some(v) = v {
                            rows.push(format!("{k}={v:?}"));
                        }
                        continue;
                    }
                    let Some(bv) = base.get(&k) else { continue };
                    if *lock != LockMode::None && !self.lock(t, k, *lock) {
                        return "err:LockTimeout".into();
                    }
                    rows.push(format!("{k}={bv:?}"));
                }
                format!("[{}]", rows.join(" "))
            }
            OpKind::Mutate(muts) => {
                for (_, k, _) in muts {
                    if !self.lock(t, *k, LockMode::Exclusive) {
                        return "err:LockTimeout".into();
                    }
                }
                for (op, k, v) in muts {
                    match op {
                        Op::Put => {
                            self.txns
                                .get_mut(&t)
                                .unwrap()
                                .layer()
                                .writes
                                .insert(*k, Some(v.clone()));
                        }
                        Op::Delete => {
                            self.txns.get_mut(&t).unwrap().layer().writes.insert(*k, None);
                        }
                        Op::InsertOnly => {
                            match self.txns[&t].own(*k, true) {
                                Some(Some(_)) => return "err:DuplicateKey".into(),
                                Some(None) => {}
                                None if self.committed.contains_key(k) => return "err:DuplicateKey".into(),
                                None => {}
                            }
                            self.txns
                                .get_mut(&t)
                                .unwrap()
                                .layer()
                                .writes
                                .insert(*k, Some(v.clone()));
                        }
                    }
                }
                "ok".into()
            }
            OpKind::RollbackStmt => {
                let mt = self.txns.get_mut(&t).unwrap();
                let stmt = mt.stmt;
                if let Some(i) = mt.layers.iter().position(|l| l.stmt == stmt) {
                    mt.layers.remove(i);
                }
                "ok".into()
            }
            OpKind::Rollback => {
                self.txns.remove(&t);
                self.unlock_all(t);
                "ok".into()
            }
            OpKind::Commit => {
                let mt = self.txns.remove(&t).unwrap();
                self.unlock_all(t);
                let mut merged = HashMap::new();
                for l in mt.layers {
                    merged.extend(l.writes);
                }
                if merged.is_empty() {
                    return "ok:empty".into();
                }
                for (k, v) in merged {
                    match v {
                        Some(v) => self.committed.insert(k, v),
                        None => self.committed.remove(&k),
                    };
                }
                "ok:committed".into()
            }
            OpKind::Gc => "ok".into(),
            OpKind::Restart => {
                for id in std::mem::take(&mut self.txns).into_keys() {
                    self.unlock_all(id);
                }
                "ok".into()
            }
        }
    }
}

// ---- the generator ----------------------------------------------------------------

fn lock_mode(r: &mut StdRng) -> LockMode {
    if r.random_range(0..4) == 0 {
        if r.random_bool(0.5) {
            LockMode::Shared
        } else {
            LockMode::Exclusive
        }
    } else {
        LockMode::None
    }
}

fn generate(seed: u64, n: usize) -> (Vec<MetaOp>, Vec<String>) {
    let mut r = rng(seed);
    let mut m = Model::default();
    let (mut ops, mut want) = (vec![], vec![]);
    let mut next_txn = 0;
    while ops.len() < n {
        let open: Vec<usize> = m.txns.keys().copied().collect();
        let w = r.random_range(0..100);
        let op = if open.is_empty() || (w < 8 && open.len() < 4) {
            next_txn += 1;
            let iso = if r.random_bool(0.5) {
                Isolation::RepeatableRead
            } else {
                Isolation::ReadCommitted
            };
            MetaOp {
                txn: next_txn,
                kind: OpKind::Begin {
                    iso,
                    snap: r.random_range(0..4) == 0,
                },
            }
        } else if w < 10 {
            MetaOp {
                txn: 0,
                kind: OpKind::Gc,
            }
        } else if w < 11 {
            MetaOp {
                txn: 0,
                kind: OpKind::Restart,
            }
        } else {
            let txn = open[r.random_range(0..open.len())];
            let lock = lock_mode(&mut r);
            let include = r.random_range(0..3) == 0;
            let kind = match r.random_range(0..100) {
                0..22 => {
                    let keys = (0..1 + r.random_range(0..3)).map(|_| r.random_range(0..KEYS)).collect();
                    OpKind::Get { keys, lock, include }
                }
                22..36 => {
                    let lo = r.random_range(0..KEYS);
                    let hi = lo + 1 + r.random_range(0..KEYS - lo);
                    OpKind::Scan {
                        lo,
                        hi,
                        limit: r.random_range(0..4),
                        lock,
                        include,
                    }
                }
                36..70 => {
                    let muts = (0..1 + r.random_range(0..3))
                        .map(|_| {
                            let op = [Op::Put, Op::Delete, Op::InsertOnly][r.random_range(0..3)];
                            let v = match op {
                                Op::Delete => String::new(),
                                // empty values (secondary-index entries) must read back as present
                                _ if r.random_range(0..8) == 0 => String::new(),
                                _ => format!("v{}", ops.len()),
                            };
                            (op, r.random_range(0..KEYS), v)
                        })
                        .collect();
                    OpKind::Mutate(muts)
                }
                70..80 => OpKind::NextStmt,
                80..84 => OpKind::RollbackStmt,
                84..89 => OpKind::Rollback,
                _ => OpKind::Commit,
            };
            MetaOp { txn, kind }
        };
        want.push(m.apply(&op));
        ops.push(op);
    }
    (ops, want)
}

// ---- running against a partition ---------------------------------------------------

/// User key i: raw bytes, each a prefix of the next ("", "\0", "a", "a\0",
/// "aa", ...), so every version of a key must sort before the longer keys.
fn user_key(i: usize) -> Vec<u8> {
    let mut k = PID.to_be_bytes().to_vec();
    k.extend_from_slice(&1024u32.to_be_bytes());
    k.extend_from_slice(&0u32.to_be_bytes());
    k.extend(std::iter::repeat_n(b'a', i / 2));
    if i % 2 == 1 {
        k.push(0);
    }
    k
}

fn key_index(k: &[u8]) -> usize {
    (0..=KEYS).find(|i| user_key(*i) == k).unwrap_or(usize::MAX)
}

fn err_result(e: kvd::error::KvError) -> String {
    format!("err:{:?}", e.kind)
}

struct Runner {
    store: Arc<dyn ObjectStore>,
    cfg: Config,
    p: Arc<Partition>,
    ids: HashMap<usize, (TxnId, u64)>,
}

impl Runner {
    async fn run(&mut self, op: &MetaOp) -> String {
        let p = self.p.clone();
        let e = p.epoch;
        let (id, stmt) = self.ids.get(&op.txn).copied().unwrap_or(([0; 16], 0));
        let read = |lock, include| ReadOptions {
            stmt,
            include_stmt: include,
            lock,
            lock_wait: LOCK_WAIT,
        };
        match &op.kind {
            OpKind::Begin { iso, snap } => {
                let id = txn_id();
                self.ids.insert(op.txn, (id, 1));
                p.begin(e, id, *iso, *snap).map_or_else(err_result, |_| "ok".into())
            }
            OpKind::NextStmt => {
                self.ids.get_mut(&op.txn).unwrap().1 += 1;
                "ok".into()
            }
            OpKind::Get { keys, lock, include } => {
                let ks: Vec<Vec<u8>> = keys.iter().map(|k| user_key(*k)).collect();
                match p.get(e, id, &ks, read(*lock, *include)).await {
                    Ok(vs) => vs
                        .iter()
                        .map(|v| fmt_val(v.as_deref().map(|b| std::str::from_utf8(b).unwrap())))
                        .collect::<Vec<_>>()
                        .join(" "),
                    Err(err) => err_result(err),
                }
            }
            OpKind::Scan {
                lo,
                hi,
                limit,
                lock,
                include,
            } => {
                // Page through with the limit, as query engines do.
                let (mut start, end) = (user_key(*lo), user_key(*hi));
                let mut rows = vec![];
                loop {
                    let (kvs, resume) = match p.scan(e, id, &start, &end, *limit, read(*lock, *include)).await {
                        Ok(r) => r,
                        Err(err) => return err_result(err),
                    };
                    if *limit > 0 && kvs.len() > *limit {
                        return format!("scan returned {} rows over its limit {limit}", kvs.len());
                    }
                    for (k, v) in kvs {
                        rows.push(format!("{}={:?}", key_index(&k), String::from_utf8(v).unwrap()));
                    }
                    match resume {
                        Some(r) if r <= start => return "scan resume key did not advance".into(),
                        Some(r) => start = r,
                        None => break,
                    }
                }
                format!("[{}]", rows.join(" "))
            }
            OpKind::Mutate(muts) => {
                let ms = muts
                    .iter()
                    .map(|(op, k, v)| Mutation {
                        op: *op,
                        key: user_key(*k),
                        value: v.as_bytes().to_vec(),
                    })
                    .collect();
                p.mutate(e, id, stmt, ms, LOCK_WAIT)
                    .await
                    .map_or_else(err_result, |_| "ok".into())
            }
            OpKind::RollbackStmt => p
                .rollback_statement(e, id, stmt)
                .map_or_else(err_result, |_| "ok".into()),
            OpKind::Rollback => p.rollback(e, id).map_or_else(err_result, |_| "ok".into()),
            OpKind::Commit => match p.commit(e, id, vec![]).await {
                Ok(0) => "ok:empty".into(),
                Ok(_) => "ok:committed".into(),
                Err(err) => err_result(err),
            },
            OpKind::Gc => p.collect_garbage().await.map_or_else(err_result, |_| "ok".into()),
            OpKind::Restart => {
                p.close().await;
                self.p = open_with(&self.store, self.cfg.clone()).await;
                "ok".into()
            }
        }
    }
}

/// Configurations that change how the owner stores data, never what it returns.
fn configs() -> Vec<(&'static str, Config)> {
    vec![
        ("default", cfg()),
        (
            "no-restore-window",
            Config {
                restore_window: Duration::ZERO,
                ..cfg()
            },
        ),
        (
            "tiny-memtable",
            Config {
                l0_sst_bytes: 4 << 10,
                max_unflushed_bytes: 64 << 10,
                restore_window: Duration::ZERO,
                ..cfg()
            },
        ),
        (
            "slow-flush",
            Config {
                flush_interval: Duration::from_millis(50),
                ..cfg()
            },
        ),
    ]
}

fn env(name: &str, default: usize) -> usize {
    std::env::var(name).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

#[tokio::test]
async fn metamorphic() {
    let first = env("KVD_META_SEED", 1) as u64;
    let seeds = env("KVD_META_SEEDS", 8) as u64;
    let n = env("KVD_META_OPS", 300);
    for seed in first..first + seeds {
        let (ops, want) = generate(seed, n);
        for (name, cfg) in configs() {
            let store = mem();
            let p = open_with(&store, cfg.clone()).await;
            let mut r = Runner {
                store,
                cfg,
                p,
                ids: HashMap::new(),
            };
            for (i, op) in ops.iter().enumerate() {
                let got = r.run(op).await;
                if got != want[i] {
                    let hist: Vec<String> = (i.saturating_sub(15)..=i)
                        .map(|j| format!("  {j:3} t{} {:?} -> {}", ops[j].txn, ops[j].kind, want[j]))
                        .collect();
                    panic!(
                        "seed {seed}/{name}, op {i}: t{} {:?}\n  got:  {got}\n  want: {}\nlast operations (with expected results):\n{}\nreplay: KVD_META_SEED={seed} KVD_META_SEEDS=1 KVD_META_OPS={n}",
                        op.txn,
                        op.kind,
                        want[i],
                        hist.join("\n")
                    );
                }
            }
            r.p.close().await;
        }
    }
}
