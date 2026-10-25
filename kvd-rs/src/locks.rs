//! Row locks for one partition, a port of `internal/kv/locks.go`: shared and
//! exclusive locks keyed by user key, FIFO queues, no gap locks, and a
//! wait-for graph checked on every wait whose youngest member is the
//! deadlock victim.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::oneshot;

use crate::error::{Kind, KvError, Result};

pub type TxnId = [u8; 16];

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum LockMode {
    #[default]
    None = 0,
    Shared = 1,
    Exclusive = 2,
}

impl LockMode {
    pub fn from_i32(v: i32) -> Self {
        match v {
            1 => LockMode::Shared,
            2 => LockMode::Exclusive,
            _ => LockMode::None,
        }
    }
}

/// MySQL's default innodb_lock_wait_timeout.
pub const DEFAULT_LOCK_WAIT: Duration = Duration::from_secs(50);

/// Who asks for a lock: the id, and when the transaction began (the
/// youngest transaction on a cycle is the deadlock victim).
#[derive(Clone, Copy, Debug)]
pub struct TxnRef {
    pub id: TxnId,
    pub began: Instant,
}

struct Waiter {
    seq: u64,
    txn: TxnRef,
    mode: LockMode,
    key: Vec<u8>,
    done: Option<oneshot::Sender<Result<()>>>,
}

#[derive(Default)]
struct Entry {
    holders: HashMap<TxnId, LockMode>,
    queue: Vec<TxnId>, // waiting transactions, in arrival order
}

#[derive(Default)]
struct Inner {
    locks: HashMap<Vec<u8>, Entry>,
    held: HashMap<TxnId, HashMap<Vec<u8>, LockMode>>,
    waiting: HashMap<TxnId, Waiter>,
    seq: u64,
}

#[derive(Default)]
pub struct LockTable {
    inner: Mutex<Inner>,
}

fn compatible(a: LockMode, b: LockMode) -> bool {
    a == LockMode::Shared && b == LockMode::Shared
}

fn deadlock() -> KvError {
    KvError::new(
        Kind::Deadlock,
        "Deadlock found when trying to get lock; try restarting transaction",
    )
}

fn younger(a: &TxnRef, b: &TxnRef) -> bool {
    if a.began != b.began {
        return a.began > b.began;
    }
    a.id > b.id
}

impl Inner {
    fn grantable(&self, key: &[u8], id: &TxnId, mode: LockMode, ignore_queue: bool) -> bool {
        let Some(e) = self.locks.get(key) else { return true };
        if e.holders.iter().any(|(h, m)| h != id && !compatible(*m, mode)) {
            return false;
        }
        if ignore_queue {
            return true;
        }
        // FIFO: don't overtake earlier waiters with a conflicting request.
        !e.queue
            .iter()
            .any(|q| q != id && self.waiting.get(q).is_some_and(|w| !compatible(w.mode, mode)))
    }

    fn grant(&mut self, key: &[u8], id: TxnId, mode: LockMode) {
        let e = self.locks.entry(key.to_vec()).or_default();
        let h = e.holders.entry(id).or_default();
        if *h < mode {
            *h = mode;
        }
        let h = self.held.entry(id).or_default().entry(key.to_vec()).or_default();
        if *h < mode {
            *h = mode;
        }
    }

    fn gc(&mut self, key: &[u8]) {
        if self
            .locks
            .get(key)
            .is_some_and(|e| e.holders.is_empty() && e.queue.is_empty())
        {
            self.locks.remove(key);
        }
    }

    /// Removes txn's waiter (if it is the one numbered seq, or any with None).
    fn remove_waiter(&mut self, id: &TxnId, seq: Option<u64>) -> Option<Waiter> {
        if seq.is_some_and(|s| self.waiting.get(id).is_none_or(|w| w.seq != s)) {
            return None;
        }
        let w = self.waiting.remove(id)?;
        if let Some(e) = self.locks.get_mut(&w.key) {
            e.queue.retain(|q| q != id);
        }
        self.gc(&w.key);
        Some(w)
    }

    /// Grants queued requests on key that have become compatible, in order.
    fn wake(&mut self, key: &[u8]) {
        loop {
            let Some(&next) = self.locks.get(key).and_then(|e| e.queue.first()) else {
                break;
            };
            let Some(mode) = self.waiting.get(&next).map(|w| w.mode) else {
                self.locks.get_mut(key).unwrap().queue.remove(0);
                continue;
            };
            if !self.grantable(key, &next, mode, true) {
                break;
            }
            self.locks.get_mut(key).unwrap().queue.remove(0);
            let mut w = self.waiting.remove(&next).unwrap();
            self.grant(key, next, mode);
            if let Some(tx) = w.done.take() {
                let _ = tx.send(Ok(()));
            }
        }
        self.gc(key);
    }

    fn release(&mut self, id: &TxnId) {
        if let Some(mut w) = self.remove_waiter(id, None)
            && let Some(tx) = w.done.take()
        {
            let _ = tx.send(Err(KvError::new(
                Kind::TxnNotFound,
                "transaction aborted while waiting for a lock",
            )));
        }
        if let Some(keys) = self.held.remove(id) {
            for k in keys.keys() {
                if let Some(e) = self.locks.get_mut(k) {
                    e.holders.remove(id);
                }
                self.wake(k);
            }
        }
    }

    /// The transactions w waits for: conflicting holders that themselves
    /// wait (others can't be on a cycle) and conflicting waiters ahead of it.
    fn blockers(&self, w: &Waiter) -> Vec<TxnRef> {
        let Some(e) = self.locks.get(&w.key) else { return vec![] };
        let mut seen = HashSet::new();
        let mut out = vec![];
        for (h, m) in &e.holders {
            if *h != w.txn.id
                && !compatible(*m, w.mode)
                && seen.insert(*h)
                && let Some(hw) = self.waiting.get(h)
            {
                out.push(hw.txn);
            }
        }
        for q in &e.queue {
            if *q == w.txn.id {
                break;
            }
            if let Some(qw) = self.waiting.get(q)
                && !compatible(qw.mode, w.mode)
                && seen.insert(*q)
            {
                out.push(qw.txn);
            }
        }
        out
    }

    /// Looks for a wait-for cycle through start and returns its youngest
    /// transaction, the victim.
    fn find_deadlock(&self, start: TxnRef) -> Option<TxnRef> {
        fn dfs(inner: &Inner, t: TxnRef, path: &mut Vec<TxnRef>, visited: &mut HashSet<TxnId>) -> Option<Vec<TxnRef>> {
            if let Some(i) = path.iter().position(|p| p.id == t.id) {
                return Some(path[i..].to_vec());
            }
            if !visited.insert(t.id) {
                return None;
            }
            let w = inner.waiting.get(&t.id)?;
            path.push(t);
            for b in inner.blockers(w) {
                if let Some(c) = dfs(inner, b, path, visited) {
                    return Some(c);
                }
            }
            path.pop();
            None
        }
        let cycle = dfs(self, start, &mut vec![], &mut HashSet::new())?;
        let mut victim = cycle[0];
        for t in &cycle[1..] {
            if younger(t, &victim) {
                victim = *t;
            }
        }
        Some(victim)
    }
}

impl LockTable {
    /// Takes mode on key for txn, waiting up to timeout. A transaction picked
    /// as another wait's deadlock victim is handed to on_victim (which aborts
    /// it) after its locks are released.
    pub async fn acquire(
        &self,
        txn: TxnRef,
        key: &[u8],
        mode: LockMode,
        timeout: Duration,
        on_victim: impl FnOnce(TxnId),
    ) -> Result<()> {
        if mode == LockMode::None {
            return Ok(());
        }
        let (rx, seq) = {
            let mut g = self.inner.lock();
            let cur = g
                .locks
                .get(key)
                .and_then(|e| e.holders.get(&txn.id))
                .copied()
                .unwrap_or_default();
            if cur >= mode {
                return Ok(());
            }
            // A re-entrant upgrade or a free lock is granted at once; an
            // upgrade does not queue behind waiters, which would only
            // deadlock with itself.
            if g.grantable(key, &txn.id, mode, cur != LockMode::None) {
                g.grant(key, txn.id, mode);
                return Ok(());
            }
            if g.waiting.contains_key(&txn.id) {
                return Err(KvError::new(Kind::Internal, "transaction already waits for a lock"));
            }
            let (tx, rx) = oneshot::channel();
            g.seq += 1;
            let seq = g.seq;
            g.locks.entry(key.to_vec()).or_default().queue.push(txn.id);
            g.waiting.insert(
                txn.id,
                Waiter {
                    seq,
                    txn,
                    mode,
                    key: key.to_vec(),
                    done: Some(tx),
                },
            );
            if let Some(victim) = g.find_deadlock(txn) {
                if victim.id == txn.id {
                    g.remove_waiter(&txn.id, Some(seq));
                    g.wake(key);
                    return Err(deadlock());
                }
                if let Some(mut vw) = g.remove_waiter(&victim.id, None)
                    && let Some(tx) = vw.done.take()
                {
                    let _ = tx.send(Err(deadlock()));
                }
                g.release(&victim.id);
                g.wake(key);
                drop(g);
                on_victim(victim.id);
            }
            (rx, seq)
        };
        let mut rx = rx;
        match tokio::time::timeout(timeout, &mut rx).await {
            Ok(Ok(res)) => return res,
            Ok(Err(_)) => return Err(KvError::new(Kind::TxnNotFound, "lock wait cancelled")),
            Err(_) => {}
        }
        let mut g = self.inner.lock();
        if let Ok(res) = rx.try_recv() {
            return res; // granted as the timer fired
        }
        g.remove_waiter(&txn.id, Some(seq));
        g.wake(key);
        Err(KvError::new(
            Kind::LockTimeout,
            "Lock wait timeout exceeded; try restarting transaction",
        ))
    }

    /// Drops every lock txn holds or waits for.
    pub fn release_all(&self, id: &TxnId) {
        self.inner.lock().release(id);
    }

    #[cfg(test)]
    pub fn holds(&self, id: &TxnId, key: &[u8], mode: LockMode) -> bool {
        self.inner
            .lock()
            .held
            .get(id)
            .and_then(|h| h.get(key))
            .is_some_and(|m| *m >= mode)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn txn(n: u8, began: Instant) -> TxnRef {
        let mut id = [0u8; 16];
        id[15] = n;
        TxnRef { id, began }
    }

    #[tokio::test]
    async fn shared_exclusive_and_timeout() {
        let lt = LockTable::default();
        let now = Instant::now();
        let (a, b) = (txn(1, now), txn(2, now));
        lt.acquire(a, b"k", LockMode::Shared, DEFAULT_LOCK_WAIT, |_| {})
            .await
            .unwrap();
        lt.acquire(b, b"k", LockMode::Shared, DEFAULT_LOCK_WAIT, |_| {})
            .await
            .unwrap();
        let err = lt
            .acquire(b, b"k", LockMode::Exclusive, Duration::from_millis(30), |_| {})
            .await
            .unwrap_err();
        assert_eq!(err.kind, Kind::LockTimeout);
        lt.release_all(&a.id);
        lt.acquire(b, b"k", LockMode::Exclusive, Duration::from_millis(30), |_| {})
            .await
            .unwrap();
        assert!(lt.holds(&b.id, b"k", LockMode::Exclusive));
    }

    #[tokio::test]
    async fn waiter_granted_on_release() {
        let lt = Arc::new(LockTable::default());
        let now = Instant::now();
        let (a, b) = (txn(1, now), txn(2, now));
        lt.acquire(a, b"k", LockMode::Exclusive, DEFAULT_LOCK_WAIT, |_| {})
            .await
            .unwrap();
        let lt2 = lt.clone();
        let h = tokio::spawn(async move {
            lt2.acquire(b, b"k", LockMode::Exclusive, DEFAULT_LOCK_WAIT, |_| {})
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        lt.release_all(&a.id);
        h.await.unwrap().unwrap();
        assert!(lt.holds(&b.id, b"k", LockMode::Exclusive));
    }

    #[tokio::test]
    async fn deadlock_picks_youngest() {
        let lt = Arc::new(LockTable::default());
        let now = Instant::now();
        let old = txn(1, now);
        let young = txn(2, now + Duration::from_millis(1));
        lt.acquire(old, b"x", LockMode::Exclusive, DEFAULT_LOCK_WAIT, |_| {})
            .await
            .unwrap();
        lt.acquire(young, b"y", LockMode::Exclusive, DEFAULT_LOCK_WAIT, |_| {})
            .await
            .unwrap();
        let lt2 = lt.clone();
        let h = tokio::spawn(async move {
            lt2.acquire(young, b"x", LockMode::Exclusive, DEFAULT_LOCK_WAIT, |_| {})
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        let mut victims = vec![];
        lt.acquire(old, b"y", LockMode::Exclusive, DEFAULT_LOCK_WAIT, |v| victims.push(v))
            .await
            .unwrap();
        assert_eq!(victims, vec![young.id]);
        assert_eq!(h.await.unwrap().unwrap_err().kind, Kind::Deadlock);
    }
}
