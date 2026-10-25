//! Point-in-time restore on the SlateDB layout (`internal/kv/restore.go` is
//! the Go owner's). A partition keeps every MVCC version for its restore
//! window, and each commit leaves a record of when it happened
//! (`partition::time_key`). Restoring partition src as of time T into dst:
//! find the last commit at or before T, read src through a SlateDB reader
//! (which holds a checkpoint and does not fence the live owner), and write
//! the newest version of every key at or below that commit into a fresh
//! database for dst, with the partition id in each key rewritten (keys
//! start with it; values never hold it).

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use object_store::ObjectStore;
use object_store::path::Path;
use slatedb::{Db, DbReader, WriteBatch};
use tokio_stream::StreamExt;

use crate::error::{Kind, KvError, Result};
use crate::keys;
use crate::partition::{DELETE_MARKER, split_time_key, time_key};

fn err(msg: impl Into<String>) -> KvError {
    KvError::new(Kind::Internal, msg)
}

fn micros(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).unwrap_or_default().as_micros() as u64
}

pub fn time_of(micros: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_micros(micros)
}

fn scan_opts() -> slatedb::config::ScanOptions {
    slatedb::config::ScanOptions {
        read_ahead_bytes: 1 << 20,
        max_fetch_tasks: 4,
        ..Default::default()
    }
}

/// The times partition pid can be restored to: from the first commit its
/// history still covers to its last commit. None if it has no commits.
pub async fn restore_points(store: Arc<dyn ObjectStore>, pid: u64) -> Result<Option<(SystemTime, SystemTime)>> {
    let r = DbReader::builder(format!("p/{pid}"), store).build().await?;
    let start = keys::meta_key(crate::partition::META_TIME);
    let end = time_key(u64::MAX, u64::MAX);
    let mut it = r.scan_with_options(start..=end, &scan_opts()).await?;
    let (mut first, mut last) = (None, None);
    while let Some(kv) = it.next().await? {
        if let Some((wall, _)) = split_time_key(&kv.key) {
            first.get_or_insert(wall);
            last = Some(wall);
        }
    }
    drop(it);
    r.close().await?;
    Ok(first.zip(last).map(|(f, l)| (time_of(f), time_of(l))))
}

pub struct Restored {
    pub rows: usize,
    pub commit_ts: u64,
    pub commit_time: SystemTime,
}

/// Writes partition dst as partition src was at time at. dst must never
/// have been used.
pub async fn restore(store: Arc<dyn ObjectStore>, src: u64, dst: u64, at: SystemTime) -> Result<Restored> {
    if src == dst {
        return Err(err("source and destination are the same partition"));
    }
    let dst_path = Path::from(format!("p/{dst}"));
    if let Some(o) = store.list(Some(&dst_path)).next().await {
        let o = o.map_err(|e| err(e.to_string()))?;
        return Err(err(format!("partition {dst} is in use ({} exists)", o.location)));
    }
    let r = DbReader::builder(format!("p/{src}"), store.clone()).build().await?;

    // The last commit at or before at; the first time record bounds the window.
    let at_us = micros(at);
    let start = keys::meta_key(crate::partition::META_TIME);
    let mut it = r
        .scan_with_options(start..=time_key(at_us, u64::MAX), &scan_opts())
        .await?;
    let mut found = None;
    while let Some(kv) = it.next().await? {
        if let Some((wall, ts)) = split_time_key(&kv.key) {
            found = Some((wall, ts));
        }
    }
    drop(it);
    let Some((wall, ts)) = found else {
        r.close().await?;
        return Err(err(format!(
            "partition {src} has no commit at or before {} within its restore window",
            humantime::format_rfc3339_micros(at)
        )));
    };

    let db = Db::builder(format!("p/{dst}"), store).build().await?;
    let src_prefix = src.to_be_bytes();
    let dst_prefix = dst.to_be_bytes();
    let mut it = r.scan_with_options(keys::DATA_START.to_vec().., &scan_opts()).await?;
    let mut b = WriteBatch::new();
    let mut batch_bytes = 0;
    let mut rows = 0;
    let mut decided: Option<Vec<u8>> = None;
    while let Some(kv) = it.next().await? {
        let Some((uk, vts)) = keys::split(&kv.key) else {
            continue;
        };
        if vts > ts || decided.as_ref() == Some(&uk) {
            continue;
        }
        decided = Some(uk.clone());
        if kv.value.len() == 1 && kv.value[0] == DELETE_MARKER {
            continue;
        }
        let Some(rest) = uk.strip_prefix(&src_prefix[..]) else {
            return Err(err(format!(
                "key {} does not belong to partition {src}",
                hex::encode(&uk)
            )));
        };
        let mut nk = dst_prefix.to_vec();
        nk.extend_from_slice(rest);
        batch_bytes += nk.len() + kv.value.len();
        b.put(keys::version_key(&nk, 1), &kv.value);
        rows += 1;
        if batch_bytes > 4 << 20 {
            db.write(std::mem::take(&mut b)).await?;
            batch_bytes = 0;
        }
    }
    drop(it);
    b.put(keys::meta_key(b"maxts"), 1u64.to_be_bytes());
    db.write(b).await?.await_durable().await?;
    db.close().await?;
    r.close().await?;
    Ok(Restored {
        rows,
        commit_ts: ts,
        commit_time: time_of(wall),
    })
}
