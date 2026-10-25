//! KV errors, encoded on the wire exactly as the Go owner encodes them
//! (`internal/kvrpc.toStatus`): code ABORTED, message "kv|Kind|hexkey|msg".

use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Internal,
    LockTimeout,
    DuplicateKey,
    Deadlock,
    EpochChanged,
    TxnTooLarge,
    TxnNotFound,
    Unavailable,
    SchemaChanged,
}

#[derive(Clone, Debug)]
pub struct KvError {
    pub kind: Kind,
    pub msg: String,
    /// The conflicting user key for DuplicateKey.
    pub key: Vec<u8>,
}

pub type Result<T> = std::result::Result<T, KvError>;

impl KvError {
    pub fn new(kind: Kind, msg: impl Into<String>) -> Self {
        KvError {
            kind,
            msg: msg.into(),
            key: Vec::new(),
        }
    }

    pub fn duplicate(key: &[u8], msg: &str) -> Self {
        KvError {
            kind: Kind::DuplicateKey,
            msg: msg.into(),
            key: key.to_vec(),
        }
    }

    pub fn fenced() -> Self {
        Self::new(Kind::EpochChanged, "partition fenced by a newer owner")
    }

    pub fn stopped() -> Self {
        Self::new(Kind::EpochChanged, "partition no longer served by this owner")
    }

    pub fn suspended() -> Self {
        Self::new(Kind::EpochChanged, "partition suspended; resolve the owner again")
    }

    pub fn abandoned() -> Self {
        Self::new(Kind::Unavailable, "object store unreachable; commit outcome unknown")
    }
}

impl fmt::Display for KvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "kv {:?}: {}", self.kind, self.msg)
    }
}

impl std::error::Error for KvError {}

impl From<KvError> for tonic::Status {
    fn from(e: KvError) -> Self {
        tonic::Status::aborted(format!("kv|{:?}|{}|{}", e.kind, hex::encode(&e.key), e.msg))
    }
}

/// Classifies a SlateDB error: a fenced or closed database means this owner
/// no longer serves the partition.
impl From<slatedb::Error> for KvError {
    fn from(e: slatedb::Error) -> Self {
        use slatedb::{CloseReason, ErrorKind};
        match e.kind() {
            ErrorKind::Closed(CloseReason::Fenced) => KvError::fenced(),
            ErrorKind::Closed(_) => KvError::stopped(),
            ErrorKind::Unavailable => KvError::new(Kind::Unavailable, e.to_string()),
            _ => KvError::new(Kind::Internal, e.to_string()),
        }
    }
}
