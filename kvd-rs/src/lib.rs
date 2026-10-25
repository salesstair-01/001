//! The KV owner on SlateDB: the library behind the `kvd` binary
//! (src/main.rs), and what the tests in tests/ exercise directly.

pub mod agent;
pub mod error;
pub mod keys;
pub mod localstore;
pub mod locks;
pub mod metrics;
pub mod node;
pub mod partition;
pub mod restore;
pub mod service;

pub mod pb {
    tonic::include_proto!("s2z.kv");

    pub mod dir {
        tonic::include_proto!("s2z.directory");
    }
}
