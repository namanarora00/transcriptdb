//! TranscriptDB: a storage and query engine for agent transcripts.
//!
//! This crate is the storage core: requests are split into messages, each session's
//! messages are kept once as a tree stored main line first in compressed frames, and
//! every request can be rebuilt byte for byte. See `db` for the write and read paths.

#![allow(clippy::needless_range_loop, clippy::should_implement_trait, clippy::type_complexity)]

pub mod blob;
pub mod block;
pub mod bloom;
pub mod cache;
pub mod check;
pub mod claude;
pub mod codec;
pub mod db;
#[cfg(feature = "duckdb")]
pub mod duck;
pub mod h2h;
pub mod index;
pub mod json;
pub mod layout;
pub mod manifest;
pub mod pg;
pub mod query;
pub mod record;
pub mod rule_check;
pub mod rules;
pub mod search_check;
pub mod segment;
pub mod server;
pub mod split;
pub mod stats;
pub mod steps;
pub mod store;
pub mod synth;
pub mod text;
pub mod textindex;
pub mod trace;
pub mod wal;

pub use db::{req_id, req_parts, Db, DropReport, Options, Prepared, Put, PutOutcome, ReadStats, ReqId, Stats, VacuumReport};
pub use store::{Request, Store, StoreOptions};
pub use wal::SyncMode;
