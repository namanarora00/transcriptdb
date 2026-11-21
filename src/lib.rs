//! TranscriptDB: a storage and query engine for agent transcripts.
//!
//! This crate is the storage core: requests are split into messages, each session's
//! messages are kept once as a tree stored main line first in compressed frames, and
//! every request can be rebuilt byte for byte. See `db` for the write and read paths.

#![allow(clippy::needless_range_loop, clippy::should_implement_trait, clippy::type_complexity)]

pub mod check;
pub mod claude;
pub mod codec;
pub mod db;
pub mod json;
pub mod layout;
pub mod manifest;
pub mod query;
pub mod record;
pub mod segment;
pub mod split;
pub mod steps;
pub mod synth;
pub mod trace;
pub mod wal;

pub use db::{Db, Options, Put, PutOutcome, ReadStats, Stats};
pub use wal::SyncMode;
