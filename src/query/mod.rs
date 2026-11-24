//! The query language: Kusto-style pipes over steps, with a `match` stage for sequences.
//!
//! ```text
//! where time > ago(7d)
//! | match a: tool == "Bash" and failed
//!     then b: tool in ("Edit", "Write") and file_path contains "test"
//!     then c: tool == "Bash" and not failed
//!     where no (tool in ("Edit", "Write") and not file_path contains "test") between a and c
//! | summarize n = count() by session
//! | top 10 by n
//! ```
//!
//! How it runs: leading `where` stages and each element's test compile against the
//! dictionaries (`plan`), so a filter is evaluated once per distinct value and tool or
//! outcome tests become a 256-entry table over shape codes; sessions without the tools a
//! pattern needs are skipped; the matcher (`matcher`) makes one pass per session over the
//! step tree; later stages (`exec`) work on the matched rows.

pub mod exec;
pub mod lex;
pub mod matcher;
pub mod messages;
pub mod parse;
pub mod plan;
pub mod reference;
pub mod summary;
pub mod value;

use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub struct QError {
    pub at: Option<usize>,
    pub msg: String,
}

impl QError {
    pub fn new(m: impl Into<String>) -> QError {
        QError { at: None, msg: m.into() }
    }
    pub fn at(at: usize, m: impl Into<String>) -> QError {
        QError { at: Some(at), msg: m.into() }
    }
    /// The message with a caret under the position in the query.
    pub fn show(&self, src: &str) -> String {
        match self.at {
            None => self.msg.clone(),
            Some(at) => {
                let at = at.min(src.len());
                let line_start = src[..at].rfind('\n').map(|i| i + 1).unwrap_or(0);
                let line_end = src[at..].find('\n').map(|i| at + i).unwrap_or(src.len());
                let col = src[line_start..at].chars().count();
                format!("{}\n  {}\n  {}^", self.msg, &src[line_start..line_end], " ".repeat(col))
            }
        }
    }
}

impl fmt::Display for QError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl std::error::Error for QError {}

/// Parse and run a query.
pub fn run(db: &crate::db::Db, src: &str, threads: usize) -> Result<exec::Run, QError> {
    let q = parse::parse(src)?;
    exec::run(db, &q, threads)
}
