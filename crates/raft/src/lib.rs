//! # deterrust-raft
//!
//! A from-scratch, **sans-IO** implementation of the Raft consensus algorithm
//! (Ongaro & Ousterhout, *In Search of an Understandable Consensus Algorithm*).
//!
//! The node is a pure state machine: [`RaftNode::step`] consumes one [`Input`]
//! and returns a list of [`Output`] intents. It performs no I/O, reads no clock,
//! and uses no randomness — every source of nondeterminism is delegated to the
//! runtime. That is precisely what lets `deterrust-sim` run an entire cluster
//! inside a single deterministic, replayable event loop.
//!
//! It implements the minimal *correct* subset — leader election, log
//! replication, and commit advancement — including the subtle safety rules that
//! are easy to get wrong (the §5.4.2 commit restriction, vote up-to-dateness,
//! term adoption on every RPC, and conflict truncation). Snapshots and
//! membership changes are intentionally out of scope; see `docs/ARCHITECTURE.md`.
//!
//! [`BugConfig`] can plant classic real-world Raft bugs at runtime so the
//! simulator can prove its harness catches and replays them.

mod message;
mod node;
mod types;

pub use message::{Input, Output, RaftMessage};
pub use node::RaftNode;
pub use types::{BugConfig, ClientId, LogEntry, LogIndex, NodeId, Role, Term};
