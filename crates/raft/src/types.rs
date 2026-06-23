//! Core identifier and log types shared across the Raft state machine.

/// A Raft server identifier. Small integers in this implementation.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct NodeId(pub u64);

/// A client identifier. Used to route replies and to deduplicate retried
/// requests (linearizable exactly-once semantics live in the application layer).
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct ClientId(pub u64);

/// Raft term number. Monotonically increasing logical clock for elections.
pub type Term = u64;

/// 1-based index into the replicated log. Index `0` means "before the log".
pub type LogIndex = u64;

/// The role a node currently believes it holds.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

/// One replicated log entry. `command` is opaque to Raft (generic `C`); the
/// `client`/`seq` metadata travels with it so the application can deduplicate
/// retried client requests and route replies once the entry is applied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogEntry<C> {
    pub term: Term,
    pub client: ClientId,
    pub seq: u64,
    pub command: C,
}

/// Runtime-toggleable correctness bugs. All default to `false` (correct Raft).
///
/// These exist so the deterministic simulator can *plant* a known safety
/// violation and prove the harness catches and replays it. Each one corresponds
/// to a classic real-world Raft mistake.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BugConfig {
    /// Leader advances `commit_index` once a *single* follower acks, instead of
    /// requiring a majority. Causes acknowledged writes to be lost when the
    /// minority leader is partitioned away. (§5.3 / §5.4 majority rule.)
    pub commit_on_single_ack: bool,
    /// Follower accepts `AppendEntries` from a leader whose term is *older* than
    /// its own. Causes split brain / divergence. (Figure 2, AppendEntries rule 1.)
    pub accept_stale_append: bool,
    /// Candidate grants a vote without checking the requester's log is at least
    /// as up-to-date. Lets a node missing committed entries win an election and
    /// overwrite them — a Leader Completeness violation. (§5.4.1.)
    pub vote_without_log_check: bool,
}
