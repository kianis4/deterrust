//! Wire messages, inputs, and output intents for the sans-IO Raft node.
//!
//! The node never performs I/O. It consumes [`Input`]s and returns [`Output`]
//! intents that the runtime (real or simulated) realizes. This is what makes the
//! whole cluster runnable inside a single deterministic event loop.

use crate::types::{ClientId, LogEntry, LogIndex, NodeId, Term};

/// Messages exchanged between Raft peers (Figure 2 RPCs).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RaftMessage<C> {
    RequestVote {
        term: Term,
        candidate_id: NodeId,
        last_log_index: LogIndex,
        last_log_term: Term,
    },
    RequestVoteReply {
        term: Term,
        vote_granted: bool,
    },
    AppendEntries {
        term: Term,
        leader_id: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<LogEntry<C>>,
        leader_commit: LogIndex,
    },
    AppendEntriesReply {
        term: Term,
        success: bool,
        /// On success: the highest log index now known to match the leader.
        /// On failure: a hint (the follower's last index) so the leader can back
        /// up `next_index` quickly instead of one entry per round trip.
        match_index: LogIndex,
    },
}

/// Something that happens *to* a node and drives its state machine forward.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Input<C> {
    /// The election timer expired (followers/candidates start an election).
    ElectionTimeout,
    /// The heartbeat timer expired (leaders broadcast `AppendEntries`).
    HeartbeatTick,
    /// A peer message was delivered.
    Deliver { from: NodeId, msg: RaftMessage<C> },
    /// A client wants to replicate a command.
    Propose {
        client: ClientId,
        seq: u64,
        command: C,
    },
}

/// An intent the node asks the runtime to carry out. The node itself stays pure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Output<C> {
    /// Send a message to a peer.
    Send { to: NodeId, msg: RaftMessage<C> },
    /// (Re)arm the randomized election timeout. The runtime picks the jittered
    /// duration from its seeded RNG — keeping all randomness in one place.
    ArmElectionTimer,
    /// (Re)arm the fixed-interval heartbeat timer (leaders only).
    ArmHeartbeatTimer,
    /// A committed entry is ready to apply to the application state machine.
    Apply {
        index: LogIndex,
        term: Term,
        client: ClientId,
        seq: u64,
        command: C,
    },
    /// A non-leader received a proposal; tell the client where the leader is.
    Redirect {
        client: ClientId,
        seq: u64,
        leader_hint: Option<NodeId>,
    },
}
