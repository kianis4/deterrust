//! The Raft consensus state machine — pure, deterministic, I/O-free.
//!
//! [`RaftNode::step`] is the entire surface: feed it one [`Input`], get back a
//! list of [`Output`] intents. It never blocks, never reads a clock, never
//! touches the network. Election-timeout randomness is delegated to the runtime
//! via [`Output::ArmElectionTimer`], so the node is a deterministic function of
//! its inputs.

use std::collections::{HashMap, HashSet};

use crate::message::{Input, Output, RaftMessage};
use crate::types::{BugConfig, ClientId, LogEntry, LogIndex, NodeId, Role, Term};

/// A single Raft server.
pub struct RaftNode<C> {
    id: NodeId,
    peers: Vec<NodeId>,

    // --- persistent state (would be on stable storage in production) ---
    current_term: Term,
    voted_for: Option<NodeId>,
    /// 1-indexed log. `log[0]` is index 1. Index 0 means "empty / before start".
    log: Vec<LogEntry<C>>,

    // --- volatile state (all nodes) ---
    role: Role,
    commit_index: LogIndex,
    last_applied: LogIndex,
    leader_id: Option<NodeId>,

    // --- volatile state (candidate) ---
    votes_received: HashSet<NodeId>,

    // --- volatile state (leader) ---
    next_index: HashMap<NodeId, LogIndex>,
    match_index: HashMap<NodeId, LogIndex>,

    bug: BugConfig,
}

impl<C: Clone> RaftNode<C> {
    /// Create a follower. `peers` must NOT include `id`.
    pub fn new(id: NodeId, peers: Vec<NodeId>, bug: BugConfig) -> Self {
        Self {
            id,
            peers,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            role: Role::Follower,
            commit_index: 0,
            last_applied: 0,
            leader_id: None,
            votes_received: HashSet::new(),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            bug,
        }
    }

    // ---- read-only accessors (used by the runtime and invariant monitors) ----

    pub fn id(&self) -> NodeId {
        self.id
    }
    pub fn role(&self) -> Role {
        self.role
    }
    pub fn current_term(&self) -> Term {
        self.current_term
    }
    pub fn commit_index(&self) -> LogIndex {
        self.commit_index
    }
    pub fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }
    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }
    pub fn last_log_index(&self) -> LogIndex {
        self.log.len() as LogIndex
    }
    /// Term of the entry at `index` (0 for index 0 or out of range).
    pub fn term_at(&self, index: LogIndex) -> Term {
        if index == 0 || index > self.last_log_index() {
            0
        } else {
            self.log[(index - 1) as usize].term
        }
    }
    /// The committed entry at `index`, if present.
    pub fn entry_at(&self, index: LogIndex) -> Option<&LogEntry<C>> {
        if index == 0 || index > self.last_log_index() {
            None
        } else {
            Some(&self.log[(index - 1) as usize])
        }
    }

    fn cluster_size(&self) -> usize {
        self.peers.len() + 1
    }
    fn majority(&self) -> usize {
        self.cluster_size() / 2 + 1
    }
    fn last_log_term(&self) -> Term {
        self.term_at(self.last_log_index())
    }

    // ---- the single entry point ----

    /// Drive the state machine with one input; returns intents for the runtime.
    pub fn step(&mut self, input: Input<C>) -> Vec<Output<C>> {
        let mut out = Vec::new();
        match input {
            Input::ElectionTimeout => self.on_election_timeout(&mut out),
            Input::HeartbeatTick => self.on_heartbeat_tick(&mut out),
            Input::Deliver { from, msg } => self.on_message(from, msg, &mut out),
            Input::Propose {
                client,
                seq,
                command,
            } => self.on_propose(client, seq, command, &mut out),
        }
        self.flush_apply(&mut out);
        out
    }

    // ---- timers ----

    fn on_election_timeout(&mut self, out: &mut Vec<Output<C>>) {
        if self.role == Role::Leader {
            return; // leaders keep authority via heartbeats; they never time out
        }
        // Become a candidate and start a new election (§5.2).
        self.current_term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.id);
        self.leader_id = None;
        self.votes_received.clear();
        self.votes_received.insert(self.id);
        out.push(Output::ArmElectionTimer); // restart timer for the next round

        if self.peers.is_empty() {
            self.become_leader(out); // single-node cluster: instant majority
            return;
        }
        let (lli, llt) = (self.last_log_index(), self.last_log_term());
        for &peer in &self.peers {
            out.push(Output::Send {
                to: peer,
                msg: RaftMessage::RequestVote {
                    term: self.current_term,
                    candidate_id: self.id,
                    last_log_index: lli,
                    last_log_term: llt,
                },
            });
        }
    }

    fn on_heartbeat_tick(&mut self, out: &mut Vec<Output<C>>) {
        if self.role != Role::Leader {
            return;
        }
        for i in 0..self.peers.len() {
            let peer = self.peers[i];
            self.send_append_entries(peer, out);
        }
        out.push(Output::ArmHeartbeatTimer);
    }

    // ---- leadership transitions ----

    fn become_leader(&mut self, out: &mut Vec<Output<C>>) {
        self.role = Role::Leader;
        self.leader_id = Some(self.id);
        let next = self.last_log_index() + 1;
        self.next_index.clear();
        self.match_index.clear();
        for &peer in &self.peers {
            self.next_index.insert(peer, next);
            self.match_index.insert(peer, 0);
        }
        // Establish authority immediately and start heartbeating.
        for i in 0..self.peers.len() {
            let peer = self.peers[i];
            self.send_append_entries(peer, out);
        }
        out.push(Output::ArmHeartbeatTimer);
        // A single-node cluster can commit right away.
        self.maybe_advance_commit();
    }

    fn step_down(&mut self, term: Term, out: &mut Vec<Output<C>>) {
        // Adopt a newer term and revert to follower (Figure 2, "Rules for Servers").
        self.current_term = term;
        self.role = Role::Follower;
        self.voted_for = None;
        self.leader_id = None;
        self.votes_received.clear();
        out.push(Output::ArmElectionTimer);
    }

    // ---- client proposals ----

    fn on_propose(&mut self, client: ClientId, seq: u64, command: C, out: &mut Vec<Output<C>>) {
        if self.role != Role::Leader {
            out.push(Output::Redirect {
                client,
                seq,
                leader_hint: self.leader_id,
            });
            return;
        }
        self.log.push(LogEntry {
            term: self.current_term,
            client,
            seq,
            command,
        });
        // Replicate eagerly to shorten latency; heartbeats also carry entries.
        for i in 0..self.peers.len() {
            let peer = self.peers[i];
            self.send_append_entries(peer, out);
        }
        // In a single-node cluster the new entry is immediately committable.
        self.maybe_advance_commit();
    }

    // ---- replication ----

    fn send_append_entries(&self, to: NodeId, out: &mut Vec<Output<C>>) {
        let next = *self.next_index.get(&to).unwrap_or(&(self.last_log_index() + 1));
        let prev_log_index = next.saturating_sub(1);
        let prev_log_term = self.term_at(prev_log_index);
        let entries: Vec<LogEntry<C>> = if next <= self.last_log_index() {
            self.log[(next - 1) as usize..].to_vec()
        } else {
            Vec::new()
        };
        out.push(Output::Send {
            to,
            msg: RaftMessage::AppendEntries {
                term: self.current_term,
                leader_id: self.id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            },
        });
    }

    // ---- message handling ----

    fn on_message(&mut self, from: NodeId, msg: RaftMessage<C>, out: &mut Vec<Output<C>>) {
        // Rule for all servers: if the message term is newer, adopt it and step
        // down BEFORE processing the body (Figure 2).
        let msg_term = match &msg {
            RaftMessage::RequestVote { term, .. }
            | RaftMessage::RequestVoteReply { term, .. }
            | RaftMessage::AppendEntries { term, .. }
            | RaftMessage::AppendEntriesReply { term, .. } => *term,
        };
        if msg_term > self.current_term {
            self.step_down(msg_term, out);
        }

        match msg {
            RaftMessage::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => self.handle_request_vote(term, candidate_id, last_log_index, last_log_term, out),
            RaftMessage::RequestVoteReply { term, vote_granted } => {
                self.handle_vote_reply(from, term, vote_granted, out)
            }
            RaftMessage::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => self.handle_append_entries(
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                out,
            ),
            RaftMessage::AppendEntriesReply {
                term,
                success,
                match_index,
            } => self.handle_append_reply(from, term, success, match_index, out),
        }
    }

    fn handle_request_vote(
        &mut self,
        term: Term,
        candidate_id: NodeId,
        last_log_index: LogIndex,
        last_log_term: Term,
        out: &mut Vec<Output<C>>,
    ) {
        let mut grant = false;
        if term >= self.current_term {
            let log_ok = self.bug.vote_without_log_check
                || (last_log_term, last_log_index) >= (self.last_log_term(), self.last_log_index());
            let can_vote = self.voted_for.is_none() || self.voted_for == Some(candidate_id);
            if can_vote && log_ok {
                grant = true;
                self.voted_for = Some(candidate_id);
                out.push(Output::ArmElectionTimer); // granting a vote resets our timer
            }
        }
        out.push(Output::Send {
            to: candidate_id,
            msg: RaftMessage::RequestVoteReply {
                term: self.current_term,
                vote_granted: grant,
            },
        });
    }

    fn handle_vote_reply(
        &mut self,
        from: NodeId,
        term: Term,
        granted: bool,
        out: &mut Vec<Output<C>>,
    ) {
        if self.role != Role::Candidate || term != self.current_term {
            return; // stale reply or no longer campaigning
        }
        if granted {
            self.votes_received.insert(from);
            if self.votes_received.len() >= self.majority() {
                self.become_leader(out);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_append_entries(
        &mut self,
        term: Term,
        leader_id: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<LogEntry<C>>,
        leader_commit: LogIndex,
        out: &mut Vec<Output<C>>,
    ) {
        // Reject a stale-term leader (unless the planted bug disables this check).
        if term < self.current_term && !self.bug.accept_stale_append {
            out.push(Output::Send {
                to: leader_id,
                msg: RaftMessage::AppendEntriesReply {
                    term: self.current_term,
                    success: false,
                    match_index: 0,
                },
            });
            return;
        }

        // Valid current-term leader: become/stay follower and reset election timer.
        self.role = Role::Follower;
        self.leader_id = Some(leader_id);
        out.push(Output::ArmElectionTimer);

        // Log-consistency check (§5.3): we must already hold prev_log_index@prev_log_term.
        if prev_log_index > self.last_log_index() || self.term_at(prev_log_index) != prev_log_term {
            let hint = self.last_log_index().min(prev_log_index);
            out.push(Output::Send {
                to: leader_id,
                msg: RaftMessage::AppendEntriesReply {
                    term: self.current_term,
                    success: false,
                    match_index: hint,
                },
            });
            return;
        }

        // Append, truncating on conflict (Figure 2, AppendEntries rules 3 & 4).
        let mut idx = prev_log_index;
        for e in entries {
            idx += 1;
            if idx <= self.last_log_index() {
                if self.term_at(idx) != e.term {
                    self.log.truncate((idx - 1) as usize); // drop conflict + everything after
                    self.log.push(e);
                }
                // else: entry already present and matching — idempotent, skip.
            } else {
                self.log.push(e);
            }
        }
        let index_of_last_new = idx;

        // Advance commit to the leader's, bounded by what we actually hold.
        if leader_commit > self.commit_index {
            self.commit_index = leader_commit.min(index_of_last_new);
        }

        out.push(Output::Send {
            to: leader_id,
            msg: RaftMessage::AppendEntriesReply {
                term: self.current_term,
                success: true,
                match_index: index_of_last_new,
            },
        });
    }

    fn handle_append_reply(
        &mut self,
        from: NodeId,
        term: Term,
        success: bool,
        match_index: LogIndex,
        out: &mut Vec<Output<C>>,
    ) {
        if self.role != Role::Leader || term != self.current_term {
            return;
        }
        if success {
            let m = self.match_index.entry(from).or_insert(0);
            *m = (*m).max(match_index);
            self.next_index.insert(from, *m + 1);
            self.maybe_advance_commit();
            // Keep streaming if the follower is still behind.
            if *self.next_index.get(&from).unwrap_or(&0) <= self.last_log_index() {
                self.send_append_entries(from, out);
            }
        } else {
            // Back up using the follower's hint, but never below 1, and re-send.
            let cur = *self.next_index.get(&from).unwrap_or(&1);
            let backed = cur.saturating_sub(1).min(match_index + 1).max(1);
            self.next_index.insert(from, backed);
            self.send_append_entries(from, out);
        }
    }

    fn maybe_advance_commit(&mut self) {
        // §5.4.2: a leader only commits an entry from its CURRENT term by counting
        // replicas; earlier-term entries commit indirectly once a current-term
        // entry above them commits. Skipping the term check is a classic data-loss
        // bug.
        let threshold = if self.bug.commit_on_single_ack {
            2 // self + any one follower — wrong for clusters larger than 3
        } else {
            self.majority()
        };
        let mut n = self.last_log_index();
        while n > self.commit_index {
            if self.term_at(n) == self.current_term {
                let count = 1 + self
                    .match_index
                    .values()
                    .filter(|&&m| m >= n)
                    .count();
                if count >= threshold {
                    self.commit_index = n;
                    break;
                }
            }
            n -= 1;
        }
    }

    fn flush_apply(&mut self, out: &mut Vec<Output<C>>) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let e = &self.log[(self.last_applied - 1) as usize];
            out.push(Output::Apply {
                index: self.last_applied,
                term: e.term,
                client: e.client,
                seq: e.seq,
                command: e.command.clone(),
            });
        }
    }
}
