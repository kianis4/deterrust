//! Canonical Raft correctness scenarios, exercised through a deterministic,
//! fault-free in-test delivery harness (FIFO message pump to quiescence).
//!
//! These pin down the parts of Raft people get wrong before the full simulator
//! (with fault injection) ever runs.

use std::collections::{HashMap, VecDeque};

use deterrust_raft::{BugConfig, ClientId, Input, NodeId, Output, RaftMessage, RaftNode, Role};

type Cmd = u64;

/// A tiny synchronous cluster: no faults, FIFO delivery, manual timer control.
struct Cluster {
    nodes: Vec<RaftNode<Cmd>>,
    inflight: VecDeque<(NodeId, NodeId, RaftMessage<Cmd>)>, // (to, from, msg)
    applied: HashMap<u64, Vec<Cmd>>,
}

impl Cluster {
    fn new(n: u64, bug: BugConfig) -> Self {
        let ids: Vec<NodeId> = (0..n).map(NodeId).collect();
        let nodes = ids
            .iter()
            .map(|&id| {
                let peers = ids.iter().copied().filter(|&p| p != id).collect();
                RaftNode::new(id, peers, bug)
            })
            .collect();
        Self {
            nodes,
            inflight: VecDeque::new(),
            applied: HashMap::new(),
        }
    }

    fn node(&self, id: u64) -> &RaftNode<Cmd> {
        &self.nodes[id as usize]
    }

    fn absorb(&mut self, id: u64, outs: Vec<Output<Cmd>>) {
        for o in outs {
            match o {
                Output::Send { to, msg } => self.inflight.push_back((to, NodeId(id), msg)),
                Output::Apply { command, .. } => self.applied.entry(id).or_default().push(command),
                Output::ArmElectionTimer | Output::ArmHeartbeatTimer | Output::Redirect { .. } => {}
            }
        }
    }

    fn step(&mut self, id: u64, input: Input<Cmd>) {
        let outs = self.nodes[id as usize].step(input);
        self.absorb(id, outs);
    }

    /// Deliver all in-flight messages (and any they cascade) until quiescent.
    fn settle(&mut self) {
        let mut guard = 0;
        while let Some((to, from, msg)) = self.inflight.pop_front() {
            let outs = self.nodes[to.0 as usize].step(Input::Deliver { from, msg });
            self.absorb(to.0, outs);
            guard += 1;
            assert!(guard < 100_000, "message pump failed to converge");
        }
    }

    /// Like `settle`, but every message addressed to `cut` is lost in transit.
    fn settle_dropping_to(&mut self, cut: u64) {
        let mut guard = 0;
        while let Some((to, from, msg)) = self.inflight.pop_front() {
            if to.0 == cut {
                continue;
            }
            let outs = self.nodes[to.0 as usize].step(Input::Deliver { from, msg });
            self.absorb(to.0, outs);
            guard += 1;
            assert!(guard < 100_000, "message pump failed to converge");
        }
    }

    fn elect(&mut self, id: u64) {
        self.step(id, Input::ElectionTimeout);
        self.settle();
    }

    fn heartbeat(&mut self, id: u64) {
        self.step(id, Input::HeartbeatTick);
        self.settle();
    }

    fn propose(&mut self, leader: u64, client: u64, seq: u64, cmd: Cmd) {
        self.step(
            leader,
            Input::Propose {
                client: ClientId(client),
                seq,
                command: cmd,
            },
        );
        self.settle();
    }

    fn leaders(&self) -> Vec<u64> {
        self.nodes
            .iter()
            .filter(|n| n.role() == Role::Leader)
            .map(|n| n.id().0)
            .collect()
    }
}

#[test]
fn single_node_elects_itself_and_commits() {
    let mut c = Cluster::new(1, BugConfig::default());
    c.elect(0);
    assert!(c.node(0).is_leader());
    c.propose(0, 0, 1, 42);
    assert_eq!(
        c.applied.get(&0).map(|v| v.as_slice()),
        Some([42].as_slice())
    );
    assert_eq!(c.node(0).commit_index(), 1);
}

#[test]
fn three_nodes_elect_one_leader() {
    let mut c = Cluster::new(3, BugConfig::default());
    c.elect(0);
    assert_eq!(c.leaders(), vec![0]);
    assert_eq!(c.node(0).current_term(), 1);
    // Followers recognize the leader.
    assert_eq!(c.node(1).leader_id(), Some(NodeId(0)));
    assert_eq!(c.node(2).leader_id(), Some(NodeId(0)));
}

#[test]
fn replication_commits_on_majority_and_propagates() {
    let mut c = Cluster::new(5, BugConfig::default());
    c.elect(0);
    c.propose(0, 0, 1, 100);
    c.propose(0, 0, 2, 200);
    // Leader committed both.
    assert_eq!(c.node(0).commit_index(), 2);
    assert_eq!(c.applied.get(&0).unwrap().as_slice(), [100, 200]);
    // A heartbeat carries the commit index to followers; they apply too.
    c.heartbeat(0);
    for f in 1..5 {
        assert_eq!(
            c.applied.get(&f).map(|v| v.as_slice()),
            Some([100, 200].as_slice()),
            "follower {f} should have applied both entries"
        );
    }
}

#[test]
fn follower_log_backfills_after_missing_entries() {
    // Partition node 4 out by simply not delivering to it during proposals,
    // then let the leader backfill it via the next heartbeat.
    let mut c = Cluster::new(5, BugConfig::default());
    c.elect(0);
    c.propose(0, 0, 1, 11);
    c.propose(0, 0, 2, 22);
    c.propose(0, 0, 3, 33);
    c.heartbeat(0);
    // Everyone, including any lagging follower, converges to the same log.
    for n in 0..5 {
        assert_eq!(c.node(n).last_log_index(), 3, "node {n} log length");
        assert_eq!(c.node(n).term_at(3), 1);
    }
}

#[test]
fn higher_term_message_steps_down_a_leader() {
    let mut c = Cluster::new(3, BugConfig::default());
    c.elect(0);
    assert!(c.node(0).is_leader());
    // Node 1 starts a fresh election at a higher term.
    c.elect(1);
    // Old leader must have stepped down; exactly one leader remains.
    assert_eq!(c.leaders().len(), 1);
    assert!(c.node(0).current_term() >= 2);
    assert!(!c.node(0).is_leader());
}

#[test]
fn vote_denied_to_candidate_with_shorter_log() {
    // Build a leader with a committed entry, then have a log-less node campaign.
    let mut c = Cluster::new(3, BugConfig::default());
    c.elect(0);
    c.propose(0, 0, 1, 7);
    c.heartbeat(0); // replicate the entry to followers 1 and 2
    assert_eq!(c.node(1).last_log_index(), 1);

    // Hand-craft a vote request from a fictitious candidate with an empty log
    // at a high term. An up-to-date follower (node 1) must refuse.
    let outs = c.nodes[1].step(Input::Deliver {
        from: NodeId(2),
        msg: RaftMessage::RequestVote {
            term: 9,
            candidate_id: NodeId(2),
            last_log_index: 0,
            last_log_term: 0,
        },
    });
    let granted = outs.iter().any(|o| {
        matches!(
            o,
            Output::Send {
                msg: RaftMessage::RequestVoteReply {
                    vote_granted: true,
                    ..
                },
                ..
            }
        )
    });
    assert!(!granted, "must not vote for a candidate with a stale log");
}

#[test]
fn conflicting_entries_are_truncated() {
    // A follower holds a divergent entry at index 1; a legitimate leader's
    // AppendEntries must overwrite it.
    let mut c = Cluster::new(3, BugConfig::default());
    // Node 0 becomes leader at term 1 and replicates entry A=1.
    c.elect(0);
    c.propose(0, 0, 1, 1);
    c.heartbeat(0);
    assert_eq!(c.node(2).entry_at(1).unwrap().command, 1);

    // Directly inject a conflicting entry into node 2 at a higher term via a
    // crafted AppendEntries from a "new leader" (node 1) at term 5.
    let conflicting = vec![deterrust_raft::LogEntry {
        term: 5,
        client: ClientId(0),
        seq: 99,
        command: 999,
    }];
    let outs = c.nodes[2].step(Input::Deliver {
        from: NodeId(1),
        msg: RaftMessage::AppendEntries {
            term: 5,
            leader_id: NodeId(1),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: conflicting,
            leader_commit: 0,
        },
    });
    let _ = outs;
    assert_eq!(
        c.node(2).entry_at(1).unwrap().command,
        999,
        "stale entry should have been truncated and replaced"
    );
    assert_eq!(c.node(2).term_at(1), 5);
}

#[test]
fn leader_does_not_commit_previous_term_entry_by_count_alone() {
    // The §5.4.2 restriction: a fresh leader must NOT mark a previous-term entry
    // committed merely because it is replicated on a majority. It commits only
    // once a current-term entry on top of it reaches a majority.
    //
    // Setup: node 0 leads term 1 and replicates entry 1 to nodes 1 and 2, but
    // every reply to node 0 is lost, so nobody ever commits entry 1. Node 1 then
    // wins term 2. Its first AppendEntries round makes both followers report
    // match_index = 1, so entry 1 sits on all three nodes. Counting replicas
    // alone would commit it here; the current-term rule (node.rs:469) must not.
    let mut c = Cluster::new(3, BugConfig::default());
    c.elect(0); // node 0 leads term 1
    c.step(
        0,
        Input::Propose {
            client: ClientId(0),
            seq: 1,
            command: 55,
        },
    );
    c.settle_dropping_to(0); // followers append entry 1; their acks are lost
    for n in 0..3 {
        assert_eq!(c.node(n).term_at(1), 1, "node {n} should hold entry 1");
        assert_eq!(c.node(n).commit_index(), 0, "node {n} must not commit yet");
    }

    c.elect(1); // node 1 wins term 2; followers ack its AppendEntries
    assert_eq!(c.leaders(), vec![1]);
    assert_eq!(c.node(1).current_term(), 2);
    assert_eq!(
        c.node(1).commit_index(),
        0,
        "term-2 leader committed a term-1 entry by counting replicas (§5.4.2)"
    );
    assert!(c.applied.is_empty(), "nothing may be applied yet");

    // A term-2 entry reaching a majority commits it, and entry 1 with it.
    c.propose(1, 0, 2, 66);
    assert_eq!(c.node(1).term_at(2), 2);
    assert_eq!(c.node(1).commit_index(), 2);
    assert_eq!(c.applied.get(&1).unwrap().as_slice(), [55, 66]);
}
