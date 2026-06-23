//! # deterrust-sim
//!
//! The deterministic simulation testing (DST) harness. It runs an entire Raft
//! cluster — every node, every client, the whole network — inside one
//! single-threaded event loop driven by virtual time and one 64-bit seed.
//!
//! There are no real threads, no real clocks, and no real sockets, so there is
//! no flakiness: a given seed reproduces a given run exactly, every time, on any
//! machine. The harness injects faults (latency, drop, duplication, partitions),
//! continuously checks safety invariants, records the client history, and at the
//! end runs a linearizability checker over it. When something breaks it reports
//! the seed; replaying the seed reproduces the identical failure.

mod rng;

use std::collections::{BinaryHeap, HashMap};

use deterrust_check::{self as check, History};
use deterrust_kv::{KvCommand, KvReply, KvStateMachine};
use deterrust_raft::{BugConfig, ClientId, Input, NodeId, Output, RaftMessage, RaftNode, Role};

pub use rng::Rng;

/// How nasty the injected network is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultLevel {
    /// Perfect network: no loss, no duplication, no partitions.
    None,
    /// Light loss/duplication and occasional partitions.
    Mild,
    /// Heavy loss/duplication and frequent partitions — the bug-hunting setting.
    Aggressive,
}

impl FaultLevel {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "none" => Some(Self::None),
            "mild" => Some(Self::Mild),
            "aggressive" => Some(Self::Aggressive),
            _ => None,
        }
    }
    fn drop_ppm(self) -> u64 {
        match self {
            Self::None => 0,
            Self::Mild => 20_000,
            Self::Aggressive => 60_000,
        }
    }
    fn dup_ppm(self) -> u64 {
        match self {
            Self::None => 0,
            Self::Mild => 10_000,
            Self::Aggressive => 30_000,
        }
    }
    fn partition_windows(self) -> u64 {
        match self {
            Self::None => 0,
            Self::Mild => 2,
            Self::Aggressive => 5,
        }
    }
}

/// Everything needed to run one simulation. `seed` makes it reproducible.
#[derive(Clone, Debug)]
pub struct SimConfig {
    pub seed: u64,
    pub nodes: u64,
    pub clients: u64,
    pub ops: u64,
    pub keys: u64,
    pub faults: FaultLevel,
    pub bug: BugConfig,
    pub election_timeout: (u64, u64),
    pub heartbeat_interval: u64,
    pub net_latency: (u64, u64),
    pub retry_timeout: u64,
    pub max_time: u64,
}

impl SimConfig {
    /// A sensible default cluster for a given seed.
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            nodes: 5,
            clients: 3,
            ops: 120,
            keys: 4,
            faults: FaultLevel::Aggressive,
            bug: BugConfig::default(),
            election_timeout: (150, 300),
            heartbeat_interval: 40,
            net_latency: (5, 25),
            retry_timeout: 900,
            max_time: 5_000_000,
        }
    }
}

/// What happened in a run. Anything other than [`Outcome::Ok`] is a failure;
/// errors and stalls are never reported as success.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// All client ops completed and the history is linearizable.
    Ok,
    /// A safety invariant was violated mid-run (split brain, divergence, ...).
    InvariantViolated(String),
    /// All ops completed but the observed history is not linearizable.
    NonLinearizable(String),
    /// The cluster failed to make progress within the time budget (a liveness
    /// problem — NOT success).
    Stalled(String),
}

impl Outcome {
    pub fn is_failure(&self) -> bool {
        !matches!(self, Outcome::Ok)
    }
    pub fn label(&self) -> &'static str {
        match self {
            Outcome::Ok => "OK",
            Outcome::InvariantViolated(_) => "INVARIANT-VIOLATED",
            Outcome::NonLinearizable(_) => "NON-LINEARIZABLE",
            Outcome::Stalled(_) => "STALLED",
        }
    }
}

/// One human-readable line of the recorded timeline, for the replay viewer.
#[derive(Clone, Debug)]
pub struct TraceEvent {
    pub time: u64,
    pub text: String,
}

/// The result of a run: enough to print a full replay and to drive a fuzzer.
#[derive(Clone, Debug)]
pub struct Report {
    pub seed: u64,
    pub outcome: Outcome,
    pub trace: Vec<TraceEvent>,
    pub ops_completed: u64,
    pub leader_changes: u64,
    pub events_processed: u64,
}

// ---------------------------------------------------------------------------
// Internal event queue
// ---------------------------------------------------------------------------

enum EventKind {
    Deliver {
        to: usize,
        from: NodeId,
        msg: RaftMessage<KvCommand>,
    },
    ElectionTimer {
        node: usize,
        epoch: u64,
    },
    HeartbeatTimer {
        node: usize,
        epoch: u64,
    },
    ClientTick {
        client: usize,
        seq: u64,
    },
    PartitionStart,
    PartitionHeal,
}

struct Event {
    time: u64,
    seq: u64,
    kind: EventKind,
}

// Order so that the BinaryHeap (a max-heap) yields the EARLIEST (time, seq).
impl PartialEq for Event {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time && self.seq == other.seq
    }
}
impl Eq for Event {}
impl PartialOrd for Event {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Event {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // reversed: smaller (time, seq) is "greater" so it pops first
        other
            .time
            .cmp(&self.time)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

// ---------------------------------------------------------------------------
// Client model (closed-loop: one outstanding op at a time)
// ---------------------------------------------------------------------------

struct PendingOp {
    seq: u64,
    op: KvCommand,
    call_ts: u64,
}

struct ClientState {
    id: ClientId,
    target: usize,
    pending: Option<PendingOp>,
    next_seq: u64,
}

// ---------------------------------------------------------------------------
// The simulator
// ---------------------------------------------------------------------------

/// A single deterministic run. Construct with [`Simulator::new`] then call
/// [`Simulator::run`].
pub struct Simulator {
    cfg: SimConfig,
    rng: Rng,
    clock: u64,
    seq_counter: u64,
    queue: BinaryHeap<Event>,

    nodes: Vec<RaftNode<KvCommand>>,
    sms: Vec<KvStateMachine>,
    prev_role: Vec<Role>,
    election_epoch: Vec<u64>,
    heartbeat_epoch: Vec<u64>,

    clients: Vec<ClientState>,
    issued: u64,
    completed: u64,

    /// node index -> partition group id. Equal group ids can communicate.
    groups: Vec<u8>,

    history: History,
    applied_sig: HashMap<u64, (u64, u64, u64)>, // index -> (term, client, seq)
    trace: Vec<TraceEvent>,
    violation: Option<Outcome>,
    leader_changes: u64,
}

impl Simulator {
    pub fn new(cfg: SimConfig) -> Self {
        let n = cfg.nodes as usize;
        let ids: Vec<NodeId> = (0..cfg.nodes).map(NodeId).collect();
        let nodes = ids
            .iter()
            .map(|&id| {
                let peers = ids.iter().copied().filter(|&p| p != id).collect();
                RaftNode::new(id, peers, cfg.bug)
            })
            .collect();
        let sms = (0..n).map(|_| KvStateMachine::new()).collect();
        let clients = (0..cfg.clients)
            .map(|c| ClientState {
                id: ClientId(c),
                target: (c as usize) % n,
                pending: None,
                next_seq: 1,
            })
            .collect();
        let rng = Rng::new(cfg.seed);
        Self {
            clock: 0,
            seq_counter: 0,
            queue: BinaryHeap::new(),
            nodes,
            sms,
            prev_role: vec![Role::Follower; n],
            election_epoch: vec![0; n],
            heartbeat_epoch: vec![0; n],
            clients,
            issued: 0,
            completed: 0,
            groups: vec![0; n],
            history: History::new(),
            applied_sig: HashMap::new(),
            trace: Vec::new(),
            violation: None,
            leader_changes: 0,
            rng,
            cfg,
        }
    }

    fn schedule(&mut self, after: u64, kind: EventKind) {
        let seq = self.seq_counter;
        self.seq_counter += 1;
        self.queue.push(Event {
            time: self.clock + after,
            seq,
            kind,
        });
    }

    fn trace(&mut self, text: impl Into<String>) {
        self.trace.push(TraceEvent {
            time: self.clock,
            text: text.into(),
        });
    }

    /// Run the simulation to completion and return a [`Report`].
    pub fn run(mut self) -> Report {
        // Arm initial election timers with staggered jitter so the cluster can
        // bootstrap a first leader.
        for node in 0..self.nodes.len() {
            let epoch = self.election_epoch[node];
            let t = self.rng.range(0, self.cfg.election_timeout.1);
            self.schedule(t, EventKind::ElectionTimer { node, epoch });
        }
        // Kick every client.
        for c in 0..self.clients.len() {
            self.schedule(0, EventKind::ClientTick { client: c, seq: 0 });
        }
        // Lay down the fault schedule (partition windows in the active phase).
        self.schedule_partitions();

        let mut events_processed = 0u64;
        while let Some(ev) = self.queue.pop() {
            if ev.time > self.cfg.max_time {
                break;
            }
            self.clock = ev.time;
            self.handle(ev.kind);
            events_processed += 1;

            if self.violation.is_some() {
                break;
            }
            // Terminate once all ops are done (heartbeats would otherwise run
            // forever). The drain happens naturally: clients keep retrying until
            // their pending op completes.
            if self.issued >= self.cfg.ops && self.clients.iter().all(|c| c.pending.is_none()) {
                break;
            }
        }

        let outcome = self.finalize();
        Report {
            seed: self.cfg.seed,
            outcome,
            trace: self.trace,
            ops_completed: self.completed,
            leader_changes: self.leader_changes,
            events_processed,
        }
    }

    fn finalize(&mut self) -> Outcome {
        if let Some(v) = self.violation.take() {
            return v;
        }
        if self.clients.iter().any(|c| c.pending.is_some()) {
            return Outcome::Stalled(format!(
                "{} client op(s) never completed within the time budget",
                self.clients.iter().filter(|c| c.pending.is_some()).count()
            ));
        }
        // Durability / leader-completeness: every entry that was ever applied
        // (and therefore acknowledged) must still exist on at least one node in
        // the converged cluster. A correct cluster only applies majority-
        // committed entries, which are never truncated; a premature-commit bug
        // applies entries that were never durably committed, so they vanish.
        let mut indices: Vec<u64> = self.applied_sig.keys().copied().collect();
        indices.sort_unstable();
        for index in indices {
            let sig = self.applied_sig[&index];
            let present = self
                .nodes
                .iter()
                .any(|n| n.entry_at(index).map(|e| (e.term, e.client.0, e.seq)) == Some(sig));
            if !present {
                return Outcome::InvariantViolated(format!(
                    "durability violated: applied entry at log index {index} {sig:?} is gone from \
                     every node — an acknowledged write was lost"
                ));
            }
        }
        match check::check(&self.history) {
            check::CheckResult::Linearizable => Outcome::Ok,
            check::CheckResult::NotLinearizable { explanation } => {
                Outcome::NonLinearizable(explanation)
            }
            check::CheckResult::BudgetExceeded { explanation } => {
                // Be honest: we could not decide. Treat as a non-success outcome.
                Outcome::Stalled(format!(
                    "linearizability checker budget exceeded: {explanation}"
                ))
            }
        }
    }

    // ---- event handlers ----

    fn handle(&mut self, kind: EventKind) {
        match kind {
            EventKind::Deliver { to, from, msg } => self.on_deliver(to, from, msg),
            EventKind::ElectionTimer { node, epoch } => {
                if epoch == self.election_epoch[node] {
                    let outs = self.nodes[node].step(Input::ElectionTimeout);
                    self.process(node, outs);
                }
            }
            EventKind::HeartbeatTimer { node, epoch } => {
                if epoch == self.heartbeat_epoch[node] {
                    let outs = self.nodes[node].step(Input::HeartbeatTick);
                    self.process(node, outs);
                }
            }
            EventKind::ClientTick { client, seq } => self.on_client_tick(client, seq),
            EventKind::PartitionStart => self.start_partition(),
            EventKind::PartitionHeal => self.heal_partition(),
        }
        self.check_invariants();
    }

    fn on_deliver(&mut self, to: usize, from: NodeId, msg: RaftMessage<KvCommand>) {
        // Partition is enforced at send time; if it changed we still drop a
        // message that now crosses a cut.
        if self.groups[to] != self.groups[from.0 as usize] {
            return;
        }
        let outs = self.nodes[to].step(Input::Deliver { from, msg });
        self.process(to, outs);
    }

    fn process(&mut self, node: usize, outs: Vec<Output<KvCommand>>) {
        // Note role transitions for the trace + leader-change counter.
        let role_now = self.nodes[node].role();
        if role_now != self.prev_role[node] {
            match role_now {
                Role::Leader => {
                    self.leader_changes += 1;
                    let term = self.nodes[node].current_term();
                    self.trace(format!("n{node} BECAME LEADER (term {term})"));
                }
                Role::Candidate => {
                    let term = self.nodes[node].current_term();
                    self.trace(format!("n{node} starts election (term {term})"));
                }
                Role::Follower => {}
            }
            self.prev_role[node] = role_now;
        }

        for o in outs {
            match o {
                Output::Send { to, msg } => self.route(node, to, msg),
                Output::ArmElectionTimer => {
                    self.election_epoch[node] += 1;
                    let epoch = self.election_epoch[node];
                    let t = self
                        .rng
                        .range(self.cfg.election_timeout.0, self.cfg.election_timeout.1);
                    self.schedule(t, EventKind::ElectionTimer { node, epoch });
                }
                Output::ArmHeartbeatTimer => {
                    self.heartbeat_epoch[node] += 1;
                    let epoch = self.heartbeat_epoch[node];
                    let hb = self.cfg.heartbeat_interval;
                    self.schedule(hb, EventKind::HeartbeatTimer { node, epoch });
                }
                Output::Apply {
                    index,
                    term,
                    client,
                    seq,
                    command,
                } => self.on_apply(node, index, term, client, seq, command),
                Output::Redirect {
                    client,
                    seq,
                    leader_hint,
                } => self.on_redirect(client, seq, leader_hint),
            }
        }
    }

    fn route(&mut self, from: usize, to: NodeId, msg: RaftMessage<KvCommand>) {
        // Drop across a partition cut.
        if self.groups[from] != self.groups[to.0 as usize] {
            return;
        }
        if self.rng.chance(self.cfg.faults.drop_ppm(), 1_000_000) {
            return; // lost in transit
        }
        let latency = self
            .rng
            .range(self.cfg.net_latency.0, self.cfg.net_latency.1 + 1);
        let from_id = self.nodes[from].id();
        self.schedule(
            latency,
            EventKind::Deliver {
                to: to.0 as usize,
                from: from_id,
                msg: msg.clone(),
            },
        );
        // Occasionally duplicate.
        if self.rng.chance(self.cfg.faults.dup_ppm(), 1_000_000) {
            let extra = self
                .rng
                .range(self.cfg.net_latency.0, self.cfg.net_latency.1 + 1);
            self.schedule(
                latency + extra,
                EventKind::Deliver {
                    to: to.0 as usize,
                    from: from_id,
                    msg,
                },
            );
        }
    }

    fn on_apply(
        &mut self,
        node: usize,
        index: u64,
        term: u64,
        client: ClientId,
        seq: u64,
        command: KvCommand,
    ) {
        // State-machine safety / log matching: every node that applies `index`
        // must apply the SAME (term, client, seq). A mismatch is divergence.
        let sig = (term, client.0, seq);
        match self.applied_sig.get(&index) {
            Some(prev) if *prev != sig => {
                let msg = format!(
                    "state-machine safety violated at log index {index}: {prev:?} vs {sig:?}"
                );
                self.trace(format!("✗ {msg}"));
                self.violation = Some(Outcome::InvariantViolated(msg));
                return;
            }
            None => {
                self.applied_sig.insert(index, sig);
            }
            _ => {}
        }

        let reply = self.sms[node].apply(client, seq, &command);

        // Deliver the reply to the client only from the node it is talking to.
        let ci = client.0 as usize;
        if ci < self.clients.len()
            && self.clients[ci].target == node
            && self.clients[ci].pending.as_ref().map(|p| p.seq) == Some(seq)
        {
            self.complete_client_op(ci, &command, &reply);
        }
    }

    fn complete_client_op(&mut self, ci: usize, command: &KvCommand, reply: &KvReply) {
        let pending = self.clients[ci].pending.take().expect("pending op");
        // Record into the linearizability history.
        let (op, ret) = to_check(command, reply);
        self.history.record(check::Op {
            client: self.clients[ci].id.0,
            op,
            ret,
            call_ts: pending.call_ts,
            return_ts: self.clock,
        });
        self.completed += 1;
        // Issue the next op after some "think time" so the workload spreads
        // across virtual time and overlaps the fault schedule. Without this, a
        // closed-loop client collapses the whole history into one instant and
        // faults never bite.
        let think = self
            .rng
            .range(self.cfg.net_latency.1, self.cfg.election_timeout.0);
        let next = self.clients[ci].next_seq;
        self.schedule(
            think,
            EventKind::ClientTick {
                client: ci,
                seq: next,
            },
        );
    }

    fn on_redirect(&mut self, client: ClientId, seq: u64, leader_hint: Option<NodeId>) {
        let ci = client.0 as usize;
        if ci >= self.clients.len() {
            return;
        }
        if self.clients[ci].pending.as_ref().map(|p| p.seq) != Some(seq) {
            return; // stale
        }
        // Move toward the hinted leader (or a random node) and retry soon.
        let n = self.nodes.len();
        self.clients[ci].target = match leader_hint {
            Some(NodeId(id)) => id as usize % n,
            None => self.rng.index(n),
        };
        let backoff = self.cfg.election_timeout.0 / 2 + 1;
        self.schedule(backoff, EventKind::ClientTick { client: ci, seq });
    }

    fn on_client_tick(&mut self, ci: usize, seq: u64) {
        // A retry tick for an op that already completed: ignore.
        if let Some(p) = &self.clients[ci].pending {
            if p.seq != seq {
                return;
            }
        }

        // No pending op: start a new one if the global budget allows.
        if self.clients[ci].pending.is_none() {
            if self.issued >= self.cfg.ops {
                return;
            }
            let new_seq = self.clients[ci].next_seq;
            self.clients[ci].next_seq += 1;
            let op = self.gen_op(ci, new_seq);
            self.clients[ci].pending = Some(PendingOp {
                seq: new_seq,
                op,
                call_ts: self.clock,
            });
            self.issued += 1;
        }

        // Send (or resend) the pending op to the current target.
        let pending_seq = self.clients[ci].pending.as_ref().unwrap().seq;
        let op = self.clients[ci].pending.as_ref().unwrap().op.clone();
        let target = self.clients[ci].target;
        let client_id = self.clients[ci].id;
        let outs = self.nodes[target].step(Input::Propose {
            client: client_id,
            seq: pending_seq,
            command: op,
        });
        self.process(target, outs);

        // Arm a retry in case this attempt is lost or the leader changes.
        self.schedule(
            self.cfg.retry_timeout,
            EventKind::ClientTick {
                client: ci,
                seq: pending_seq,
            },
        );
    }

    fn gen_op(&mut self, ci: usize, seq: u64) -> KvCommand {
        let key = format!("k{}", self.rng.index(self.cfg.keys as usize));
        let roll = self.rng.range(0, 100);
        if roll < 45 {
            KvCommand::Put {
                key,
                val: format!("c{ci}.{seq}"),
            }
        } else if roll < 90 {
            KvCommand::Get { key }
        } else {
            KvCommand::Cas {
                key,
                expect: None,
                new: format!("c{ci}.{seq}"),
            }
        }
    }

    // ---- partitions ----

    fn schedule_partitions(&mut self) {
        if self.cfg.faults.partition_windows() == 0 {
            return;
        }
        // A self-perpetuating cycle confined to the ACTIVE phase (while ops are
        // still being issued). It heals before the drain so pending ops can
        // complete on a clean network. Scaling to the workload — not to
        // `max_time` — is what makes partitions actually overlap real traffic.
        let first = self
            .rng
            .range(self.cfg.election_timeout.0, self.cfg.election_timeout.1);
        self.schedule(first, EventKind::PartitionStart);
    }

    fn start_partition(&mut self) {
        // During the drain phase we stop injecting partitions so in-flight ops
        // can finish on a healthy network.
        if self.issued >= self.cfg.ops {
            return;
        }
        // A partition lasts a few election timeouts so the majority side can
        // actually elect a new leader and write the same log indices.
        let dur = self
            .rng
            .range(self.cfg.election_timeout.1, self.cfg.election_timeout.1 * 3);
        self.schedule(dur, EventKind::PartitionHeal);

        // Randomly two-color the nodes, guaranteeing both sides are non-empty.
        let n = self.nodes.len();
        loop {
            let mut g = vec![0u8; n];
            let mut ones = 0;
            for slot in g.iter_mut() {
                if self.rng.chance(1, 2) {
                    *slot = 1;
                    ones += 1;
                }
            }
            if ones != 0 && ones != n {
                self.groups = g;
                break;
            }
        }
        let side: Vec<String> = (0..n).map(|i| format!("n{i}:{}", self.groups[i])).collect();
        self.trace(format!("PARTITION {{ {} }}", side.join(" ")));
    }

    fn heal_partition(&mut self) {
        self.groups = vec![0u8; self.nodes.len()];
        self.trace("PARTITION HEALED".to_string());
        // Keep the churn going through the active phase only.
        if self.issued < self.cfg.ops {
            let gap = self
                .rng
                .range(self.cfg.election_timeout.0, self.cfg.election_timeout.1 * 2);
            self.schedule(gap, EventKind::PartitionStart);
        }
    }

    // ---- invariants ----

    fn check_invariants(&mut self) {
        if self.violation.is_some() {
            return;
        }
        // Election safety: no two leaders sharing the same term.
        let mut leaders: Vec<(u64, usize)> = Vec::new();
        for (i, node) in self.nodes.iter().enumerate() {
            if node.role() == Role::Leader {
                leaders.push((node.current_term(), i));
            }
        }
        for a in 0..leaders.len() {
            for b in (a + 1)..leaders.len() {
                if leaders[a].0 == leaders[b].0 {
                    let msg = format!(
                        "election safety violated: n{} and n{} both leader in term {}",
                        leaders[a].1, leaders[b].1, leaders[a].0
                    );
                    self.trace(format!("✗ {msg}"));
                    self.violation = Some(Outcome::InvariantViolated(msg));
                    return;
                }
            }
        }
    }
}

fn to_check(command: &KvCommand, reply: &KvReply) -> (check::KvOp, check::KvRet) {
    let op = match command {
        KvCommand::Put { key, val } => check::KvOp::Put {
            key: key.clone(),
            val: val.clone(),
        },
        KvCommand::Get { key } => check::KvOp::Get { key: key.clone() },
        KvCommand::Cas { key, expect, new } => check::KvOp::Cas {
            key: key.clone(),
            expect: expect.clone(),
            new: new.clone(),
        },
    };
    let ret = match reply {
        KvReply::PutOk => check::KvRet::PutOk,
        KvReply::GetVal(v) => check::KvRet::GetVal(v.clone()),
        KvReply::CasOk(b) => check::KvRet::CasOk(*b),
    };
    (op, ret)
}

/// Convenience: run one seed with common knobs.
pub fn run_seed(seed: u64, bug: BugConfig, faults: FaultLevel, nodes: u64, ops: u64) -> Report {
    let mut cfg = SimConfig::new(seed);
    cfg.bug = bug;
    cfg.faults = faults;
    cfg.nodes = nodes;
    cfg.ops = ops;
    Simulator::new(cfg).run()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_network_is_linearizable() {
        // No faults, no bugs: every run must be OK and complete all ops.
        for seed in 0..40 {
            let r = run_seed(seed, BugConfig::default(), FaultLevel::None, 3, 30);
            assert_eq!(r.outcome, Outcome::Ok, "seed {seed} should be OK");
            assert_eq!(r.ops_completed, 30);
        }
    }

    #[test]
    fn determinism_same_seed_same_run() {
        let a = run_seed(12345, BugConfig::default(), FaultLevel::Aggressive, 5, 60);
        let b = run_seed(12345, BugConfig::default(), FaultLevel::Aggressive, 5, 60);
        assert_eq!(a.outcome, b.outcome);
        assert_eq!(a.events_processed, b.events_processed);
        assert_eq!(a.ops_completed, b.ops_completed);
        assert_eq!(a.leader_changes, b.leader_changes);
        // The recorded timelines must be byte-identical.
        let ta: Vec<_> = a.trace.iter().map(|e| (e.time, &e.text)).collect();
        let tb: Vec<_> = b.trace.iter().map(|e| (e.time, &e.text)).collect();
        assert_eq!(ta, tb);
    }

    #[test]
    fn correct_raft_survives_aggressive_faults() {
        // The whole point: a CORRECT cluster never loses linearizability even
        // under heavy partitions/loss. Sweep many seeds; none may fail.
        for seed in 0..200 {
            let r = run_seed(seed, BugConfig::default(), FaultLevel::Aggressive, 5, 80);
            assert!(
                !r.outcome.is_failure(),
                "correct cluster failed on seed {seed}: {:?}",
                r.outcome
            );
        }
    }

    fn sweep_finds_bug(bug: BugConfig) -> Option<u64> {
        (0..3000).find(|&seed| {
            run_seed(seed, bug, FaultLevel::Aggressive, 5, 100)
                .outcome
                .is_failure()
        })
    }

    #[test]
    fn harness_catches_commit_on_single_ack() {
        let bug = BugConfig {
            commit_on_single_ack: true,
            ..Default::default()
        };
        let seed = sweep_finds_bug(bug).expect("must catch commit-on-single-ack bug");
        // And it must be reproducible: re-running the seed gives the same failure.
        let a = run_seed(seed, bug, FaultLevel::Aggressive, 5, 100);
        let b = run_seed(seed, bug, FaultLevel::Aggressive, 5, 100);
        assert!(a.outcome.is_failure());
        assert_eq!(a.outcome, b.outcome);
    }

    #[test]
    fn harness_catches_vote_without_log_check() {
        let bug = BugConfig {
            vote_without_log_check: true,
            ..Default::default()
        };
        assert!(
            sweep_finds_bug(bug).is_some(),
            "must catch vote-without-log-check bug"
        );
    }

    #[test]
    fn harness_catches_accept_stale_append() {
        let bug = BugConfig {
            accept_stale_append: true,
            ..Default::default()
        };
        assert!(
            sweep_finds_bug(bug).is_some(),
            "must catch accept-stale-append bug"
        );
    }
}
