# deterrust: architecture & engineering spec

> A from-scratch Raft-replicated key-value store wrapped in a **deterministic
> simulation testing (DST)** harness. The consensus is the system-under-test;
> the harness is the product. It runs the cluster under injected faults, checks
> safety invariants and linearizability, and **replays any failure exactly from a
> single 64-bit seed.**

This document began as the spec written before the code and was corrected
against the code in November 2026; it now describes what is built. The raft, kv,
sim and check crates are synchronous and free of ambient nondeterminism (no
`SystemTime::now`, no `rand::thread_rng`, no threads, no real I/O). All
nondeterminism is funneled through one seed. Only the CLI uses threads, to run
independent seeds in parallel.

## Design pillars

1. **Sans-IO Raft.** The Raft node is a pure state machine:
   `pub fn step(&mut self, input: Input<C>) -> Vec<Output<C>>`.
   It never blocks, never reads a clock, never touches a socket. Inputs are
   election timeouts, heartbeat ticks, delivered peer messages, and client
   proposals. Outputs are send-message, arm-timer, apply-to-state-machine, and
   redirect-client intents. The runtime decides how to realize outputs; only the
   simulated runtime exists today. `quinn-proto` and `rustls` use the same
   sans-IO style.
2. **One seed → one universe.** Given a `u64` seed, the entire cluster run
   (message delays, drops, duplications, reordering, partitions, election-timeout
   jitter, client op interleaving) is a deterministic function. Re-running the
   seed reproduces the identical event trace.
3. **Falsify, don't assert.** Correctness is checked by invariant monitors
   (election safety and state-machine safety during the run, durability at the
   end) and by a linearizability checker over the observed client history at the
   end, not by hopeful unit asserts. So far every bug the harness has caught was
   caught by the monitors, not the checker (see the sim section). The deliverable
   demo is "the harness caught a bug; here is the seed; watch it replay."

## Crate layout (cargo workspace)

```
crates/
  raft/    deterrust-raft   pure Raft state machine (no I/O, no clock, no rng)
  kv/      deterrust-kv     replicated KV application state machine + commands
  sim/     deterrust-sim    deterministic runtime: virtual clock, seeded rng,
                            network model + fault injection, the sim driver,
                            invariant checks, event-trace recording
  check/   deterrust-check  linearizability checker (WGL) over a KV register
  cli/     deterrust        binary: `fuzz`, `replay`, `demo`, `check`
```

Dependency direction (no cycles): `kv` → `raft`; `sim` → {`raft`, `kv`, `check`};
`cli` → {`sim`, `check`, `raft`, `kv`}; `check` is standalone.

## crate: deterrust-raft (the hard core)

Implements the minimal subset of Raft (Ongaro & Ousterhout, Fig. 2): leader
election + log replication + commit advancement + safety. Snapshots and
membership changes are not implemented.

### State (per node)
- "Persistent" (named as in Fig. 2; kept in memory, and the simulator never
  crashes a node): `current_term: u64`, `voted_for: Option<NodeId>`,
  `log: Vec<LogEntry<C>>` (1-indexed: `log[0]` is index 1, and index 0 means
  "before the log"). `LogEntry { term, client, seq, command }`.
- Volatile: `commit_index: u64`, `last_applied: u64`, `role: Follower|Candidate|Leader`,
  `leader_id: Option<NodeId>`, `votes_received: HashSet<NodeId>`.
- Leader-only volatile: `next_index: HashMap<NodeId,u64>`, `match_index: HashMap<NodeId,u64>`.
- Timing: the node keeps no deadlines. It asks the runtime to (re)arm timers with
  `Output::ArmElectionTimer` and `Output::ArmHeartbeatTimer`; the runtime later
  feeds back `Input::ElectionTimeout` or `Input::HeartbeatTick`.

### Inputs / Outputs
```rust
enum Input<C> {
    ElectionTimeout,                                    // election timer fired
    HeartbeatTick,                                      // heartbeat timer fired (leaders act)
    Deliver { from: NodeId, msg: RaftMessage<C> },      // a peer message arrived
    Propose { client: ClientId, seq: u64, command: C }, // a client command
}
enum Output<C> {
    Send { to: NodeId, msg: RaftMessage<C> },
    ArmElectionTimer,  // runtime picks the jittered timeout from its seeded rng
    ArmHeartbeatTimer,
    Apply { index: LogIndex, term: Term, client: ClientId, seq: u64, command: C },
    Redirect { client: ClientId, seq: u64, leader_hint: Option<NodeId> }, // not the leader
}
enum RaftMessage<C> {
    RequestVote { term, candidate_id, last_log_index, last_log_term },
    RequestVoteReply { term, vote_granted },
    AppendEntries { term, leader_id, prev_log_index, prev_log_term, entries, leader_commit },
    AppendEntriesReply { term, success, match_index }, // match_index hint for fast convergence
}
```

The node never replies to clients itself. The simulator completes a client's
operation when the node that client is talking to applies its entry.

### The 8 subtle correctness rules (implemented in `crates/raft/src/node.rs`)
1. **Term is monotonic; any RPC/reply with a higher term ⇒ step down to follower,
   adopt term, clear `voted_for`** (before processing the RPC body).
2. **RequestVote granting:** grant iff `term >= current_term` AND
   (`voted_for` is None or == candidate) AND candidate's log is **at least as
   up-to-date** (compare (last_log_term, last_log_index) lexicographically).
3. **AppendEntries consistency check:** reject unless the log contains an entry
   at `prev_log_index` with `prev_log_term`. On reject, return a `match_index`
   hint so the leader backs up efficiently (not one-at-a-time).
4. **Conflict truncation:** when appending, if an existing entry conflicts
   (same index, different term) delete it and everything after, then append.
   Never truncate committed entries (guaranteed by rules 2 and 5, not by a
   separate check).
5. **Commit advancement (leader):** advance `commit_index` to the highest `N`
   such that a majority of `match_index >= N` **AND `log[N].term == current_term`**
   (the §5.4.2 restriction: never commit a previous-term entry by counting
   replicas; it commits indirectly once a current-term entry above it commits).
   The term check is `node.rs:469`; the scenario test
   `leader_does_not_commit_previous_term_entry_by_count_alone` fails without it.
6. **Follower commit:** `commit_index = min(leader_commit, index_of_last_new_entry)`.
7. **Election safety:** at most one leader per term, enforced by the
   at-most-one-vote-per-term rule (#2) + majority.
8. **Apply only committed, in order:** apply entries `last_applied+1..=commit_index`
   exactly once, in index order, emitting `Output::Apply`.

### Determinism contract
- The node never calls rng or clock. Election-timeout jitter is requested from
  the runtime (`ArmElectionTimer`), which draws from the seeded rng. The node
  is a deterministic function of its inputs.

### Planted bugs (runtime flags, for the demo and the harness tests)
`BugConfig` (`crates/raft/src/types.rs`) holds three flags, all off by default.
They are plain struct fields, not cargo features; the CLI selects one with `--bug`:
- `commit-single-ack` (`commit_on_single_ack`): the leader's commit threshold
  drops from a majority to 1, so it commits an entry on its own copy before any
  follower has it. Under a partition, two nodes end up applying different
  entries at the same log index.
- `stale-append` (`accept_stale_append`): a follower accepts AppendEntries from
  a leader whose term is older than its own.
- `vote-no-log-check` (`vote_without_log_check`): a node grants its vote without
  the §5.4.1 up-to-date log check, so a candidate missing committed entries can
  win and overwrite them.

## crate: deterrust-kv
- `KvCommand::{ Put{key,val}, Get{key}, Cas{key,expect,new} }` and
  `KvReply::{ PutOk, GetVal, CasOk }`.
- A `KvStateMachine` holding a `BTreeMap<String,String>` plus a per-client session
  table, so a retried command that lands in the log twice takes effect once.
  Reads go through the log (linearizable reads); lease or read-index reads are
  not built.

## crate: deterrust-sim (the harness)
- **Virtual clock:** a `u64` tick counter. Time only advances by popping the next
  scheduled event.
- **Seeded RNG:** SplitMix64, hand-written in `crates/sim/src/rng.rs` rather than
  pulled from a crate. All randomness (delays, drop decisions, timeout jitter,
  client ops and think time, partitions) draws from this one stream.
- **Event queue:** a binary heap keyed by `(time, seq)`; `seq` is a monotonic
  tiebreaker so equal-time events have a deterministic order. Events: message
  delivery, election timer, heartbeat timer, client tick (start or retry an op),
  partition start, partition heal.
- **Network model + fault injection** (all seeded, set by a `FaultLevel` of
  `none`, `mild` or `aggressive`): drop probability 0 / 2% / 6% and duplication
  probability 0 / 1% / 3%; `mild` and `aggressive` run the same partition
  schedule. Every
  message gets a random latency of 5 to 25 ticks, which also reorders messages.
  A partition splits the nodes into two non-empty groups at random, lasts 300 to
  900 ticks, and messages across the cut are dropped. Partitions repeat while
  clients are still issuing operations, then stop so pending operations can
  finish. There are no node crashes or restarts.
- **Driver:** owns N raft nodes + KV state machines + client model. Loop: pop
  event → feed node `step` → realize outputs (schedule sends with seeded
  latency/drop/dup, arm timers with seeded jitter, apply to KV, record client
  replies) → record into the **event trace** and the **history** → repeat until
  every client operation has completed, an invariant trips, or virtual time
  passes `max_time`.
- **Invariant monitors:** election safety (≤1 leader per term across nodes),
  checked after every event; state-machine safety (no two nodes apply different
  entries at the same index), checked on every apply. At the end of a run that
  completed all client operations: durability (every applied entry still exists
  on at least one node), then the linearizability checker. There is no separate
  log-matching monitor.
- **What has caught bugs:** in the November 2026 sweeps (each planted bug over
  3,000 seeds, and the §5.4.2 mutation over 20,000 seeds), every failure came
  from state-machine safety or durability. The linearizability checker has not
  yet caught a bug end to end; the runs that reached it were judged linearizable.
- **Recording:** the run produces a `Report { seed, outcome, trace,
  ops_completed, leader_changes, events_processed }`.
  `outcome ∈ { Ok, InvariantViolated(..), NonLinearizable(..), Stalled(..) }`;
  `Stalled` covers ops that never finished and a checker that hit its search
  budget. Anything but `Ok` is a failure.

## crate: deterrust-check (linearizability)
- A `History` is a list of completed operations,
  `Op { client, op, ret, call_ts, return_ts }`.
- Sequential model: a KV map spec.
- **Wing-Gong-Lowe (WGL)** search: repeatedly try to linearize a pending op at a
  "linearization point," apply it to the model, recurse; backtrack on failure;
  **memoize visited (model-state, pending-set) configurations** to prune the
  exponential search. The search stops after 5,000,000 visited configurations.
  The largest history in its tests has 400 operations.
- Returns `Linearizable`, `NotLinearizable { explanation }`, or
  `BudgetExceeded { explanation }`.
- Tested by 12 unit tests on hand-built histories, linearizable and not.

## crate: deterrust (CLI)
- `deterrust fuzz [--seeds 10000] [--start 0] [--nodes 5] [--ops 120] [--faults aggressive] [--bug none]`:
  sweep seeds (parallel across cores), report the lowest failing seed + outcome,
  exit 1 if any seed fails.
- `deterrust replay --seed 0xDEADBEEF [--nodes] [--ops] [--faults] [--bug] [-v]`:
  deterministically re-run one seed, print the event-trace timeline (the last
  40 lines unless `-v`), show the violation.
- `deterrust demo [--bug commit-single-ack] [--seeds 50000] [--nodes 5] [--ops 120]`:
  run with a planted bug enabled, find+replay it end-to-end.
- `deterrust check --seed <seed> [--nodes] [--ops] [--faults] [--bug]`: re-run a
  seed and print the linearizability verdict for its history, or the failure
  label if an invariant tripped first. It does not read a history file.

## Test strategy
- `raft`: 8 scenario tests in `crates/raft/tests/scenarios.rs`, driven through a
  hand-pumped FIFO message queue: single-node commit, three-node election,
  majority commit and propagation, follower log convergence, higher-term
  step-down, vote denied to a stale log, conflicting-entry truncation, and the
  §5.4.2 commit restriction.
- `sim`: determinism test (same seed ⇒ identical trace, outcome and counters);
  a clean network is OK on 40 seeds; the correct build survives 200 aggressive
  seeds at 80 ops; each planted bug is found within 3,000 seeds at 100 ops.
  Different seeds diverging is tested at the PRNG level. There is no
  property-testing framework; randomized coverage comes from these seeded
  sweeps and `deterrust fuzz`.
- `check`: 12 hand-built histories, linearizable and not.
- CI: `cargo test --workspace`, `cargo clippy --all-targets -- -D warnings`,
  `cargo fmt --check`, and a `fuzz` smoke of 10,000 seeds at 200 ops, deep enough
  that removing the §5.4.2 term check fails it (first failing seed 1370).
