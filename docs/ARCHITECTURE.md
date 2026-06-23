# deterrust — architecture & engineering spec

> A from-scratch Raft-replicated key-value store wrapped in a **deterministic
> simulation testing (DST)** harness. The consensus is the system-under-test;
> the harness is the product. It hunts linearizability violations under injected
> faults and **replays any failure bit-for-bit from a single 64-bit seed.**

This document is the authoritative contract for implementers. Every crate is
synchronous, allocation-light, and free of ambient nondeterminism (no
`SystemTime::now`, no `rand::thread_rng`, no real threads, no real I/O in the
core). All nondeterminism is funneled through one seed.

## Design pillars

1. **Sans-IO Raft.** The Raft node is a pure state machine:
   `fn step(&mut self, input: Input, now: LogicalTime) -> Vec<Output>`.
   It never blocks, never reads a clock, never touches a socket. Inputs are
   timer ticks, inbound messages, and client proposals. Outputs are
   send-message, reset-timer, apply-to-state-machine, and client-response
   intents. The runtime decides how to realize outputs (simulated or real).
   This is how `quinn-proto`, `rustls`, and TigerBeetle achieve testability.
2. **One seed → one universe.** Given a `u64` seed, the entire cluster run —
   message delays, drops, duplications, reordering, partitions, election-timeout
   jitter, client op interleaving — is a deterministic function. Re-running the
   seed reproduces the identical event trace.
3. **Falsify, don't assert.** Correctness is checked by a linearizability
   checker over the observed client history, plus invariant checks
   (election safety, log matching, no-lost-acked-write), not by hopeful unit
   asserts. The deliverable demo is "the harness caught a bug; here is the
   seed; watch it replay."

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

Dependency direction (no cycles): `kv` → `raft`; `sim` → {`raft`, `kv`};
`cli` → {`sim`, `check`, `raft`, `kv`}; `check` is standalone.

## crate: deterrust-raft (the hard core)

Implements the minimal **correct** subset of Raft (Ongaro & Ousterhout, Fig. 2):
leader election + log replication + commit advancement + safety. Snapshots and
membership changes are out of scope tonight but the module notes their hooks.

### State (per node)
- Persistent: `current_term: u64`, `voted_for: Option<NodeId>`, `log: Vec<LogEntry>`
  (1-indexed conceptually; index 0 is a sentinel). `LogEntry { term, command }`.
- Volatile: `commit_index: u64`, `last_applied: u64`, `role: Follower|Candidate|Leader`.
- Leader-only volatile: `next_index: HashMap<NodeId,u64>`, `match_index: HashMap<NodeId,u64>`.
- Timing: `election_deadline` and `heartbeat_deadline` are expressed as logical
  ticks; the node asks the runtime to reset timers via `Output::ResetTimer`.

### Inputs / Outputs
```rust
enum Input {
    Tick,                                    // logical clock advanced; maybe time out
    Message { from: NodeId, msg: RaftMessage },
    Propose { client: ClientId, command: Command }, // client write/read
}
enum Output {
    Send { to: NodeId, msg: RaftMessage },
    ResetElectionTimer { jitter_request: () }, // runtime picks timeout via seeded rng
    ResetHeartbeatTimer,
    Apply { index: u64, term: u64, command: Command }, // state machine apply
    ClientResult { client: ClientId, result: ClientReply }, // committed / not-leader / redirect
}
enum RaftMessage {
    RequestVote { term, candidate_id, last_log_index, last_log_term },
    RequestVoteReply { term, vote_granted },
    AppendEntries { term, leader_id, prev_log_index, prev_log_term, entries, leader_commit },
    AppendEntriesReply { term, success, match_index }, // match_index hint for fast convergence
}
```

### The ~8 subtle correctness rules implementers MUST honor
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
   Never truncate committed entries.
5. **Commit advancement (leader):** advance `commit_index` to the highest `N`
   such that a majority of `match_index >= N` **AND `log[N].term == current_term`**
   (the §5.4.2 restriction — never commit a previous-term entry by counting
   replicas; it commits indirectly once a current-term entry above it commits).
6. **Follower commit:** `commit_index = min(leader_commit, index_of_last_new_entry)`.
7. **Election safety:** at most one leader per term — enforced by the
   at-most-one-vote-per-term rule (#2) + majority.
8. **Apply only committed, in order:** apply entries `last_applied+1..=commit_index`
   exactly once, in index order, emitting `Output::Apply`.

### Determinism contract
- The node never calls rng or clock. Election-timeout jitter is requested from
  the runtime (`ResetElectionTimer`), which draws from the seeded rng. The node
  is a deterministic function of its inputs.

### Planted bugs (feature-flagged, for the demo)
Behind cargo features so the "watch the harness catch it" demo is reliable:
- `bug-commit-single-ack`: leader advances `commit_index` on a single follower
  ack instead of a majority → lost writes / divergence under partition.
- `bug-ignore-term-on-append`: follower accepts AppendEntries from a stale-term
  leader → split brain.
- `bug-stale-read`: a deposed leader serves a read locally without confirming
  leadership → stale read, a linearizability violation.

## crate: deterrust-kv
- `Command::{ Put{key,val}, Get{key}, Cas{key,expect,val} }`, `ClientReply`.
- A `KvStateMachine { map: BTreeMap<String,String> }` applying committed commands
  deterministically. Reads go through the log (linearizable reads) in MVP;
  lease/read-index reads are a documented optimization, not built tonight.

## crate: deterrust-sim (the harness)
- **Virtual clock:** `LogicalTime(u64)` in abstract ticks. Time only advances by
  popping the next scheduled event.
- **Seeded RNG:** a small, explicit PRNG (SplitMix64 / PCG — implement, don't
  pull a crate, it's part of the point). All randomness (delays, drop decisions,
  timeout jitter, client interleaving) draws from this one stream.
- **Event queue:** a binary heap keyed by `(deliver_at, seq)`; `seq` is a
  monotonic tiebreaker so equal-time events have a deterministic order. Events:
  message delivery, timer fire, client op start.
- **Network model + fault injection** (all seeded, parameterized by a `FaultPlan`):
  per-link latency range, drop probability, duplication probability, reorder
  (via randomized delay), and **partitions** (a set of blocked node-pairs with
  scheduled heal times). A partition simply means messages across the cut are
  dropped while active.
- **Driver:** owns N raft nodes + KV state machines + client model. Loop: pop
  event → feed node `step` → realize outputs (schedule sends with seeded
  latency/drop/dup, reset timers with seeded jitter, apply to KV, record client
  replies) → record into the **event trace** and the **history** → repeat until
  the op budget is exhausted or an invariant trips.
- **Invariant monitors** (checked continuously): election safety (≤1 leader per
  term across nodes), log-matching (committed prefixes agree), state-machine
  safety (no two nodes apply different commands at the same index), and
  no-acked-write-lost.
- **Recording:** the run produces a `Report { seed, fault_plan, history, trace,
  outcome }`. `outcome ∈ { Ok, InvariantViolated(..), NonLinearizable(..) }`.

## crate: deterrust-check (linearizability)
- Records a `History` of `Event::{ Invoke{client,op}, Return{client,result} }`.
- Sequential model: a single-register / KV map spec.
- **Wing-Gong-Lowe (WGL)** search: repeatedly try to linearize a pending op at a
  "linearization point," apply it to the model, recurse; backtrack on failure;
  **memoize visited (model-state, pending-set) configurations** to prune the
  exponential search. Handles a few thousand ops.
- Returns `Linearizable` or `NotLinearizable { witness }`.

## crate: deterrust (CLI)
- `deterrust fuzz --seeds N [--nodes 5] [--ops 200] [--faults aggressive]`:
  sweep seeds (parallel across cores), report the first failing seed + outcome.
- `deterrust replay --seed 0xDEADBEEF [-v]`: deterministically re-run one seed,
  print the event-trace timeline (ASCII), show the violation.
- `deterrust demo`: run with a planted bug enabled, find+replay it end-to-end.
- `deterrust check <history.json>`: run the linearizability checker standalone.

## Test strategy
- `raft`: unit tests for every Fig. 2 rule; canonical scenarios (split vote,
  log backfill, leader crash + re-election, conflicting-entry truncation,
  §5.4.2 commit restriction).
- `sim`: determinism test — same seed ⇒ byte-identical trace; different seeds ⇒
  different traces; a planted bug ⇒ reliably caught; property test (proptest or
  hand-rolled seed sweep): "no acknowledged write lost across 1000 partition
  seeds" on the correct build.
- `check`: known linearizable/non-linearizable histories from the literature.
- CI: `cargo test --workspace`, `cargo clippy --all-targets -- -D warnings`,
  `cargo fmt --check`, and a short `fuzz` smoke (a few hundred seeds).
