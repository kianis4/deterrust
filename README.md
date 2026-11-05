# deterrust

**A from-scratch Raft key-value store, and a deterministic simulation harness that hunts for consensus bugs and replays any failure exactly from a single 64-bit seed.**

[![ci](https://github.com/kianis4/deterrust/actions/workflows/ci.yml/badge.svg)](https://github.com/kianis4/deterrust/actions/workflows/ci.yml)
[![license: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

> deterrust treats the consensus protocol as the *system under test*. It runs an
> entire replicated cluster inside one deterministic, single-threaded simulator,
> injects message loss, duplication, random delay and network partitions, checks
> safety invariants while the run executes, and checks the client history for
> **linearizability** at the end. When a run fails it prints the seed. Re-run that
> seed and the exact same failure reproduces, every time.

This is deterministic simulation testing, the methodology behind FoundationDB's
and TigerBeetle's own testing and the Antithesis platform. deterrust is a small, readable, dependency-light
implementation of it you can hold in your head.

---

## The demo

Real output of `deterrust demo` (banner and closing lines trimmed):

```console
$ deterrust demo
① building a 5-node cluster with the bug enabled
② fuzzing up to 50000 seeds for a safety violation …

   ✗ found it: seed 0x8 breaks the cluster.

③ replaying seed 0x8 deterministically — same bug, every time:

  t=17       n1 starts election (term 1)
  t=22       n0 starts election (term 1)
  t=58       n1 BECAME LEADER (term 1)
  t=282      PARTITION { n0:1 n1:0 n2:1 n3:1 n4:0 }
  t=434      n2 starts election (term 2)
  t=471      n2 BECAME LEADER (term 2)
  t=1120     PARTITION HEALED
  t=1201     ✗ state-machine safety violated at log index 4: (1, 2, 2) vs (2, 1, 20)

✗ seed 0x0000000000000008 (8)  —  INVARIANT-VIOLATED  [54 ops, 2 leader changes, 731 events]
  → state-machine safety violated at log index 4: (1, 2, 2) vs (2, 1, 20)

   reproduce forever: deterrust replay --seed 0x8 --bug commit-single-ack -v
```

The demo turns on a planted bug, `commit-single-ack`: the leader's commit
threshold drops from a majority to 1, so it commits an entry on its own copy
before any follower has it. The fuzzer searches seeds from 0 upward and the
lowest failing seed is 0x8. In that run n1 leads term 1, a partition leaves n1
and n4 on one side, and the other three nodes elect n2 for term 2. Two nodes end up
applying different entries at log index 4, one written in term 1 and one in term 2,
and the state-machine safety monitor stops the run. Each tuple is the applied
entry's (term, client, request sequence number).

Every line of that trace is a deterministic function of the seed. There is no
flakiness, because there are no real threads, no real clocks, and no real sockets.

---

## Why this is hard (and why Rust)

A distributed system is hard to test because its bugs hide in the *interleavings*:
the one-in-a-million ordering of a delayed vote, a healed partition, and a
client read. You cannot reliably reproduce those with real networking. The DST
answer is to make **all** nondeterminism injectable and seed-driven:

- **Sans-IO core.** The Raft node is a pure state machine.
  `RaftNode::step(&mut self, input: Input<C>) -> Vec<Output<C>>` in
  `crates/raft/src/node.rs` is the only method that changes its state. It never
  blocks, never reads a clock, never touches a socket, and never draws a random
  number. Inputs are an election timeout, a heartbeat tick, a delivered peer
  message, or a client proposal. Outputs are intents: send a message, arm the
  election timer, arm the heartbeat timer, apply a committed entry, or redirect a
  client to the leader. The simulator decides how to realize them. A real network
  runtime could reuse the same state machine, but none exists yet. `quinn-proto`
  and `rustls` use the same sans-IO style.
- **One seed, one universe.** A hand-written SplitMix64 PRNG
  (`crates/sim/src/rng.rs`) drives every message latency, drop, duplication,
  partition start and heal, election-timeout jitter, and client operation.
  Random latency is also what reorders messages. Same seed, same event trace.
- **Falsify, don't hope.** Every run is checked by invariant monitors and, if it
  gets that far, by a from-scratch Wing-Gong-Lowe linearizability checker. See
  [What catches bugs today](#what-catches-bugs-today).

None of this needs trait abstractions. The node is generic only over its command
type, and the simulator owns virtual time, the network model, and the one seeded
RNG directly. Each run is single-threaded with no global state; `deterrust fuzz`
gets its speed by running independent seeds on separate threads.

---

## What catches bugs today

Each simulated run (`crates/sim/src/lib.rs`) is checked in four places:

| check | when | what it asserts |
|---|---|---|
| election safety | after every event | no two nodes are leader in the same term |
| state-machine safety | on every apply | every node that applies log index *i* applies the same entry |
| durability | once, at the end of the run | every entry any node applied still exists on at least one node |
| linearizability | once, at the end, if all client ops completed | the WGL checker finds a valid linearization of the client history |

A run whose client operations do not finish within the time budget, or whose
history exhausts the checker's search budget, is reported as `STALLED`. That is a
failure, never a pass. There is no separate log-matching monitor.

So far every bug the harness has caught was caught by the state-machine safety
monitor or the end-of-run durability check. In the sweeps run for the November
2026 truth pass (each of the three planted bugs over 3,000 seeds at 120 ops, and
the §5.4.2 mutation described below over 20,000 seeds at 200 ops), 4,106 runs
failed on state-machine safety, 19 on durability, none on election safety or
linearizability, and none stalled.

The linearizability checker has not caught a bug end to end. It runs at the end
of every run that passes the monitors, and in every sweep above the runs that
reached it were judged linearizable. Its ability to reject bad histories is
tested by 12 unit tests on hand-built histories in `crates/check/src/lib.rs`,
including a lost acknowledged write, a stale read, a non-atomic register, and a
double CAS.

Planted bugs are runtime flags (`BugConfig` in `crates/raft/src/types.rs`),
selected with `--bug`:

- `commit-single-ack`: the leader commits on its own copy instead of a majority.
- `vote-no-log-check`: a node grants its vote without the §5.4.1 up-to-date log check.
- `stale-append`: a follower accepts AppendEntries from a leader with an older term.

---

## What's inside

| crate | what it is |
|---|---|
| `deterrust-raft` | the pure Raft state machine: election, log replication, commit advancement, and the eight safety rules listed in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) |
| `deterrust-kv` | the replicated key-value application state machine, with per-client sessions for exactly-once application |
| `deterrust-sim` | the deterministic runtime: virtual clock, seeded PRNG, network and fault model, the driver, invariant monitors, trace recording |
| `deterrust-check` | the Wing-Gong-Lowe linearizability checker |
| `deterrust` | the CLI: `fuzz`, `replay`, `demo`, `check` |

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the full design and the
exact correctness rules.

---

## Usage

Run any command with `cargo run --release -p deterrust -- <command>`.

```bash
# sweep seeds across all cores; report the lowest failing seed and exit 1 if any fails
deterrust fuzz --seeds 100000 --nodes 5 --ops 200 --faults aggressive

# the same sweep with a planted bug turned on
deterrust fuzz --seeds 10000 --bug commit-single-ack

# deterministically re-run one seed and print its full event timeline
deterrust replay --seed 0x8 --bug commit-single-ack -v

# plant a bug, find the lowest failing seed, and replay it
deterrust demo

# re-run one seed and print the linearizability verdict for its client history
deterrust check --seed 0x8
```

`replay` and `check` reproduce a run only with the same `--nodes`, `--ops`,
`--faults` and `--bug` it was found with (defaults: 5, 120, aggressive, none).
`check` re-runs a seed; it does not read a history file.

---

## Build & test

```bash
cargo test --workspace          # unit, scenario, and seeded-sweep tests
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
cargo run --release -p deterrust -- fuzz --seeds 10000 --nodes 5 --ops 200
```

There is no property-testing framework. Randomized coverage comes from seeded
sweeps over the simulator, in the `deterrust-sim` tests and in `deterrust fuzz`.

## How this is verified

- **CI** (`.github/workflows/ci.yml`) runs on every push to main and every pull
  request: `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test
  --workspace`, and a fuzz smoke of 10,000 seeds at 200 client operations each.
- **Tests.** Eight Raft scenario tests drive nodes through a hand-pumped message
  queue (`crates/raft/tests/scenarios.rs`). The simulator tests check that running a
  seed twice produces the same trace, that the correct build survives 200
  aggressive seeds, and that each planted bug is found within 3,000 seeds. The checker, KV store and PRNG have their own unit tests.
- **Mutation check.** A test is only worth something if it fails when the code is
  wrong. Replace the §5.4.2 current-term condition in commit advancement
  (`crates/raft/src/node.rs:469`) with `true` and both the scenario test and the CI fuzz
  smoke must fail:

  ```bash
  perl -pi -e 's/if self\.term_at\(n\) == self\.current_term \{/if true {/' crates/raft/src/node.rs
  cargo test -p deterrust-raft --test scenarios
  # fails: leader_does_not_commit_previous_term_entry_by_count_alone (left: 1, right: 0)
  cargo run --release -p deterrust -- fuzz --seeds 10000 --nodes 5 --ops 200
  # fails at seed 0x55a (1370): state-machine safety violated at log index 188
  git checkout -- crates/raft/src/node.rs
  ```

  Before November 2026 neither check caught this mutation. All 36 tests passed
  with the rule removed. The scenario test meant to guard it committed its entry
  before the leader change, so it could not fail. The old CI smoke (500 seeds at 80
  ops) never reached a failing seed; the first one at 80 ops is 5533.

## Roadmap

The first version was produced on 2026-06-23 by an autonomous agent run in the
author's own Claude Code harness (scaffold to green CI in 34 minutes). In
November 2026 a truth pass found a test that could not fail, proved it with a
mutation, and closed it. The same pass corrected this README and
`docs/ARCHITECTURE.md` where they described things the code does not do.

Built: deterministic runtime, minimal Raft (election, replication, commit),
seeded fault-injection network, linearizability checker, and the `fuzz`,
`replay`, `demo` and `check` commands.

Not built yet: log compaction and snapshots, single-server membership changes, a
real Tokio TCP transport reusing the identical state machine, WGL pruning tuned
for tens of thousands of ops, a TUI replay viewer, and coverage-guided seed
selection.

## License

MIT. See [LICENSE](./LICENSE).
