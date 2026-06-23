# deterrust

**A from-scratch Raft key-value store, and the deterministic simulation harness that hunts its own consensus bugs — then replays any failure bit-for-bit from a single 64-bit seed.**

[![ci](https://github.com/kianis4/deterrust/actions/workflows/ci.yml/badge.svg)](https://github.com/kianis4/deterrust/actions/workflows/ci.yml)
[![license: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

> Most Raft side-projects stop at "it elected a leader." deterrust treats the
> consensus protocol as the *system under test* and makes the **test harness the
> product**. It runs an entire replicated cluster inside one deterministic,
> single-threaded simulator, injects partitions and clock skew and message loss,
> checks every run for **linearizability** violations, and when it finds one it
> prints a seed. Re-run that seed and the exact same bug reproduces, every time.

This is the methodology FoundationDB, TigerBeetle, and Antithesis use to ship
distributed systems people trust. deterrust is a small, readable, dependency-light
implementation of it you can hold in your head.

---

## The 30-second demo

```console
$ deterrust demo
▸ building cluster with a PLANTED bug (commit-on-single-ack) ...
▸ fuzzing seeds for a linearizability violation ...
  ✗ seed 0x00000000000004d2 — NON-LINEARIZABLE (acknowledged write lost)
▸ replaying seed 0x00000000000004d2 deterministically ...

  t=0      n0 ELECTION_TIMEOUT      becomes candidate (term 1)
  t=3      n0 -> n1,n2,n3,n4        RequestVote(term=1)
  t=7      n0 WINS                  leader (term 1)
  t=12     client c0 -> n0          Put(x=1)            [invoke]
  t=14     PARTITION {n0,n1} | {n2,n3,n4}
  t=15     n0 commit-on-single-ack  commit_index=1  ◀ BUG: only n1 acked
  t=16     client c0 <- n0          ok                  [return]   ← acked!
  t=40     {n2,n3,n4} elect n2      leader (term 2), no entry for x
  t=55     client c1 -> n2          Get(x) = ∅          ← write x=1 LOST
  ✗ history is NOT linearizable: c0's acknowledged Put(x=1) never happened

  reproduce: deterrust replay --seed 0x4d2
```

Every byte of that trace is a deterministic function of the seed. There is no
flakiness, because there are no real threads, no real clocks, and no real sockets.

---

## Why this is hard (and why Rust)

A distributed system is hard to test because its bugs hide in the *interleavings*
— the one-in-a-million ordering of a delayed vote, a healed partition, and a
client read. You cannot reliably reproduce those with real networking. The DST
answer is to make **all** nondeterminism injectable and seed-driven:

- **Sans-IO core.** The Raft node is a pure state machine —
  `step(&mut self, input) -> Vec<Output>`. It never blocks, never reads the
  clock, never touches a socket. It consumes timer ticks / messages / client
  proposals and emits send / reset-timer / apply / reply *intents*. The runtime
  decides whether to realize them over a simulated network or (future) a real
  one. This is exactly how `quinn-proto`, `rustls`, and TigerBeetle stay testable.
- **One seed → one universe.** A hand-written PRNG drives every latency, drop,
  duplication, reorder, partition heal, election-timeout jitter, and client
  interleaving. Same seed ⇒ byte-identical event trace.
- **Falsify, don't hope.** Each run is checked by a from-scratch
  Wing-Gong-Lowe linearizability checker plus continuous invariant monitors
  (election safety, log matching, state-machine safety, no-acked-write-lost).

Rust makes the injectable-everything design tractable: trait boundaries
(`Clock`/`Network`/`Storage`) with a deterministic implementation, zero ambient
global state, and a single-threaded driver with no data races by construction.

---

## What's inside

| crate | what it is |
|---|---|
| `deterrust-raft` | the pure Raft state machine — election, log replication, commit advancement, and the eight subtle safety rules |
| `deterrust-kv` | the replicated key-value application state machine |
| `deterrust-sim` | the deterministic runtime: virtual clock, seeded PRNG, network + fault model, the driver, invariant monitors, trace recording |
| `deterrust-check` | the Wing-Gong-Lowe linearizability checker |
| `deterrust` | the CLI: `fuzz`, `replay`, `demo`, `check` |

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the full design and the
exact correctness rules.

---

## Usage

```bash
# sweep 100k seeds across all cores, stop at the first violation
deterrust fuzz --seeds 100000 --nodes 5 --ops 200 --faults aggressive

# deterministically reproduce one seed with a full event timeline
deterrust replay --seed 0xDEADBEEF -v

# run the planted-bug end-to-end demo
deterrust demo

# check a recorded history for linearizability
deterrust check history.json
```

---

## Build & test

```bash
cargo test --workspace          # unit + scenario + property tests
cargo clippy --all-targets -- -D warnings
cargo run --release -p deterrust -- fuzz --seeds 1000
```

## Roadmap

Built tonight: deterministic runtime, correct minimal Raft (election + replication
+ commit), seeded fault-injection network, linearizability checker, fuzz/replay/demo.

Designed but not yet built (hooks noted in the code): log compaction + snapshots,
single-server membership changes, a real Tokio TCP transport reusing the identical
state machine, WGL pruning tuned for tens of thousands of ops, a TUI replay viewer,
and coverage-guided seed selection.

## License

MIT — see [LICENSE](./LICENSE).
