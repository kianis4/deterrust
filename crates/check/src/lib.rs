//! A from-scratch linearizability checker for key-value histories.
//!
//! This crate decides whether a recorded [`History`] of concurrent key-value
//! operations is *linearizable*: whether there exists a single total order of
//! the operations that (a) respects the real-time happens-before order and
//! (b) reproduces every operation's observed return value when replayed against
//! a sequential key-value model.
//!
//! # The Wing-Gong-Lowe (WGL) algorithm
//!
//! Naively, deciding linearizability means searching every permutation of the
//! operations that is consistent with the real-time order — exponential, and
//! intractable beyond a handful of ops. WGL (as implemented by Porcupine in Go
//! and Knossos in Clojure) makes the search practical with two ideas:
//!
//! 1. **Linearization points, chosen greedily.** Instead of guessing a full
//!    order up front, we repeatedly pick the *next* operation to linearize. An
//!    operation is eligible (a "minimal" op) only if its call timestamp is at
//!    or before the smallest return timestamp among all not-yet-linearized ops.
//!    Picking anything later would force an op that already returned to be
//!    ordered after one that had not yet been called — a real-time violation.
//!    For each minimal op we apply it to a clone of the model; if the model's
//!    produced result matches the op's observed return, we recurse with that op
//!    removed. On mismatch we backtrack and try the next minimal op.
//!
//! 2. **Memoization of visited configurations.** A configuration is the pair
//!    *(model state, set of remaining operations)*. Two different prefixes can
//!    lead to the same configuration; once a configuration has been proven
//!    unsatisfiable there is no point exploring it again. We record every
//!    visited configuration in a [`HashSet`] keyed by a stable, canonical
//!    serialization (sorted map entries + sorted remaining indices). This
//!    pruning is what collapses the exponential search to something tractable
//!    on well-behaved histories.
//!
//! The search is still worst-case exponential — that is inherent to the
//! problem — but the memo table makes realistic histories of a few thousand
//! ops checkable.
//!
//! # Example
//!
//! ```
//! use deterrust_check::{check, CheckResult, History, KvOp, KvRet, Op};
//!
//! let mut h = History::new();
//! h.record(Op {
//!     client: 0,
//!     op: KvOp::Put { key: "x".into(), val: "1".into() },
//!     ret: KvRet::PutOk,
//!     call_ts: 0,
//!     return_ts: 1,
//! });
//! h.record(Op {
//!     client: 0,
//!     op: KvOp::Get { key: "x".into() },
//!     ret: KvRet::GetVal(Some("1".into())),
//!     call_ts: 2,
//!     return_ts: 3,
//! });
//! assert_eq!(check(&h), CheckResult::Linearizable);
//! ```

use std::collections::{BTreeMap, HashSet};

/// A key-value operation invoked by a client.
#[derive(Clone, Debug, PartialEq)]
pub enum KvOp {
    /// Unconditionally set `key` to `val`.
    Put { key: String, val: String },
    /// Read the current value of `key`.
    Get { key: String },
    /// Compare-and-swap: set `key` to `new` only if its current value equals
    /// `expect` (`None` means "expect the key to be absent").
    Cas {
        key: String,
        expect: Option<String>,
        new: String,
    },
}

/// The value an operation observed when it returned.
#[derive(Clone, Debug, PartialEq)]
pub enum KvRet {
    /// A [`KvOp::Put`] completed.
    PutOk,
    /// A [`KvOp::Get`] observed this value (`None` = key absent).
    GetVal(Option<String>),
    /// A [`KvOp::Cas`] observed this success (`true`) or failure (`false`).
    CasOk(bool),
}

/// A single recorded operation with its real-time call/return window.
#[derive(Clone, Debug)]
pub struct Op {
    /// Identifier of the client that issued the operation.
    pub client: u64,
    /// The operation itself.
    pub op: KvOp,
    /// The value the operation observed on return.
    pub ret: KvRet,
    /// Logical time the operation was invoked.
    pub call_ts: u64,
    /// Logical time the operation returned (always `>= call_ts`).
    pub return_ts: u64,
}

/// An ordered log of operations as observed at runtime.
#[derive(Clone, Debug, Default)]
pub struct History {
    /// The recorded operations, in arbitrary (e.g. wall-clock) order.
    pub ops: Vec<Op>,
}

/// The verdict produced by [`check`].
#[derive(Clone, Debug, PartialEq)]
pub enum CheckResult {
    /// A valid linearization exists.
    Linearizable,
    /// No valid linearization exists; `explanation` names a witnessing op.
    NotLinearizable {
        /// Human-readable witness/reason for the violation.
        explanation: String,
    },
    /// The search exceeded its visited-state budget before deciding. The
    /// history is *not* reported as linearizable — the result is unknown.
    BudgetExceeded {
        /// Human-readable note on why the search was abandoned.
        explanation: String,
    },
}

impl History {
    /// Create an empty history.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append one operation to the history.
    pub fn record(&mut self, op: Op) {
        self.ops.push(op)
    }
}

/// The sequential key-value model: a sorted map from key to value.
type Model = BTreeMap<String, String>;

/// Apply `op` to `model`, mutating it on success, and return the [`KvRet`] the
/// model produces. The caller compares this against the op's observed return.
fn apply(model: &mut Model, op: &KvOp) -> KvRet {
    match op {
        KvOp::Put { key, val } => {
            model.insert(key.clone(), val.clone());
            KvRet::PutOk
        }
        KvOp::Get { key } => KvRet::GetVal(model.get(key).cloned()),
        KvOp::Cas { key, expect, new } => {
            let current = model.get(key).cloned();
            if &current == expect {
                model.insert(key.clone(), new.clone());
                KvRet::CasOk(true)
            } else {
                KvRet::CasOk(false)
            }
        }
    }
}

/// Build a stable, canonical byte key for the memo table from the model state
/// and the still-remaining op indices. Both halves are already sorted (the
/// `BTreeMap` is ordered by key; `remaining` is maintained ascending), so equal
/// configurations always serialize identically.
fn memo_key(model: &Model, remaining: &[usize]) -> Vec<u8> {
    let mut buf = Vec::new();
    for (k, v) in model {
        buf.extend_from_slice(&(k.len() as u64).to_le_bytes());
        buf.extend_from_slice(k.as_bytes());
        buf.extend_from_slice(&(v.len() as u64).to_le_bytes());
        buf.extend_from_slice(v.as_bytes());
    }
    // Separator that cannot collide with the length-prefixed section above.
    buf.push(0xff);
    for &idx in remaining {
        buf.extend_from_slice(&(idx as u64).to_le_bytes());
    }
    buf
}

/// Default ceiling on distinct configurations explored before giving up.
const DEFAULT_BUDGET: usize = 5_000_000;

struct Search<'a> {
    ops: &'a [Op],
    visited: HashSet<Vec<u8>>,
    budget: usize,
    exceeded: bool,
}

impl Search<'_> {
    /// Recursive WGL search. `remaining` is the ascending list of indices into
    /// `self.ops` not yet linearized. Returns `true` if a valid linearization
    /// of `remaining` exists from this `model` state.
    fn linearize(&mut self, model: &Model, remaining: &[usize]) -> bool {
        if remaining.is_empty() {
            return true;
        }
        if self.exceeded {
            return false;
        }

        let key = memo_key(model, remaining);
        if !self.visited.insert(key) {
            // Already explored this configuration and it failed; prune.
            return false;
        }
        if self.visited.len() > self.budget {
            self.exceeded = true;
            return false;
        }

        // An op is minimal (eligible to go next) iff its call_ts does not
        // exceed the minimum return_ts among the remaining ops — ordering it
        // first cannot violate real-time order.
        let min_return = remaining
            .iter()
            .map(|&i| self.ops[i].return_ts)
            .min()
            .expect("remaining is non-empty");

        for (pos, &idx) in remaining.iter().enumerate() {
            let op = &self.ops[idx];
            if op.call_ts > min_return {
                continue;
            }
            let mut next_model = model.clone();
            let produced = apply(&mut next_model, &op.op);
            if produced != op.ret {
                continue;
            }
            let mut next_remaining = Vec::with_capacity(remaining.len() - 1);
            next_remaining.extend_from_slice(&remaining[..pos]);
            next_remaining.extend_from_slice(&remaining[pos + 1..]);
            if self.linearize(&next_model, &next_remaining) {
                return true;
            }
            if self.exceeded {
                return false;
            }
        }
        false
    }
}

/// Diagnose a failed search by naming a concrete operation whose observed
/// return is the most plausible culprit. We replay the history in real-time
/// order (call_ts, then return_ts) and report the first op whose observed
/// result disagrees with that simple sequential replay — a strong hint even
/// though linearizability allows reorderings.
fn explain_failure(history: &History) -> String {
    let mut order: Vec<usize> = (0..history.ops.len()).collect();
    order.sort_by_key(|&i| (history.ops[i].call_ts, history.ops[i].return_ts, i));

    let mut model = Model::new();
    for &i in &order {
        let op = &history.ops[i];
        let produced = apply(&mut model.clone(), &op.op);
        if produced != op.ret {
            return format!(
                "client {}'s {:?} observed {:?} but no valid linearization can \
                 reconcile it (a real-time replay produces {:?})",
                op.client, op.op, op.ret, produced
            );
        }
        // Commit the op to the running model so later ops see its effect.
        apply(&mut model, &op.op);
    }

    "no total order consistent with the real-time order reproduces every \
     observed return value (a non-atomic / stale-read style violation)"
        .to_string()
}

/// Decide whether `history` is linearizable against the sequential key-value
/// model.
///
/// Returns [`CheckResult::Linearizable`] if a valid total order exists,
/// [`CheckResult::NotLinearizable`] with a human-readable witness otherwise, or
/// [`CheckResult::BudgetExceeded`] if the search ran past its safety bound
/// without deciding (never silently reported as linearizable).
pub fn check(history: &History) -> CheckResult {
    let remaining: Vec<usize> = (0..history.ops.len()).collect();
    let mut search = Search {
        ops: &history.ops,
        visited: HashSet::new(),
        budget: DEFAULT_BUDGET,
        exceeded: false,
    };
    let model = Model::new();

    if search.linearize(&model, &remaining) {
        return CheckResult::Linearizable;
    }
    if search.exceeded {
        return CheckResult::BudgetExceeded {
            explanation: format!(
                "checker budget exceeded after exploring {} configurations; \
                 verdict unknown (history not confirmed linearizable)",
                search.visited.len()
            ),
        };
    }
    CheckResult::NotLinearizable {
        explanation: explain_failure(history),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(client: u64, key: &str, val: &str, call_ts: u64, return_ts: u64) -> Op {
        Op {
            client,
            op: KvOp::Put {
                key: key.into(),
                val: val.into(),
            },
            ret: KvRet::PutOk,
            call_ts,
            return_ts,
        }
    }

    fn get(client: u64, key: &str, observed: Option<&str>, call_ts: u64, return_ts: u64) -> Op {
        Op {
            client,
            op: KvOp::Get { key: key.into() },
            ret: KvRet::GetVal(observed.map(|s| s.to_string())),
            call_ts,
            return_ts,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn cas(
        client: u64,
        key: &str,
        expect: Option<&str>,
        new: &str,
        observed: bool,
        call_ts: u64,
        return_ts: u64,
    ) -> Op {
        Op {
            client,
            op: KvOp::Cas {
                key: key.into(),
                expect: expect.map(|s| s.to_string()),
                new: new.into(),
            },
            ret: KvRet::CasOk(observed),
            call_ts,
            return_ts,
        }
    }

    fn is_lin(r: &CheckResult) -> bool {
        matches!(r, CheckResult::Linearizable)
    }

    fn is_not_lin(r: &CheckResult) -> bool {
        matches!(r, CheckResult::NotLinearizable { .. })
    }

    // 1. Empty history is trivially linearizable.
    #[test]
    fn empty_history_is_linearizable() {
        let h = History::new();
        assert_eq!(check(&h), CheckResult::Linearizable);
    }

    // 2. A Put followed by a Get that observes the written value.
    #[test]
    fn put_then_get_observes_value() {
        let mut h = History::new();
        h.record(put(0, "x", "1", 0, 1));
        h.record(get(0, "x", Some("1"), 2, 3));
        assert!(is_lin(&check(&h)));
    }

    // 3. A classic linearizable concurrent history: overlapping ops on distinct
    //    keys can be ordered either way.
    #[test]
    fn overlapping_ops_orderable() {
        let mut h = History::new();
        // Both invoked at t=0, both return at t=5 (fully concurrent).
        h.record(put(0, "x", "1", 0, 5));
        h.record(put(1, "y", "2", 0, 5));
        h.record(get(0, "x", Some("1"), 6, 7));
        h.record(get(1, "y", Some("2"), 6, 7));
        assert!(is_lin(&check(&h)));
    }

    // 4. An acknowledged Put(x=1) that returned, then a strictly-later
    //    non-overlapping Get(x) observing None — impossible.
    #[test]
    fn ack_put_then_get_none_is_not_linearizable() {
        let mut h = History::new();
        h.record(put(0, "x", "1", 0, 1));
        h.record(get(0, "x", None, 2, 3));
        let r = check(&h);
        assert!(is_not_lin(&r), "expected NotLinearizable, got {r:?}");
    }

    // 5. Stale read: a newer write completed, but a strictly-later read still
    //    observes the old value.
    #[test]
    fn stale_read_after_newer_write_is_not_linearizable() {
        let mut h = History::new();
        h.record(put(0, "x", "1", 0, 1));
        h.record(put(0, "x", "2", 2, 3));
        // Get is invoked after the second Put returned, yet sees "1".
        h.record(get(1, "x", Some("1"), 4, 5));
        assert!(is_not_lin(&check(&h)));
    }

    // 6. CAS success and failure correctness against the model.
    #[test]
    fn cas_success_and_failure() {
        // Successful CAS: key absent, expect None, swap to "1".
        let mut h = History::new();
        h.record(cas(0, "x", None, "1", true, 0, 1));
        h.record(get(0, "x", Some("1"), 2, 3));
        assert!(is_lin(&check(&h)), "expected linearizable CAS-success path");

        // Failing CAS: key holds "1", expect "9", so the swap must fail and the
        // value stays "1".
        let mut h2 = History::new();
        h2.record(put(0, "x", "1", 0, 1));
        h2.record(cas(0, "x", Some("9"), "2", false, 2, 3));
        h2.record(get(0, "x", Some("1"), 4, 5));
        assert!(
            is_lin(&check(&h2)),
            "expected linearizable CAS-failure path"
        );

        // A CAS that *claims* success but couldn't have (wrong expect) — not
        // linearizable.
        let mut h3 = History::new();
        h3.record(put(0, "x", "1", 0, 1));
        h3.record(cas(0, "x", Some("9"), "2", true, 2, 3));
        assert!(is_not_lin(&check(&h3)));
    }

    // 7. A Get overlapping a Put may observe EITHER the old or the new value;
    //    both must be accepted as linearizable.
    #[test]
    fn get_overlapping_put_either_value() {
        // Observe the OLD value (key absent): Get linearized before Put.
        let mut h_old = History::new();
        h_old.record(put(0, "x", "1", 0, 10));
        h_old.record(get(1, "x", None, 0, 10));
        assert!(is_lin(&check(&h_old)), "old-value reading must linearize");

        // Observe the NEW value: Get linearized after Put.
        let mut h_new = History::new();
        h_new.record(put(0, "x", "1", 0, 10));
        h_new.record(get(1, "x", Some("1"), 0, 10));
        assert!(is_lin(&check(&h_new)), "new-value reading must linearize");
    }

    // 8. The non-atomic register violation: each read looks fine alone, but no
    //    single linearization point for the write satisfies both. Writer puts
    //    x=2 (x already 1). Reader A returns seeing the NEW value 2; Reader B,
    //    invoked AFTER A returned, sees the OLD value 1. Once the register is
    //    observed to be 2, no strictly-later read can legitimately see 1.
    #[test]
    fn non_atomic_register_is_not_linearizable() {
        let mut h = History::new();
        h.record(put(0, "x", "1", 0, 1)); // establish x = 1
        h.record(put(0, "x", "2", 2, 12)); // concurrent writer: x -> 2
        h.record(get(1, "x", Some("2"), 5, 6)); // Reader A sees the NEW value
        h.record(get(2, "x", Some("1"), 7, 8)); // Reader B (later) sees the OLD value
        let r = check(&h);
        assert!(is_not_lin(&r), "expected NotLinearizable, got {r:?}");
    }

    // Two concurrent CAS-from-absent that both claim success: impossible,
    // because the first to linearize creates the key and the second must fail.
    #[test]
    fn double_cas_from_absent_is_not_linearizable() {
        let mut h = History::new();
        h.record(cas(0, "a", None, "1", true, 0, 10));
        h.record(cas(1, "a", None, "2", true, 0, 10));
        let r = check(&h);
        assert!(
            is_not_lin(&r),
            "two CAS-from-None successes must not linearize"
        );
    }

    // A larger well-behaved (sequential) history to exercise efficiency.
    #[test]
    fn sequential_many_ops_linearizable() {
        let mut h = History::new();
        let mut t = 0u64;
        for i in 0..200u64 {
            let v = i.to_string();
            h.record(put(0, "k", &v, t, t + 1));
            t += 2;
            h.record(get(0, "k", Some(&v), t, t + 1));
            t += 2;
        }
        assert!(is_lin(&check(&h)));
    }

    // The budget path must be distinct from Linearizable: a search that cannot
    // complete reports BudgetExceeded, never a false "Linearizable".
    #[test]
    fn budget_exceeded_is_distinct_from_linearizable() {
        // Many fully-concurrent independent puts on distinct keys: the honest
        // verdict is Linearizable, but with a tiny budget the search bails out
        // rather than lying.
        let mut h = History::new();
        for i in 0..20u64 {
            let k = format!("k{i}");
            h.record(put(i, &k, "1", 0, 100));
            h.record(get(i, &k, Some("1"), 0, 100));
        }
        let remaining: Vec<usize> = (0..h.ops.len()).collect();
        let mut search = Search {
            ops: &h.ops,
            visited: HashSet::new(),
            budget: 3, // absurdly small on purpose
            exceeded: false,
        };
        let model = Model::new();
        let ok = search.linearize(&model, &remaining);
        assert!(!ok);
        assert!(search.exceeded, "tiny budget must trip the exceeded flag");
    }

    // The public NotLinearizable explanation names the culprit op.
    #[test]
    fn explanation_is_helpful() {
        let mut h = History::new();
        h.record(put(0, "x", "1", 0, 1));
        h.record(get(0, "x", None, 2, 3));
        match check(&h) {
            CheckResult::NotLinearizable { explanation } => {
                assert!(
                    explanation.contains("client 0"),
                    "explanation should name the client: {explanation}"
                );
            }
            other => panic!("expected NotLinearizable, got {other:?}"),
        }
    }
}
