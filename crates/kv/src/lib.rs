//! # deterrust-kv
//!
//! The replicated application that sits on top of [`deterrust_raft`]. Raft
//! decides *what order* commands apply in; this crate decides *what they do*.
//!
//! It is an ordinary deterministic key-value map plus a per-client **session
//! table** that makes command application **exactly-once**. Raft replicates the
//! same log everywhere, but a client that times out and retries can land its
//! command in the log twice; the session table returns the cached reply for any
//! already-seen sequence number so a retried `Put` (or a counter increment via
//! `Cas`) never takes effect twice. Without this, the system would not be
//! linearizable.

use std::collections::BTreeMap;
use std::collections::HashMap;

use deterrust_raft::ClientId;

/// A command replicated through Raft. This is the `C` in `RaftNode<C>`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KvCommand {
    Put {
        key: String,
        val: String,
    },
    Get {
        key: String,
    },
    Cas {
        key: String,
        expect: Option<String>,
        new: String,
    },
}

/// The observed result of applying a [`KvCommand`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KvReply {
    PutOk,
    GetVal(Option<String>),
    CasOk(bool),
}

/// The deterministic key-value state machine with exactly-once client sessions.
#[derive(Default)]
pub struct KvStateMachine {
    map: BTreeMap<String, String>,
    /// client -> (highest applied seq, cached reply for that seq)
    sessions: HashMap<ClientId, (u64, KvReply)>,
}

impl KvStateMachine {
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply a committed command exactly once for `(client, seq)`.
    ///
    /// If this `(client, seq)` (or an older seq from the same client) has already
    /// been applied, the cached reply is returned and the map is left untouched.
    pub fn apply(&mut self, client: ClientId, seq: u64, cmd: &KvCommand) -> KvReply {
        if let Some((last_seq, last_reply)) = self.sessions.get(&client) {
            if seq <= *last_seq {
                return last_reply.clone();
            }
        }
        let reply = match cmd {
            KvCommand::Put { key, val } => {
                self.map.insert(key.clone(), val.clone());
                KvReply::PutOk
            }
            KvCommand::Get { key } => KvReply::GetVal(self.map.get(key).cloned()),
            KvCommand::Cas { key, expect, new } => {
                let current = self.map.get(key).cloned();
                if &current == expect {
                    self.map.insert(key.clone(), new.clone());
                    KvReply::CasOk(true)
                } else {
                    KvReply::CasOk(false)
                }
            }
        };
        self.sessions.insert(client, (seq, reply.clone()));
        reply
    }

    /// Read a key directly (for invariant checks; not a linearizable client op).
    pub fn get(&self, key: &str) -> Option<&String> {
        self.map.get(key)
    }

    /// A stable digest of the visible state, used to detect divergence between
    /// replicas that have applied the same committed prefix.
    pub fn snapshot(&self) -> Vec<(String, String)> {
        self.map
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(k: &str, v: &str) -> KvCommand {
        KvCommand::Put {
            key: k.into(),
            val: v.into(),
        }
    }

    #[test]
    fn put_then_get() {
        let mut sm = KvStateMachine::new();
        assert_eq!(sm.apply(ClientId(0), 1, &put("x", "1")), KvReply::PutOk);
        assert_eq!(
            sm.apply(ClientId(0), 2, &KvCommand::Get { key: "x".into() }),
            KvReply::GetVal(Some("1".into()))
        );
    }

    #[test]
    fn get_absent_key() {
        let mut sm = KvStateMachine::new();
        assert_eq!(
            sm.apply(
                ClientId(0),
                1,
                &KvCommand::Get {
                    key: "missing".into()
                }
            ),
            KvReply::GetVal(None)
        );
    }

    #[test]
    fn duplicate_request_is_deduplicated() {
        let mut sm = KvStateMachine::new();
        // Same (client, seq) applied twice — second time must NOT re-run.
        let cas = KvCommand::Cas {
            key: "n".into(),
            expect: None,
            new: "1".into(),
        };
        assert_eq!(sm.apply(ClientId(7), 1, &cas), KvReply::CasOk(true));
        // A retry of the SAME seq returns the cached reply, even though the
        // precondition (key absent) is no longer true.
        assert_eq!(sm.apply(ClientId(7), 1, &cas), KvReply::CasOk(true));
        // The value was set exactly once.
        assert_eq!(sm.get("n"), Some(&"1".to_string()));
    }

    #[test]
    fn cas_respects_precondition() {
        let mut sm = KvStateMachine::new();
        sm.apply(ClientId(0), 1, &put("k", "a"));
        // Wrong expectation fails and does not mutate.
        assert_eq!(
            sm.apply(
                ClientId(0),
                2,
                &KvCommand::Cas {
                    key: "k".into(),
                    expect: Some("WRONG".into()),
                    new: "b".into()
                }
            ),
            KvReply::CasOk(false)
        );
        assert_eq!(sm.get("k"), Some(&"a".to_string()));
        // Correct expectation succeeds.
        assert_eq!(
            sm.apply(
                ClientId(0),
                3,
                &KvCommand::Cas {
                    key: "k".into(),
                    expect: Some("a".into()),
                    new: "b".into()
                }
            ),
            KvReply::CasOk(true)
        );
        assert_eq!(sm.get("k"), Some(&"b".to_string()));
    }

    #[test]
    fn replicas_with_same_log_have_same_snapshot() {
        let mut a = KvStateMachine::new();
        let mut b = KvStateMachine::new();
        for (i, (k, v)) in [("x", "1"), ("y", "2"), ("x", "3")].iter().enumerate() {
            let cmd = put(k, v);
            a.apply(ClientId(0), i as u64 + 1, &cmd);
            b.apply(ClientId(0), i as u64 + 1, &cmd);
        }
        assert_eq!(a.snapshot(), b.snapshot());
        assert_eq!(a.get("x"), Some(&"3".to_string()));
    }
}
