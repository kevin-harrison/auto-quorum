use omnipaxos::{macros::Entry, storage::Snapshot};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type CommandId = u64;
pub type ClientId = u64;

#[derive(Debug, Clone, Entry, Serialize, Deserialize)]
pub struct Command {
    pub id: CommandId,
    pub client_id: ClientId,
    pub command: KVCommand,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KVCommand {
    Put(String, String),
    Delete(String),
    // Get(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KVSnapshot {
    snapshotted: HashMap<String, String>,
    deleted_keys: Vec<String>,
}

impl Snapshot<Command> for KVSnapshot {
    fn create(entries: &[Command]) -> Self {
        let mut snapshotted = HashMap::new();
        let mut deleted_keys: Vec<String> = Vec::new();
        for e in entries {
            match &e.command {
                KVCommand::Put(key, value) => {
                    snapshotted.insert(key.clone(), value.clone());
                }
                KVCommand::Delete(key) => {
                    if snapshotted.remove(key).is_none() {
                        // key was not in the snapshot
                        deleted_keys.push(key.clone());
                    }
                }
                // KVCommand::Get(_) => (),
            }
        }
        // remove keys that were put back
        deleted_keys.retain(|k| !snapshotted.contains_key(k));
        Self {
            snapshotted,
            deleted_keys,
        }
    }

    fn merge(&mut self, delta: Self) {
        for (k, v) in delta.snapshotted {
            self.snapshotted.insert(k, v);
        }
        for k in delta.deleted_keys {
            self.snapshotted.remove(&k);
        }
        self.deleted_keys.clear();
    }

    fn use_snapshots() -> bool {
        true
    }
}
