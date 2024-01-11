use std::collections::{BTreeMap, HashMap};

use common::kv::{CommandId, ClientId};

type AcceptedIdx = usize;

struct ReadState {
    command_id: CommandId,
    client_id: ClientId,
    key: String,
    read_quorum_size: usize,
    replies: usize,
    max_accepted_idx: AcceptedIdx,
}

pub struct QuorumReader {
    awaiting_replies: HashMap<CommandId, ReadState>,
    awaiting_decide: BTreeMap<AcceptedIdx, ReadState>,
}

impl QuorumReader {
    pub fn new() -> Self {
        Self {
        awaiting_replies: HashMap::new(),
        awaiting_decide: BTreeMap::new(),
        }
    }
    pub fn rinse(&mut self, new_decided_idx: usize) -> Option<Vec<ReadState>> {
        // Find all pending reads that have enough replies and have max_accepted_idx <= new_decided_idx.
        // Remove them from their datastructures and return
        unimplemented!();
    }

    pub fn new_read(&mut self, command_id: CommandId, read_quorum: usize, accepted_idx: AcceptedIdx) {
        // add to awaiting replies
        unimplemented!();
    }

    pub fn new_response(&mut self, command_id: CommandId, read_quorum: usize, accepted_idx: AcceptedIdx) -> Option<Vec<ReadState>> {
        // update read state with command_id
        // if read_quorum = # of replies => move to rinse datastructure
        unimplemented!();
    }
}
