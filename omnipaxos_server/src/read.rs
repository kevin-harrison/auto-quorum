use log::debug;
use omnipaxos::{storage::ReadQuorumConfig, util::NodeId};
use std::collections::{HashMap, VecDeque};

use common::{
    kv::{ClientId, Command, CommandId, KVCommand},
    messages::QuorumReadResponse,
};

type CommandKey = (ClientId, CommandId);

struct PendingRead {
    read_command: KVCommand,
    num_replies: usize,
    read_quorum_config: ReadQuorumConfig,
    max_accepted_idx: usize,
}

struct ReadyRead {
    client_id: ClientId,
    command_id: CommandId,
    read_command: KVCommand,
    read_idx: usize,
}

pub struct QuorumReader {
    id: NodeId,
    pending_reads: HashMap<CommandKey, PendingRead>,
    ready_reads: VecDeque<ReadyRead>,
}

impl QuorumReader {
    pub fn new(id: NodeId) -> Self {
        Self {
            id,
            pending_reads: HashMap::new(),
            ready_reads: VecDeque::new(),
        }
    }

    pub fn new_read(
        &mut self,
        client_id: ClientId,
        command_id: CommandId,
        read_command: KVCommand,
        read_quorum_config: ReadQuorumConfig,
        accepted_idx: usize,
    ) {
        let pending_read = PendingRead {
            read_command,
            num_replies: 1,
            read_quorum_config,
            max_accepted_idx: accepted_idx,
        };
        self.pending_reads
            .insert((client_id, command_id), pending_read);
    }

    pub fn handle_response(
        &mut self,
        response: QuorumReadResponse,
        current_decided_idx: usize,
    ) -> Option<Command> {
        let command_key = (response.client_id, response.command_id);
        let read_is_ready = if let Some(pending_read) = self.pending_reads.get_mut(&command_key) {
            if response.read_quorum_config > pending_read.read_quorum_config {
                pending_read.read_quorum_config = response.read_quorum_config;
            }
            if response.accepted_idx > pending_read.max_accepted_idx {
                pending_read.max_accepted_idx = response.accepted_idx;
            }
            pending_read.num_replies += 1;
            pending_read.num_replies >= pending_read.read_quorum_config.read_quorum_size
        } else {
            false
        };

        if read_is_ready {
            let pending_read = self.pending_reads.remove(&command_key).unwrap();
            if pending_read.max_accepted_idx <= current_decided_idx {
                let read_command = Command {
                    client_id: response.client_id,
                    coordinator_id: self.id,
                    id: response.command_id,
                    kv_cmd: pending_read.read_command,
                };
                return Some(read_command);
            } else {
                let ready_read = ReadyRead {
                    client_id: response.client_id,
                    command_id: response.command_id,
                    read_command: pending_read.read_command,
                    read_idx: pending_read.max_accepted_idx,
                };
                self.ready_reads.push_back(ready_read);
            }
        }
        None
    }

    pub fn rinse(&mut self, decided_idx: usize) -> Vec<Command> {
        debug!("Rinsing at decided_idx {decided_idx}");
        let mut result = vec![];
        while !self.ready_reads.is_empty() {
            if self.ready_reads[0].read_idx >= decided_idx {
                break;
            } else {
                let ready = self.ready_reads.pop_front().unwrap();
                let read_command = Command {
                    client_id: ready.client_id,
                    coordinator_id: self.id,
                    id: ready.command_id,
                    kv_cmd: ready.read_command,
                };
                result.push(read_command);
            }
        }
        result
    }
}
