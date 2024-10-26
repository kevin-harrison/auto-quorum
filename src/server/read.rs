use omnipaxos::{ballot_leader_election::Ballot, storage::ReadQuorumConfig, util::NodeId};
use std::collections::hash_map::Entry;
use std::collections::HashMap;

use auto_quorum::common::{
    kv::{ClientId, Command, CommandId, KVCommand},
    messages::{BallotRead, QuorumReadResponse},
};

type CommandKey = (ClientId, CommandId);
type LeaderKey = (Ballot, NodeId);

struct PendingRead {
    read_command: KVCommand,
    read_quorum_config: ReadQuorumConfig,
    rinse_idx: Option<usize>,
    num_replies: usize,
    max_accepted_idx: usize,
    ballot_reads: Vec<BallotReadState>,
    ballot_rinse_discovered: bool,
    ballot_reads_enabled: bool,
}

impl PendingRead {
    fn new(
        read_command: KVCommand,
        read_quorum_config: ReadQuorumConfig,
        accepted_idx: usize,
        ballot_read: BallotRead,
        ballot_reads_enabled: bool,
    ) -> PendingRead {
        PendingRead {
            read_command,
            read_quorum_config,
            num_replies: 1,
            max_accepted_idx: accepted_idx,
            ballot_reads: vec![BallotReadState::new(ballot_read)],
            rinse_idx: None,
            ballot_rinse_discovered: false,
            ballot_reads_enabled,
        }
    }

    fn update(&mut self, response: QuorumReadResponse) {
        if response.read_quorum_config > self.read_quorum_config {
            self.read_quorum_config = response.read_quorum_config;
        }
        if response.accepted_idx > self.max_accepted_idx {
            self.max_accepted_idx = response.accepted_idx;
        }
        self.num_replies += 1;
        if self.num_replies >= self.read_quorum_config.read_quorum_size {
            match self.rinse_idx {
                None => self.rinse_idx = Some(self.max_accepted_idx),
                Some(ref mut idx) if self.max_accepted_idx < *idx => *idx = self.max_accepted_idx,
                _ => (),
            }
        }

        if self.ballot_rinse_discovered || !self.ballot_reads_enabled {
            return;
        }
        let leader_key = match response.ballot_read {
            BallotRead::Follows(key) => key,
            BallotRead::Leader(key, _) => key,
        };
        if let Some(ballot_read_state) = self
            .ballot_reads
            .iter_mut()
            .find(|s| s.leader_key == leader_key)
        {
            ballot_read_state.update(response.ballot_read);
            if ballot_read_state.ballot_rinse_idx.is_some()
                && ballot_read_state.num_follower_replies
                    >= self.read_quorum_config.read_quorum_size
            {
                self.ballot_rinse_discovered = true;
                match self.rinse_idx {
                    None => self.rinse_idx = ballot_read_state.ballot_rinse_idx,
                    Some(ref mut idx) if ballot_read_state.ballot_rinse_idx.unwrap() < *idx => {
                        *idx = ballot_read_state.ballot_rinse_idx.unwrap()
                    }
                    _ => (),
                }
            }
        } else {
            self.ballot_reads
                .push(BallotReadState::new(response.ballot_read));
        };
    }
}

struct BallotReadState {
    leader_key: LeaderKey,
    num_follower_replies: usize,
    ballot_rinse_idx: Option<usize>,
}

impl BallotReadState {
    fn new(ballot_read: BallotRead) -> BallotReadState {
        match ballot_read {
            BallotRead::Follows(leader_key) => Self {
                leader_key,
                num_follower_replies: 1,
                ballot_rinse_idx: None,
            },
            BallotRead::Leader(leader_key, ballot_rinse_idx) => Self {
                leader_key,
                num_follower_replies: 1,
                ballot_rinse_idx,
            },
        }
    }

    fn update(&mut self, ballot_read: BallotRead) {
        self.num_follower_replies += 1;
        if let BallotRead::Leader(_, ballot_rinse_idx) = ballot_read {
            self.ballot_rinse_idx = ballot_rinse_idx;
        }
    }
}

pub struct QuorumReader {
    id: NodeId,
    pending_reads: HashMap<CommandKey, PendingRead>,
}

impl QuorumReader {
    pub fn new(id: NodeId) -> Self {
        Self {
            id,
            pending_reads: HashMap::new(),
        }
    }

    pub fn new_read(
        &mut self,
        client_id: ClientId,
        command_id: CommandId,
        read_command: KVCommand,
        read_quorum_config: ReadQuorumConfig,
        accepted_idx: usize,
        promise: Ballot,
        leader: NodeId,
        decided_idx: usize,
        max_prom_acc_idx: Option<usize>,
        ballot_read_enabled: bool,
    ) {
        let ballot_read = BallotRead::new(self.id, promise, leader, decided_idx, max_prom_acc_idx);
        let cmd_key = (client_id, command_id);
        let pending_read = PendingRead::new(
            read_command,
            read_quorum_config,
            accepted_idx,
            ballot_read,
            ballot_read_enabled,
        );
        self.pending_reads.insert(cmd_key, pending_read);
    }

    pub fn handle_response(
        &mut self,
        response: QuorumReadResponse,
        current_decided_idx: usize,
    ) -> Option<Command> {
        let command_key = (response.client_id, response.command_id);
        if let Entry::Occupied(mut o) = self.pending_reads.entry(command_key) {
            let pending_read = o.get_mut();
            let id = response.command_id;
            let client_id = response.client_id;
            pending_read.update(response);
            match pending_read.rinse_idx {
                Some(idx) if idx <= current_decided_idx => {
                    let ready_read = o.remove();
                    let read_command = Command {
                        client_id,
                        coordinator_id: self.id,
                        id,
                        kv_cmd: ready_read.read_command,
                    };
                    log::debug!("Rinsing {read_command:?}, rinse_idx = {idx}");
                    return Some(read_command);
                }
                _ => return None,
            }
        }
        None
    }

    pub fn rinse(&mut self, decided_idx: usize) -> Vec<Command> {
        let mut ready_reads = vec![];
        self.pending_reads.retain(|&k, v| {
            match v.rinse_idx {
                Some(idx) if idx <= decided_idx => {
                    let read_command = Command {
                        client_id: k.0,
                        coordinator_id: self.id,
                        id: k.1,
                        // TODO: remove clone by getting drain_filter to work
                        kv_cmd: v.read_command.clone(),
                    };
                    log::debug!("Rinsing {read_command:?}, rinse_idx = {idx}");
                    ready_reads.push(read_command);
                    false
                }
                _ => true,
            }
        });
        ready_reads
    }
}
