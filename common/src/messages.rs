use omnipaxos::{
    ballot_leader_election::Ballot, messages::Message as OmniPaxosMessage,
    storage::ReadQuorumConfig, util::NodeId,
};
use serde::{Deserialize, Serialize};

use crate::kv::{ClientId, Command, CommandId, KVCommand};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum NetworkMessage {
    NodeRegister(NodeId),
    ClientRegister,
    ClusterMessage(ClusterMessage),
    ClientMessage(ClientMessage),
    ServerMessage(ServerMessage),
    KillServer,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClientMessage {
    Append(CommandId, KVCommand),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ServerMessage {
    Write(CommandId),
    Read(CommandId, Option<String>),
}

impl ServerMessage {
    pub fn command_id(&self) -> CommandId {
        match self {
            ServerMessage::Write(id) => *id,
            ServerMessage::Read(id, _) => *id,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClusterMessage {
    OmniPaxosMessage(OmniPaxosMessage<Command>),
    QuorumReadRequest(QuorumReadRequest),
    QuorumReadResponse(QuorumReadResponse),
    MetricSync(MetricSync),
    ReadStrategyUpdate(Vec<ReadStrategy>),
}

// next
#[derive(Clone, Debug)]
pub enum Incoming {
    ClientMessage(ClientId, ClientMessage),
    ClusterMessage(NodeId, ClusterMessage),
}

// send
#[derive(Clone, Debug)]
pub enum Outgoing {
    ServerMessage(ClientId, ServerMessage),
    ClusterMessage(NodeId, ClusterMessage),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QuorumReadRequest {
    // pub from: NodeId,
    pub client_id: ClientId,
    pub command_id: CommandId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QuorumReadResponse {
    pub client_id: ClientId,
    pub command_id: CommandId,
    pub read_quorum_config: ReadQuorumConfig,
    pub accepted_idx: usize,
    pub ballot_read: BallotRead,
}

impl QuorumReadResponse {
    pub fn new(
        my_id: NodeId,
        client_id: ClientId,
        command_id: CommandId,
        read_quorum_config: ReadQuorumConfig,
        accepted_idx: usize,
        promise: Ballot,
        leader: NodeId,
        decided_idx: usize,
        max_prom_acc_idx: Option<usize>,
    ) -> Self {
        let ballot_read = BallotRead::new(my_id, promise, leader, decided_idx, max_prom_acc_idx);
        QuorumReadResponse {
            client_id,
            command_id,
            read_quorum_config,
            accepted_idx,
            ballot_read,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum BallotRead {
    Follows((Ballot, NodeId)),
    Leader((Ballot, NodeId), Option<usize>),
}

impl BallotRead {
    pub fn new(
        my_id: NodeId,
        promise: Ballot,
        leader: NodeId,
        decided_idx: usize,
        max_prom_acc_idx: Option<usize>,
    ) -> Self {
        if my_id == leader {
            let rinse_idx = match max_prom_acc_idx {
                Some(idx) => Some(decided_idx.max(idx)),
                _ => None,
            };
            BallotRead::Leader((promise, leader), rinse_idx)
        } else {
            BallotRead::Follows((promise, leader))
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MetricSync {
    MetricRequest(u64, MetricUpdate),
    MetricReply(u64, MetricUpdate),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetricUpdate {
    pub latency: Vec<f64>,
    pub load: (f64, f64),
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadStrategy {
    #[default]
    ReadAsWrite,
    QuorumRead,
    BallotRead,
}
