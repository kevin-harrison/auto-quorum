use omnipaxos::{messages::Message as OmniPaxosMessage, storage::ReadQuorumConfig, util::NodeId};
use serde::{Deserialize, Serialize};

use crate::kv::{ClientId, Command, CommandId, KVCommand};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum NetworkMessage {
    NodeRegister(NodeId),
    ClusterMessage(ClusterMessage),
    ClientRegister,
    ClientRequest(ClientRequest),
    ClientResponse(ClientResponse),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClientRequest {
    Append(CommandId, KVCommand),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClientResponse {
    Write(CommandId),
    Read(CommandId, Option<String>),
}

impl ClientResponse {
    pub fn command_id(&self) -> CommandId {
        match self {
            ClientResponse::Write(id) => *id,
            ClientResponse::Read(id, _) => *id,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClusterMessage {
    OmniPaxosMessage(OmniPaxosMessage<Command>),
    QuorumReadRequest(QuorumReadRequest),
    QuorumReadResponse(QuorumReadResponse),
    // Reads, writes
    WorkloadUpdate(u64, u64),
    ReadStrategyUpdate(Vec<ReadStrategy>),
}

// next
#[derive(Clone, Debug)]
pub enum Incoming {
    ClientRequest(ClientId, ClientRequest),
    ClusterMessage(NodeId, ClusterMessage),
}

// send
#[derive(Clone, Debug)]
pub enum Outgoing {
    ClientResponse(ClientId, ClientResponse),
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
}


#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadStrategy {
    #[default]
    QuorumRead,
    ReadAsWrite,
}
