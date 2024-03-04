use omnipaxos::{messages::Message as OmniPaxosMessage, storage::ReadQuorumConfig, util::NodeId};
use serde::{Deserialize, Serialize};

use crate::kv::{ClientId, Command, CommandId, KVCommand};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum NetworkMessage {
    NodeRegister(NodeId),
    ClientRegister,
    ClusterMessage(ClusterMessage),
    ClientMessage(ClientMessage),
    ServerMessage(ServerMessage),
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
    // Reads, writes
    WorkloadUpdate(u64, u64),
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
}


#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadStrategy {
    #[default]
    QuorumRead,
    ReadAsWrite,
}
