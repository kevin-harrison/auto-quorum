use omnipaxos::{messages::Message as OmniPaxosMessage, util::NodeId};
use serde::{Deserialize, Serialize};

use crate::kv::{ClientId, Command, CommandId};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum NetworkMessage {
    NodeHandshake(NodeId),
    ClientHandshake,
    ServerToMsg(ServerToMsg),
    ServerFromMsg(ServerFromMsg),
    ClientFromMsg(ClientFromMsg),
    ClientToMsg(ClientToMsg),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ServerToMsg {
    FromServer(ClusterMessage),
    FromClient(ClientFromMsg),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ServerFromMsg {
    ToServer(ClusterMessage),
    ToClient(ClientId, ClientToMsg),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClusterMessage {
    OmniPaxosMessage(OmniPaxosMessage<Command>),
    QuorumRead(NodeId),
    QuorumReadResponse(NodeId, usize, usize),
}

impl ClusterMessage {
    pub fn get_receiver(&self) -> NodeId {
        match self {
            ClusterMessage::OmniPaxosMessage(m) => m.get_receiver(),
            ClusterMessage::QuorumRead(to, _, _) => *to,
            ClusterMessage::QuorumReadResponse(to, _, _, _) => *to,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClientFromMsg {
    Append(Command),
    Read(ClientId, CommandId, String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClientToMsg {
    AssignedID(ClientId),
    Read(CommandId, Option<String>),
    Write(CommandId),
}
