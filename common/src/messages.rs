use omnipaxos::{messages::Message as OmniPaxosMessage, util::NodeId};
use serde::{Deserialize, Serialize};

use crate::kv::{Command, CommandId, ClientId};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum NetworkMessage {
    NodeHandshake(NodeId),
    ClientHandshake,
    ServerRequest(ServerRequest),
    ServerResponse(ServerResponse),
    ClientRequest(ClientRequest),
    ClientResponse(ClientResponse),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ServerRequest {
    FromServer(ServerMessage),
    FromClient(ClientRequest),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ServerResponse {
    ToServer(ServerMessage),
    ToClient(ClientId, ClientResponse),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ServerMessage {
    OmniPaxosMessage(OmniPaxosMessage<Command>),
}

impl ServerMessage {
    pub fn get_receiver(&self) -> NodeId {
        match self {
            ServerMessage::OmniPaxosMessage(m) => m.get_receiver(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClientRequest {
    Append(Command),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClientResponse {
    AssignedID(ClientId),
    Read(CommandId, String),
    Write(CommandId),
}
