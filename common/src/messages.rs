use omnipaxos::{messages::Message as OmniPaxosMessage, util::NodeId};
use serde::{Deserialize, Serialize};

use crate::kv::{ClientId, Command, CommandId};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum NetworkMessage {
    NodeRegister(NodeId),
    ClientRegister,
    OmniPaxosMessage(OmniPaxosMessage<Command>),
    ClientRequest(ClientRequest),
    ClientResponse(ClientResponse),
}

// next
#[derive(Clone, Debug)]
pub enum Request {
     OmniPaxosMessage(OmniPaxosMessage<Command>),
     ClientRequest(ClientId, ClientRequest),
}

// send
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Response {
     OmniPaxosMessage(OmniPaxosMessage<Command>),
     ClientResponse(ClientId, ClientResponse)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClientRequest {
     Append(Command),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClientResponse {
     Write(CommandId),
     Read(CommandId, Option<String>)
}
