use omnipaxos::{messages::Message, util::NodeId};
use serde::{Deserialize, Serialize};

use crate::kv::KeyValue;
use crate::network::ClientId;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum NetworkMessage {
    // server messages
    OmniPaxosMessage(NodeId, Message<KeyValue>),

    // client messages
    ClientMessage(ClientId, ClientMessage),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClientMessage {
    Append(KeyValue),
}
