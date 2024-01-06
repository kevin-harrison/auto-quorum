use std::time::Duration;
use log::*;
use futures::StreamExt;

use omnipaxos::{util::NodeId, OmniPaxos, OmniPaxosConfig};
use omnipaxos_storage::memory_storage::MemoryStorage;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::{kv::KeyValue, messages::{NetworkMessage, ClientMessage}};

type OmniPaxosInstance = OmniPaxos<KeyValue, MemoryStorage<KeyValue>>;

pub struct OmniPaxosServer {
    id: NodeId,
    outgoing_messages: Sender<NetworkMessage>,
    incoming_messages: Receiver<NetworkMessage>,
    omnipaxos: OmniPaxosInstance,
    latencies: Vec<Option<u128>>,
}

impl OmniPaxosServer {
    pub fn new(
        id: NodeId,
        omnipaxos_config: OmniPaxosConfig,
        outgoing_messages: Sender<NetworkMessage>,
        incoming_messages: Receiver<NetworkMessage>
        ) -> Self {
        let cluster_size = omnipaxos_config.cluster_config.nodes.len();
        let storage: MemoryStorage<KeyValue> = MemoryStorage::default();
        let omnipaxos = omnipaxos_config.build(storage).unwrap();
        OmniPaxosServer {
            id,
            outgoing_messages,
            incoming_messages,
            omnipaxos,
            latencies: vec![None; cluster_size],
        }
    }

    fn handle_election_timeout(&mut self) {
        self.latencies = self.omnipaxos.election_timeout();
    }
    
    async fn send_outgoing_msgs(&mut self) {
        let messages = self.omnipaxos.outgoing_messages();
        for msg in messages {
            let to = msg.get_receiver();
            trace!("Sending to {}: {:?}", to, msg);
            self.outgoing_messages.send(NetworkMessage::OmniPaxosMessage(to, msg)).await.unwrap();
            // match self.outgoing_messages.send(NetworkMessage::OmniPaxosMessage(msg)).await {
            //     Err(err) => trace!("Error sending message to node {receiver}, {err:?}"),
            //     _ => (),
            // }
        }
    }

    async fn handle_incoming_msg(&mut self, msg: NetworkMessage) {
        trace!("Receiving: {:?}", msg);
        match msg {
            NetworkMessage::OmniPaxosMessage(_, m) => self.omnipaxos.handle_incoming(m),
            NetworkMessage::ClientMessage(_, m) => self.handle_incoming_client_msg(m), 
        }
    }

    fn handle_incoming_client_msg(&mut self, msg: ClientMessage) {
        unimplemented!();
    }

    pub async fn run(&mut self) {
        let mut outgoing_interval = tokio::time::interval(Duration::from_millis(1));
        let mut election_interval = tokio::time::interval(Duration::from_millis(500));

        loop {
            tokio::select! {
                biased;
                _ = election_interval.tick() => { self.handle_election_timeout(); },
                _ = outgoing_interval.tick() => { self.send_outgoing_msgs().await; },
                Some(msg) = self.incoming_messages.recv() => { self.handle_incoming_msg(msg).await; },
                else => { }
            }
        }
    }
}
