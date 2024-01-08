use log::*;
use std::time::Duration;

use omnipaxos::{
    util::LogEntry,
    OmniPaxos, OmniPaxosConfig,
};
use omnipaxos_storage::memory_storage::MemoryStorage;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::database::Database;
use common::{kv::Command, messages::*};

type OmniPaxosInstance = OmniPaxos<Command, MemoryStorage<Command>>;

pub struct OmniPaxosServer {
    database: Database,
    outgoing_messages: Sender<ServerResponse>,
    incoming_messages: Receiver<ServerRequest>,
    omnipaxos: OmniPaxosInstance,
    current_decided_idx: usize,
    latencies: Vec<Option<u128>>,
}

impl OmniPaxosServer {
    pub fn new(
        omnipaxos_config: OmniPaxosConfig,
        outgoing_messages: Sender<ServerResponse>,
        incoming_messages: Receiver<ServerRequest>,
    ) -> Self {
        let cluster_size = omnipaxos_config.cluster_config.nodes.len();
        let storage: MemoryStorage<Command> = MemoryStorage::default();
        let omnipaxos = omnipaxos_config.build(storage).unwrap();
        OmniPaxosServer {
            database: Database::new(),
            outgoing_messages,
            incoming_messages,
            omnipaxos,
            current_decided_idx: 0,
            latencies: vec![None; cluster_size],
        }
    }

    fn handle_election_timeout(&mut self) {
        self.latencies = self.omnipaxos.election_timeout();
    }

    async fn handle_decided_entries(&mut self) {
        let new_decided_idx = self.omnipaxos.get_decided_idx();
        if self.current_decided_idx < new_decided_idx {
            let decided_entries = self
                .omnipaxos
                .read_decided_suffix(self.current_decided_idx)
                .unwrap();
            self.current_decided_idx = new_decided_idx;
            self.update_database(decided_entries).await;
            // TODO: Fix shapshotting
            // // snapshotting
            // if new_decided_idx % 5 == 0 {
            //     debug!(
            //         "Log before: {:?}",
            //         self.omnipaxos.read_decided_suffix(0).unwrap()
            //     );
            //     self.omnipaxos
            //         .snapshot(Some(new_decided_idx), true)
            //         .expect("Failed to snapshot");
            //     debug!(
            //         "Log after: {:?}\n",
            //         self.omnipaxos.read_decided_suffix(0).unwrap()
            //     );
            // }
        }
    }

    async fn update_database(&mut self, decided_entries: Vec<LogEntry<Command>>) {
        let commands = decided_entries.into_iter().filter_map(|e| match e {
            LogEntry::Decided(cmd) => Some(cmd),
            _ => None,
        });
        // TODO: batching responses possible here.
        for command in commands {
            let response = match self.database.handle_command(command.command) {
                Some(read_result) => ServerResponse::ToClient(command.client_id, ClientResponse::Read(command.id, read_result.clone())),
                None => ServerResponse::ToClient(command.client_id, ClientResponse::Write(command.id)),
            };
            self.outgoing_messages.send(response).await.unwrap();
        }
    }

    async fn send_outgoing_msgs(&mut self) {
        let messages = self.omnipaxos.outgoing_messages();
        for msg in messages {
            self.outgoing_messages
                .send(ServerResponse::ToServer(ServerMessage::OmniPaxosMessage(
                    msg,
                )))
                .await
                .unwrap();
        }
    }

    fn handle_incoming_msg(&mut self, msg: ServerRequest) {
        match msg {
            ServerRequest::FromServer(ServerMessage::OmniPaxosMessage(m)) => {
                self.omnipaxos.handle_incoming(m)
            }
            ServerRequest::FromClient(m) => {
                self.handle_incoming_client_msg(m)
            }
        }
    }

    fn handle_incoming_client_msg(&mut self, msg: ClientRequest) {
        match msg {
            ClientRequest::Append(command) => {
                self.omnipaxos.append(command).expect("Append to Omnipaxos log failed");
            }
        }
    }

    pub async fn run(&mut self) {
        let mut outgoing_interval = tokio::time::interval(Duration::from_millis(1));
        let mut election_interval = tokio::time::interval(Duration::from_millis(5000));

        loop {
            tokio::select! {
                biased;
                _ = election_interval.tick() => { self.handle_election_timeout(); },
                _ = outgoing_interval.tick() => {
                    self.handle_decided_entries().await;
                    self.send_outgoing_msgs().await;
                },
                Some(msg) = self.incoming_messages.recv() => { self.handle_incoming_msg(msg); },
                else => { }
            }
        }
    }
}
