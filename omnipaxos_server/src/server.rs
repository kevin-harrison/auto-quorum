use futures::StreamExt;
use log::*;
use std::{collections::HashMap, time::Duration};

use omnipaxos::{
    util::{LogEntry, NodeId},
    OmniPaxos, OmniPaxosConfig,
};
use omnipaxos_storage::memory_storage::MemoryStorage;

use crate::{database::Database, network::{Network, NetworkError}};
use common::{kv::*, messages::*};

type OmniPaxosInstance = OmniPaxos<Command, MemoryStorage<Command>>;

pub struct OmniPaxosServer {
    database: Database,
    network: Network,
    omnipaxos: OmniPaxosInstance,
    current_decided_idx: usize,
    latencies: Vec<Option<u128>>,
    pending_responses: HashMap<CommandId, ClientId>,
}

impl OmniPaxosServer {
    pub async fn new(omnipaxos_config: OmniPaxosConfig) -> Self {
        let id = omnipaxos_config.server_config.pid;
        let peers = omnipaxos_config
            .cluster_config
            .nodes
            .iter()
            .cloned()
            .filter(|pid| *pid != id)
            .collect();
        let network = Network::new(id, peers).await.unwrap();
        let cluster_size = omnipaxos_config.cluster_config.nodes.len();
        let storage: MemoryStorage<Command> = MemoryStorage::default();
        let omnipaxos = omnipaxos_config.build(storage).unwrap();
        OmniPaxosServer {
            database: Database::new(),
            network,
            omnipaxos,
            current_decided_idx: 0,
            latencies: vec![None; cluster_size],
            pending_responses: HashMap::new(),
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
                Some(msg) = self.network.next() => { self.handle_incoming_msg(msg).await; },
            }
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
            let read = self.database.handle_command(command.command);
            if let Some(waiting_client) = self.pending_responses.get(&command.id) {
                let response = match read {
                    Some(read_result) => ClientResponse::Read(command.id, read_result),
                    None => ClientResponse::Write(command.id),
                };
                self.network.send(Response::ClientResponse(*waiting_client, response)).await;
            }
        }
    }

    async fn send_outgoing_msgs(&mut self) {
        let messages = self.omnipaxos.outgoing_messages();
        for msg in messages {
            let response = Response::OmniPaxosMessage(msg);
            self.network.send(response).await;
        }
    }

    async fn handle_incoming_msg(&mut self, msg: Result<Request, NetworkError>) { 
        match msg {
            Ok(Request::ClientRequest(client_id, request)) => self.handle_incoming_client_msg(client_id, request).await,
            Ok(Request::OmniPaxosMessage(m)) => self.omnipaxos.handle_incoming(m),
            Err(err) => panic!("Network failed {err:?}"),
        }
    }

    async fn handle_incoming_client_msg(&mut self, from: ClientId, request: ClientRequest) {
        match request {
            ClientRequest::Append(command) => {
                let command_id = command.id;
                self
                    .omnipaxos
                    .append(command)
                    .expect("Append to Omnipaxos log failed");
                self.pending_responses.insert(command_id, from);
            }
        };
    }

    // // TODO: client sends to closest replica which might be the leader
    // async fn start_quorum_read(&mut self, client_id: ClientId, command_id: CommandId, key: String) {
    //     // Get local info
    //     let my_accepted_idx = self.omnipaxos.get_accepted_idx();
    //     let read_quorum_size = self
    //         .omnipaxos
    //         .get_config()
    //         .get_active_quorum()
    //         .read_quorum_size;
    //     self.quorum_reader.new_read(command_id, read_quorum, accepted_idx)
    //
    //     // Send quorum read to followers
    //     let leader = self.omnipaxos.get_current_leader();
    //     let followers = self.omnipaxos.get_peers().iter().filter(|id| match leader {
    //         Some(leader_id) => **id != leader_id,
    //         None => true,
    //     });
    //     for peer in followers {
    //         let qread_msg = ClusterMessage::QuorumRead(*peer, command_id, key.clone());
    //         let msg = ServerFromMsg::ToServer(qread_msg);
    //         self.outgoing_messages.send(msg).await.unwrap();
    //     }
    // }
}
