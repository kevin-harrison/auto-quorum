use futures::StreamExt;
use std::{collections::{HashMap, HashSet}, time::Duration};
use log::*;

use omnipaxos::{
    util::{LogEntry, NodeId},
    OmniPaxos, OmniPaxosConfig,
};
use omnipaxos_storage::memory_storage::MemoryStorage;

use crate::{database::Database, network::{Network, NetworkError}, read::QuorumReader};
use common::{kv::*, messages::*};

type OmniPaxosInstance = OmniPaxos<Command, MemoryStorage<Command>>;

pub struct OmniPaxosServer {
    id: NodeId,
    database: Database,
    network: Network,
    omnipaxos: OmniPaxosInstance,
    current_decided_idx: usize,
    quorum_reader: QuorumReader,
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
        let storage: MemoryStorage<Command> = MemoryStorage::default();
        let omnipaxos = omnipaxos_config.build(storage).unwrap();
        OmniPaxosServer {
            id,
            database: Database::new(),
            network,
            omnipaxos,
            current_decided_idx: 0,
            quorum_reader: QuorumReader::new(id),
        }
    }

    pub async fn run(&mut self) {
        let mut outgoing_interval = tokio::time::interval(Duration::from_millis(1));
        let mut election_interval = tokio::time::interval(Duration::from_millis(5000));

        loop {
            tokio::select! {
                biased;
                _ = election_interval.tick() => { self.omnipaxos.election_timeout(); },
                _ = outgoing_interval.tick() => {
                    self.handle_decided_entries().await;
                    self.send_outgoing_msgs().await;
                },
                Some(msg) = self.network.next() => { self.handle_incoming_msg(msg).await; },
            }
        }
    }

    async fn handle_decided_entries(&mut self) {
        let new_decided_idx = self.omnipaxos.get_decided_idx();
        if self.current_decided_idx < new_decided_idx {
            let decided_entries = self
                .omnipaxos
                .read_decided_suffix(self.current_decided_idx)
                .unwrap();
            self.current_decided_idx = new_decided_idx;
            let decided_commands = decided_entries.into_iter().filter_map(|e| match e {
                LogEntry::Decided(cmd) => Some(cmd),
                _ => None,
            });
            let ready_reads = self.quorum_reader.rinse(new_decided_idx);
            self.update_database_and_respond(decided_commands.chain(ready_reads).collect()).await;
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

    async fn update_database_and_respond(&mut self, commands: Vec<Command>) {
        // TODO: batching responses possible here.
        for command in commands {
            let read = self.database.handle_command(command.kv_cmd);
            // TODO: should leader respond here since it has less latency for decided
            if command.coordinator_id == self.id {
                let response = match read {
                    Some(read_result) => ClientResponse::Read(command.id, read_result),
                    None => ClientResponse::Write(command.id),
                };
                self.network.send(Outgoing::ClientResponse(command.client_id, response)).await;
            }
        }
    }

    async fn send_outgoing_msgs(&mut self) {
        let messages = self.omnipaxos.outgoing_messages();
        for msg in messages {
            let to = msg.get_receiver();
            let cluster_msg = ClusterMessage::OmniPaxosMessage(msg);
            self.network.send(Outgoing::ClusterMessage(to, cluster_msg)).await;
        }
    }

    async fn handle_incoming_msg(&mut self, msg: Result<Incoming, NetworkError>) { 
        match msg {
            Ok(Incoming::ClientRequest(from, request)) => self.handle_client_request(from, request).await,
            Ok(Incoming::ClusterMessage(_from, ClusterMessage::OmniPaxosMessage(m))) => self.omnipaxos.handle_incoming(m),
            Ok(Incoming::ClusterMessage(from, ClusterMessage::QuorumReadRequest(req))) => self.handle_quorum_read_request(from, req).await,
            Ok(Incoming::ClusterMessage(_from, ClusterMessage::QuorumReadResponse(resp))) => self.handle_quorum_read_response(resp).await,
            Err(err) => panic!("Network failed {err:?}"),
        }
    }

    async fn handle_client_request(&mut self, from: ClientId, request: ClientRequest) {
        match request {
            ClientRequest::Append(command_id, kv_command) => {
                if let KVCommand::Get(_) = &kv_command {
                    self.start_quorum_read(from, command_id, kv_command).await;
                } else {
                    let command = Command {
                        client_id: from,
                        coordinator_id: self.id,
                        id: command_id,
                        kv_cmd: kv_command,
                    };
                    self.omnipaxos.append(command).expect("Append to Omnipaxos log failed");
                }
            }
        };
    }

    async fn handle_quorum_read_request(&mut self, from: NodeId, request: QuorumReadRequest) {
        let read_response = QuorumReadResponse {
            client_id: request.client_id,
            command_id: request.command_id,
            read_quorum_config: self.omnipaxos.get_read_config(),
            accepted_idx: self.omnipaxos.get_accepted_idx(),
        };
        let response = Outgoing::ClusterMessage(from, ClusterMessage::QuorumReadResponse(read_response));
        self.network.send(response).await;
    }

    async fn handle_quorum_read_response(&mut self, response: QuorumReadResponse) {
        debug!("Got q response: {response:#?}");
        if let Some(ready_read) = self.quorum_reader.handle_response(response, self.current_decided_idx) {
            self.update_database_and_respond(vec![ready_read]).await;
        }
    }

    // TODO: client sends to closest replica which might be the leader
    async fn start_quorum_read(&mut self, client_id: ClientId, command_id: CommandId, read_command: KVCommand) {
        warn!("Starting q read: {read_command:?}");
        // Get local info
        let accepted_idx = self.omnipaxos.get_accepted_idx();
        let read_quorum_config = self.omnipaxos.get_read_config();
        self.quorum_reader.new_read(client_id, command_id, read_command, read_quorum_config, accepted_idx);

        // Send quorum read to followers
        // TODO: thrifty messaging
        let leader = self.omnipaxos.get_current_leader();
        let followers = self.omnipaxos.get_peers().iter().filter(|id| match leader {
            Some(leader_id) => **id != leader_id,
            None => true,
        });
        debug!("Sending q request ({client_id}, {command_id}) to {followers:?}");
        for peer in followers {
            let read_request = ClusterMessage::QuorumReadRequest(QuorumReadRequest { client_id, command_id });
            let msg = Outgoing::ClusterMessage(*peer, read_request);
            self.network.send(msg).await;
        }
    }
}
