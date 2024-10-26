pub mod messages {
    use omnipaxos::{
        ballot_leader_election::Ballot, messages::Message as OmniPaxosMessage,
        storage::ReadQuorumConfig, util::NodeId,
    };
    use serde::{Deserialize, Serialize};

    use super::kv::{ClientId, Command, CommandId, KVCommand};

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum NetworkMessage {
        NodeRegister(NodeId),
        ClientRegister,
        ClusterMessage(ClusterMessage),
        ClientMessage(ClientMessage),
        ServerMessage(ServerMessage),
        KillServer,
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
        MetricSync(MetricSync),
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
        pub ballot_read: BallotRead,
    }

    impl QuorumReadResponse {
        pub fn new(
            my_id: NodeId,
            client_id: ClientId,
            command_id: CommandId,
            read_quorum_config: ReadQuorumConfig,
            accepted_idx: usize,
            promise: Ballot,
            leader: NodeId,
            decided_idx: usize,
            max_prom_acc_idx: Option<usize>,
        ) -> Self {
            let ballot_read =
                BallotRead::new(my_id, promise, leader, decided_idx, max_prom_acc_idx);
            QuorumReadResponse {
                client_id,
                command_id,
                read_quorum_config,
                accepted_idx,
                ballot_read,
            }
        }
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum BallotRead {
        Follows((Ballot, NodeId)),
        Leader((Ballot, NodeId), Option<usize>),
    }

    impl BallotRead {
        pub fn new(
            my_id: NodeId,
            promise: Ballot,
            leader: NodeId,
            decided_idx: usize,
            max_prom_acc_idx: Option<usize>,
        ) -> Self {
            if my_id == leader {
                let rinse_idx = match max_prom_acc_idx {
                    Some(idx) => Some(decided_idx.max(idx)),
                    _ => None,
                };
                BallotRead::Leader((promise, leader), rinse_idx)
            } else {
                BallotRead::Follows((promise, leader))
            }
        }
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum MetricSync {
        MetricRequest(u64, MetricUpdate),
        MetricReply(u64, MetricUpdate),
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct MetricUpdate {
        pub latency: Vec<f64>,
        pub load: (f64, f64),
    }

    #[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
    pub enum ReadStrategy {
        #[default]
        ReadAsWrite,
        QuorumRead,
        BallotRead,
    }
}

pub mod kv {
    use omnipaxos::{macros::Entry, storage::Snapshot};
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    pub type CommandId = usize;
    pub type ClientId = u64;
    pub type NodeId = omnipaxos::util::NodeId;

    #[derive(Debug, Clone, Entry, Serialize, Deserialize)]
    pub struct Command {
        pub client_id: ClientId,
        pub coordinator_id: NodeId,
        pub id: CommandId,
        pub kv_cmd: KVCommand,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum KVCommand {
        Put(String, String),
        Delete(String),
        Get(String),
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct KVSnapshot {
        snapshotted: HashMap<String, String>,
        deleted_keys: Vec<String>,
    }

    impl Snapshot<Command> for KVSnapshot {
        fn create(entries: &[Command]) -> Self {
            let mut snapshotted = HashMap::new();
            let mut deleted_keys: Vec<String> = Vec::new();
            for e in entries {
                match &e.kv_cmd {
                    KVCommand::Put(key, value) => {
                        snapshotted.insert(key.clone(), value.clone());
                    }
                    KVCommand::Delete(key) => {
                        if snapshotted.remove(key).is_none() {
                            // key was not in the snapshot
                            deleted_keys.push(key.clone());
                        }
                    }
                    KVCommand::Get(_) => (),
                }
            }
            // remove keys that were put back
            deleted_keys.retain(|k| !snapshotted.contains_key(k));
            Self {
                snapshotted,
                deleted_keys,
            }
        }

        fn merge(&mut self, delta: Self) {
            for (k, v) in delta.snapshotted {
                self.snapshotted.insert(k, v);
            }
            for k in delta.deleted_keys {
                self.snapshotted.remove(&k);
            }
            self.deleted_keys.clear();
        }

        fn use_snapshots() -> bool {
            true
        }
    }
}

pub mod utils {
    use std::net::{SocketAddr, ToSocketAddrs};
    use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
    use tokio::net::TcpStream;
    use tokio_serde::{formats::Bincode, Framed};
    use tokio_util::codec::{Framed as CodecFramed, FramedRead, FramedWrite, LengthDelimitedCodec};

    use super::{kv::NodeId, messages::NetworkMessage};

    pub type Connection = Framed<
        CodecFramed<TcpStream, LengthDelimitedCodec>,
        NetworkMessage,
        NetworkMessage,
        Bincode<NetworkMessage, NetworkMessage>,
    >;

    pub fn wrap_stream(stream: TcpStream) -> Connection {
        let length_delimited = CodecFramed::new(stream, LengthDelimitedCodec::new());
        Framed::new(length_delimited, Bincode::default())
    }

    pub fn get_node_addr(node: NodeId, is_local: bool) -> Result<SocketAddr, std::io::Error> {
        let dns_name = if is_local {
            // format!("s{node}:800{node}")
            format!("localhost:800{node}")
        } else {
            format!("server-{node}.internal.zone.:800{node}")
        };
        let address = dns_name.to_socket_addrs()?.next().unwrap();
        Ok(address)
    }

    pub type NetworkSource = Framed<
        FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
        NetworkMessage,
        (),
        Bincode<NetworkMessage, ()>,
    >;
    pub type NetworkSink = Framed<
        FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
        (),
        NetworkMessage,
        Bincode<(), NetworkMessage>,
    >;

    /// Turns tcp stream into a framed read and write sink/source
    pub fn wrap_split_stream(stream: TcpStream) -> (NetworkSource, NetworkSink) {
        let (reader, writer) = stream.into_split();
        let stream = FramedRead::new(reader, LengthDelimitedCodec::new());
        let sink = FramedWrite::new(writer, LengthDelimitedCodec::new());
        (
            NetworkSource::new(stream, Bincode::default()),
            NetworkSink::new(sink, Bincode::default()),
        )
    }
}
