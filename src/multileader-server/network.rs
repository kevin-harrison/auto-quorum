use auto_quorum::common::{
    kv::{ClientId, NodeId},
    messages::*,
    utils::*,
};
use futures::{SinkExt, StreamExt};
use log::*;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{collections::HashMap, str::FromStr};
use tokio::{
    io::AsyncWriteExt,
    sync::mpsc::{Sender, UnboundedSender},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::Receiver,
};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::configs::AutoQuorumConfig;

pub struct Network {
    peers: Vec<NodeId>,
    peer_connections: Vec<Option<PeerConnection>>,
    client_connections: HashMap<ClientId, ClientConnection>,
    max_client_id: Arc<Mutex<ClientId>>,
    batch_size: usize,
    client_message_sender: Sender<(ClientId, ClientMessage)>,
    cluster_message_sender: Sender<(NodeId, MultiLeaderClusterMessage)>,
    pub cluster_messages: Receiver<(NodeId, MultiLeaderClusterMessage)>,
    pub client_messages: Receiver<(ClientId, ClientMessage)>,
    cancel_token: CancellationToken,
}

fn get_addrs(config: AutoQuorumConfig) -> (SocketAddr, Vec<SocketAddr>) {
    let listen_address_str = format!(
        "{}:{}",
        config.server.listen_address, config.server.listen_port
    );
    let listen_address = SocketAddr::from_str(&listen_address_str).expect(&format!(
        "{listen_address_str} is an invalid listen address"
    ));
    let node_addresses: Vec<SocketAddr> = config
        .cluster
        .node_addrs
        .into_iter()
        .map(|addr_str| match addr_str.to_socket_addrs() {
            Ok(mut addrs) => addrs.next().unwrap(),
            Err(e) => panic!("Address {addr_str} is invalid: {e}"),
        })
        .collect();
    (listen_address, node_addresses)
}

impl Network {
    pub async fn new(config: AutoQuorumConfig, batch_size: usize) -> Self {
        let (listen_address, node_addresses) = get_addrs(config.clone());
        let id = config.server.server_id;
        let peer_addresses: Vec<(NodeId, SocketAddr)> = config
            .cluster
            .nodes
            .into_iter()
            .zip(node_addresses.into_iter())
            .filter(|(node_id, _addr)| *node_id != id)
            .collect();
        let mut cluster_connections = vec![];
        cluster_connections.resize_with(peer_addresses.len(), Default::default);
        let (cluster_message_sender, cluster_messages) = tokio::sync::mpsc::channel(batch_size);
        let (client_message_sender, client_messages) = tokio::sync::mpsc::channel(batch_size);
        let mut network = Self {
            peers: peer_addresses.iter().map(|(id, _)| *id).collect(),
            peer_connections: cluster_connections,
            client_connections: HashMap::new(),
            max_client_id: Arc::new(Mutex::new(0)),
            batch_size,
            client_message_sender,
            cluster_message_sender,
            cluster_messages,
            client_messages,
            cancel_token: CancellationToken::new(),
        };
        let num_clients = config.server.num_clients;
        network
            .initialize_connections(id, num_clients, peer_addresses, listen_address)
            .await;
        network
    }

    async fn initialize_connections(
        &mut self,
        id: NodeId,
        num_clients: usize,
        peers: Vec<(NodeId, SocketAddr)>,
        listen_address: SocketAddr,
    ) {
        let (connection_sink, mut connection_source) = mpsc::channel(30);
        let listener_handle =
            self.spawn_connection_listener(connection_sink.clone(), listen_address);
        self.spawn_peer_connectors(connection_sink.clone(), id, peers);
        while let Some(new_connection) = connection_source.recv().await {
            match new_connection {
                NewConnection::ToPeer(connection) => {
                    let peer_idx = self.cluster_id_to_idx(connection.peer_id).unwrap();
                    self.peer_connections[peer_idx] = Some(connection);
                }
                NewConnection::ToClient(connection) => {
                    let _ = self
                        .client_connections
                        .insert(connection.client_id, connection);
                }
            }
            let all_clients_connected = self.client_connections.len() >= num_clients;
            let all_cluster_connected = self.peer_connections.iter().all(|c| c.is_some());
            if all_clients_connected && all_cluster_connected {
                listener_handle.abort();
                break;
            }
        }
    }

    fn spawn_connection_listener(
        &self,
        connection_sender: Sender<NewConnection>,
        listen_address: SocketAddr,
    ) -> tokio::task::JoinHandle<()> {
        let client_sender = self.client_message_sender.clone();
        let cluster_sender = self.cluster_message_sender.clone();
        let max_client_id_handle = self.max_client_id.clone();
        let batch_size = self.batch_size;
        let cancel_token = self.cancel_token.clone();
        tokio::spawn(async move {
            let listener = TcpListener::bind(listen_address).await.unwrap();
            loop {
                match listener.accept().await {
                    Ok((tcp_stream, socket_addr)) => {
                        debug!("New connection from {socket_addr}");
                        tcp_stream.set_nodelay(true).unwrap();
                        tokio::spawn(Self::handle_incoming_connection(
                            tcp_stream,
                            client_sender.clone(),
                            cluster_sender.clone(),
                            connection_sender.clone(),
                            max_client_id_handle.clone(),
                            batch_size,
                            cancel_token.clone(),
                        ));
                    }
                    Err(e) => error!("Error listening for new connection: {:?}", e),
                }
            }
        })
    }

    fn spawn_peer_connectors(
        &self,
        connection_sender: Sender<NewConnection>,
        my_id: NodeId,
        peers: Vec<(NodeId, SocketAddr)>,
    ) {
        let peers_to_connect_to = peers.into_iter().filter(|(peer_id, _)| *peer_id < my_id);
        for (peer, peer_address) in peers_to_connect_to {
            let reconnect_delay = Duration::from_secs(1);
            let mut reconnect_interval = tokio::time::interval(reconnect_delay);
            let cluster_sender = self.cluster_message_sender.clone();
            let connection_sender = connection_sender.clone();
            let batch_size = self.batch_size;
            let cancel_token = self.cancel_token.clone();
            tokio::spawn(async move {
                // Establish connection
                let peer_connection = loop {
                    reconnect_interval.tick().await;
                    match TcpStream::connect(peer_address).await {
                        Ok(connection) => {
                            info!("New connection to node {peer}");
                            connection.set_nodelay(true).unwrap();
                            break connection;
                        }
                        Err(err) => {
                            error!("Establishing connection to node {peer} failed: {err}")
                        }
                    }
                };
                // Send handshake
                let mut registration_connection = frame_registration_connection(peer_connection);
                let handshake = RegistrationMessage::NodeRegister(my_id);
                if let Err(err) = registration_connection.send(handshake).await {
                    error!("Error sending handshake to {peer}: {err}");
                    return;
                }
                let underlying_stream = registration_connection.into_inner().into_inner();
                // Create connection actor
                let peer_actor = PeerConnection::new(
                    peer,
                    underlying_stream,
                    batch_size,
                    cluster_sender,
                    cancel_token,
                );
                let new_connection = NewConnection::ToPeer(peer_actor);
                connection_sender.send(new_connection).await.unwrap();
            });
        }
    }

    async fn handle_incoming_connection(
        connection: TcpStream,
        client_message_sender: Sender<(ClientId, ClientMessage)>,
        cluster_message_sender: Sender<(NodeId, MultiLeaderClusterMessage)>,
        connection_sender: Sender<NewConnection>,
        max_client_id_handle: Arc<Mutex<ClientId>>,
        batch_size: usize,
        cancel_token: CancellationToken,
    ) {
        // Identify connector's ID and type by handshake
        let mut registration_connection = frame_registration_connection(connection);
        let registration_message = registration_connection.next().await;
        let new_connection = match registration_message {
            Some(Ok(RegistrationMessage::NodeRegister(node_id))) => {
                info!("Identified connection from node {node_id}");
                let underlying_stream = registration_connection.into_inner().into_inner();
                NewConnection::ToPeer(PeerConnection::new(
                    node_id,
                    underlying_stream,
                    batch_size,
                    cluster_message_sender,
                    cancel_token,
                ))
            }
            Some(Ok(RegistrationMessage::ClientRegister)) => {
                let next_client_id = {
                    let mut max_client_id = max_client_id_handle.lock().unwrap();
                    *max_client_id += 1;
                    *max_client_id
                };
                info!("Identified connection from client {next_client_id}");
                let underlying_stream = registration_connection.into_inner().into_inner();
                NewConnection::ToClient(ClientConnection::new(
                    next_client_id,
                    underlying_stream,
                    batch_size,
                    client_message_sender,
                ))
            }
            Some(Err(err)) => {
                error!("Error deserializing handshake: {:?}", err);
                return;
            }
            None => {
                debug!("Connection to unidentified source dropped");
                return;
            }
        };
        connection_sender.send(new_connection).await.unwrap();
    }

    pub fn send_to_cluster(&mut self, to: NodeId, msg: MultiLeaderClusterMessage) {
        match self.cluster_id_to_idx(to) {
            Some(idx) => match &mut self.peer_connections[idx] {
                Some(ref mut connection) => {
                    if let Err(err) = connection.send(msg) {
                        warn!("Couldn't send msg to peer {to}: {err}");
                        self.peer_connections[idx] = None;
                    }
                }
                None => warn!("Not connected to node {to}"),
            },
            None => error!("Sending to unexpected node {to}"),
        }
    }

    pub fn send_to_client(&mut self, to: ClientId, msg: ServerMessage) {
        match self.client_connections.get_mut(&to) {
            Some(connection) => {
                if let Err(err) = connection.send(msg) {
                    warn!("Couldn't send msg to client {to}: {err}");
                    self.client_connections.remove(&to);
                }
            }
            None => warn!("Not connected to client {to}"),
        }
    }

    // Removes all peer connections, but waits for queued writes to the peers to finish first
    pub async fn shutdown(&mut self) {
        self.cancel_token.cancel();
        for peer_connection in self.peer_connections.drain(..) {
            if let Some(connection) = peer_connection {
                connection.wait_for_writes_and_shutdown().await;
            }
        }
        for _ in 0..self.peers.len() {
            self.peer_connections.push(None);
        }
    }

    #[inline]
    fn cluster_id_to_idx(&self, id: NodeId) -> Option<usize> {
        self.peers.iter().position(|&p| p == id)
    }
}

enum NewConnection {
    ToPeer(PeerConnection),
    ToClient(ClientConnection),
}

struct PeerConnection {
    peer_id: NodeId,
    writer_task: JoinHandle<()>,
    outgoing_messages: UnboundedSender<MultiLeaderClusterMessage>,
}

impl PeerConnection {
    pub fn new(
        peer_id: NodeId,
        connection: TcpStream,
        batch_size: usize,
        incoming_messages: Sender<(NodeId, MultiLeaderClusterMessage)>,
        cancel_token: CancellationToken,
    ) -> Self {
        let (reader, mut writer) = frame_multileader_cluster_connection(connection);
        // Reader Actor
        let _reader_task = tokio::spawn(async move {
            let mut buf_reader = reader.ready_chunks(batch_size);
            while let Some(messages) = buf_reader.next().await {
                for msg in messages {
                    match msg {
                        Ok(m) => {
                            trace!("Received: {m:?}");
                            if let Err(_) = incoming_messages.send((peer_id, m)).await {
                                break;
                            };
                        }
                        Err(err) => {
                            error!("Error deserializing message: {:?}", err);
                        }
                    }
                }
            }
        });
        // Writer Actor
        let (message_tx, mut message_rx) = mpsc::unbounded_channel();
        let writer_task = tokio::spawn(async move {
            let mut buffer = Vec::with_capacity(batch_size);
            loop {
                tokio::select! {
                    biased;
                    num_messages = message_rx.recv_many(&mut buffer, batch_size) => {
                        if num_messages == 0 { break; }
                        for msg in buffer.drain(..) {
                            trace!("Sending: {msg:?}");
                            if let Err(err) = writer.feed(msg).await {
                                error!("Couldn't send message to node {peer_id}: {err}");
                                break;
                            }
                        }
                        if let Err(err) = writer.flush().await {
                            error!("Couldn't send message to node {peer_id}: {err}");
                            break;
                        }
                    },
                    _ = cancel_token.cancelled() => {
                        // Try to empty outgoing message queue before exiting
                        while let Ok(msg) = message_rx.try_recv() {
                            if let Err(err) = writer.feed(msg).await {
                                error!("Couldn't send message to node {peer_id}: {err}");
                                break;
                            }
                        }
                        if let Err(err) = writer.flush().await {
                            error!("Couldn't send message to node {peer_id}: {err}");
                            break;
                        }

                        // Gracefully shut down the write half of the connection
                        let mut underlying_socket = writer.into_inner().into_inner();
                        if let Err(err) = underlying_socket.shutdown().await {
                            error!("Error shutting down the stream to node {peer_id}: {err}");
                        }
                        break;
                    }
                }
            }
            info!("Connection to node {peer_id} closed");
        });
        PeerConnection {
            peer_id,
            writer_task,
            outgoing_messages: message_tx,
        }
    }

    pub fn send(
        &mut self,
        msg: MultiLeaderClusterMessage,
    ) -> Result<(), mpsc::error::SendError<MultiLeaderClusterMessage>> {
        self.outgoing_messages.send(msg)
    }

    async fn wait_for_writes_and_shutdown(self) {
        let _ = tokio::time::timeout(Duration::from_secs(5), self.writer_task).await;
    }
}

struct ClientConnection {
    client_id: ClientId,
    outgoing_messages: UnboundedSender<ServerMessage>,
}

impl ClientConnection {
    pub fn new(
        client_id: ClientId,
        connection: TcpStream,
        batch_size: usize,
        incoming_messages: Sender<(ClientId, ClientMessage)>,
    ) -> Self {
        let (reader, mut writer) = frame_servers_connection(connection);
        // Reader Actor
        let _reader_task = tokio::spawn(async move {
            let mut buf_reader = reader.ready_chunks(batch_size);
            while let Some(messages) = buf_reader.next().await {
                for msg in messages {
                    trace!("Received: {msg:?}");
                    match msg {
                        Ok(m) => incoming_messages.send((client_id, m)).await.unwrap(),
                        Err(err) => error!("Error deserializing message: {:?}", err),
                    }
                }
            }
        });
        // Writer Actor
        let (message_tx, mut message_rx) = mpsc::unbounded_channel();
        let _writer_task = tokio::spawn(async move {
            let mut buffer = Vec::with_capacity(batch_size);
            while message_rx.recv_many(&mut buffer, batch_size).await != 0 {
                for msg in buffer.drain(..) {
                    trace!("Sending: {msg:?}");
                    if let Err(err) = writer.feed(msg).await {
                        error!("Couldn't send message to client {client_id}: {err}");
                        error!("Killing connection to client {client_id}");
                        return;
                    }
                }
                if let Err(err) = writer.flush().await {
                    error!("Couldn't send message to client {client_id}: {err}");
                    error!("Killing connection to client {client_id}");
                    return;
                }
            }
        });
        ClientConnection {
            client_id,
            outgoing_messages: message_tx,
        }
    }

    pub fn send(
        &mut self,
        msg: ServerMessage,
    ) -> Result<(), mpsc::error::SendError<ServerMessage>> {
        self.outgoing_messages.send(msg)
    }
}
