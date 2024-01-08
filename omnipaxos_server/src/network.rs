use anyhow::{anyhow, Error};
use futures::{SinkExt, StreamExt};
use log::*;
use omnipaxos::messages::Message as OmniPaxosMessage;
use omnipaxos::messages::ballot_leader_election::BLEMsg;
use omnipaxos::util::NodeId;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_serde::{formats::Cbor, Framed};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

use common::{kv::ClientId, messages::*};

type NetworkSource = Framed<
    FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
    NetworkMessage,
    (),
    Cbor<NetworkMessage, ()>,
>;
type NetworkSink = Framed<
    FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
    (),
    NetworkMessage,
    Cbor<(), NetworkMessage>,
>;

/// Turns tcp stream into a framed read and write sink/source
fn wrap_stream(stream: TcpStream) -> (NetworkSource, NetworkSink) {
    let (reader, writer) = stream.into_split();
    let stream = FramedRead::new(reader, LengthDelimitedCodec::new());
    let sink = FramedWrite::new(writer, LengthDelimitedCodec::new());
    (
        NetworkSource::new(stream, Cbor::default()),
        NetworkSink::new(sink, Cbor::default()),
    )
}

enum NewConnection {
    NodeConnection(NodeId, NetworkSink),
    ClientConnection(ClientId, NetworkSink),
}

pub struct Network {
    id: NodeId,
    peers: Vec<NodeId>,
    listener: TcpListener,
    cluster_connections: HashMap<NodeId, NetworkSink>,
    client_connections: HashMap<ClientId, NetworkSink>,
    max_client_id: Arc<Mutex<ClientId>>,
    new_connection_sink: Sender<NewConnection>,
    new_connection_source: Receiver<NewConnection>,
    outgoing_messages: Receiver<ServerResponse>,
    incoming_messages: Sender<ServerRequest>,
}

impl Network {
    fn get_node_addr(node: NodeId) -> SocketAddr {
        let port = 8000 + node as u16;
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    pub async fn new(
        id: NodeId,
        peers: Vec<NodeId>,
        outgoing_messages: Receiver<ServerResponse>,
        incoming_messages: Sender<ServerRequest>,
    ) -> Result<Self, Error> {
        let (new_connection_sink, new_connection_source) = mpsc::channel(100);
        Ok(Self {
            id,
            peers,
            listener: TcpListener::bind(Self::get_node_addr(id)).await?,
            cluster_connections: HashMap::new(),
            client_connections: HashMap::new(),
            max_client_id: Arc::new(Mutex::new(0)),
            new_connection_sink,
            new_connection_source,
            incoming_messages,
            outgoing_messages,
        })
    }

    pub async fn run(&mut self) {
        for peer in self.peers.iter().filter(|p| **p < self.id) {
            self.create_connection_to_node(*peer);
        }
        loop {
            tokio::select! {
                biased;
                Some(connection) = self.new_connection_source.recv() => { self.handle_identified_connection(connection); }
                Some(msg) = self.outgoing_messages.recv() => { self.send_message(msg).await; },
                connection = self.listener.accept() => { self.incoming_connection(connection); },
            }
        }
    }

    fn incoming_connection(&mut self, connection: io::Result<(TcpStream, SocketAddr)>) {
        let (socket, socket_addr) = connection.expect("Failed to accept connection");
        debug!("New incoming connection from {socket_addr:?}");
        let incoming_message_sink = self.incoming_messages.clone();
        let new_connection_sink = self.new_connection_sink.clone();
        let max_client_id_handle = self.max_client_id.clone();
        tokio::spawn(Self::handle_incoming_connection(
            socket,
            incoming_message_sink,
            new_connection_sink,
            max_client_id_handle,
        ));
    }

    async fn handle_incoming_connection(
        connection: TcpStream,
        message_sink: Sender<ServerRequest>,
        connection_sink: Sender<NewConnection>,
        max_client_id_handle: Arc<Mutex<ClientId>>,
    ) {
        let (mut reader, mut writer) = wrap_stream(connection);

        // Identify connector's ID by handshake
        let first_message = reader.next().await;
        match first_message {
            Some(Ok(NetworkMessage::NodeHandshake(node_id))) => {
                debug!("Identified connection from node {node_id}");
                let identified_connection = NewConnection::NodeConnection(node_id, writer);
                connection_sink.send(identified_connection).await.unwrap();
            }
            Some(Ok(NetworkMessage::ClientHandshake)) => {
                let next_client_id = {
                    let mut max_client_id = max_client_id_handle.lock().unwrap();
                    *max_client_id += 1;
                    *max_client_id
                };
                debug!("Identified connection from client {next_client_id}");
                let handshake_finish = NetworkMessage::ClientResponse(ClientResponse::AssignedID(next_client_id));
                debug!("Assigning id to client {next_client_id}");
                if let Err(err) = writer.send(handshake_finish).await {
                    error!("Error sending ID to client {next_client_id}: {err}");
                    return;
                }
                let identified_connection = NewConnection::ClientConnection(next_client_id, writer);
                connection_sink.send(identified_connection).await.unwrap();
            }
            Some(Ok(msg)) => {
                warn!("Received unknown message during handshake: {:?}", msg);
                return;
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

        // Collect incoming messages
        while let Some(frame) = reader.next().await {
            match frame {
                Ok(NetworkMessage::ServerRequest(m)) => {
                    trace!("Received: {m:?}");
                    message_sink.send(m).await.unwrap();
                }
                Ok(NetworkMessage::ClientRequest(m)) => {
                    debug!("Received client request: {m:?}");
                    let request = ServerRequest::FromClient(m);
                    message_sink.send(request).await.unwrap();
                }
                Ok(m) => warn!("Received unexpected message: {m:?}"),
                Err(err) => {
                    error!("Error deserializing message: {:?}", err);
                    break;
                }
            }
        }
    }

    fn create_connection_to_node(&self, to: NodeId) {
        debug!("Trying to connect to node {to}");
        let incoming_message_sink = self.incoming_messages.clone();
        let new_connection_sink = self.new_connection_sink.clone();
        let my_id = self.id.clone();
        tokio::spawn(async move {
            match TcpStream::connect(Self::get_node_addr(to)).await {
                Ok(connection) => {
                    debug!("New connection to node {to}");
                    Self::handle_connection_to_node(
                        connection,
                        incoming_message_sink,
                        new_connection_sink,
                        my_id,
                        to,
                        )
                        .await;
                },
                Err(err) => error!("Establishing connection to node {to} failed: {err}"),
            }
        });
    }

    async fn handle_connection_to_node(
        connection: TcpStream,
        message_sink: Sender<ServerRequest>,
        connection_sink: Sender<NewConnection>,
        my_id: NodeId,
        to: NodeId,
    ) {
        let (mut reader, mut writer) = wrap_stream(connection);

        // Send handshake
        let handshake = NetworkMessage::NodeHandshake(my_id);
        debug!("Sending handshake to {to}");
        if let Err(err) = writer.send(handshake).await {
            error!("Error sending handshake to {to}: {err}");
            return;
        }
        let new_connection = NewConnection::NodeConnection(to, writer);
        connection_sink.send(new_connection).await.unwrap();

        // Collect incoming messages
        while let Some(msg) = reader.next().await {
            match msg {
                Ok(NetworkMessage::ServerRequest(m)) => {
                    trace!("Received: {m:?}");
                    message_sink.send(m).await.unwrap();
                }
                Ok(m) => warn!("Received unexpected message: {m:?}"),
                Err(err) => {
                    error!("Error deserializing message: {:?}", err);
                    break;
                }
            }
        }
    }

    fn handle_identified_connection(&mut self, connection: NewConnection) {
        match connection {
            NewConnection::NodeConnection(node_id, conn) => {
                self.cluster_connections.insert(node_id, conn)
            }
            NewConnection::ClientConnection(client_id, conn) => {
                self.client_connections.insert(client_id, conn)
            }
        };
    }

    async fn send_message(&mut self, message: ServerResponse) {
        match message {
            ServerResponse::ToClient(client_id, client_response) => {
                self.send_to_client(client_id, client_response).await
            }
            ServerResponse::ToServer(server_to_server_msg) => {
                self.send_to_cluster(server_to_server_msg).await
            }
        }
    }

    async fn send_to_cluster(&mut self, msg: ServerMessage) {
        let to = msg.get_receiver();
        if let Some(writer) = self.cluster_connections.get_mut(&to) {
            trace!("Sending to node {to}: {msg:?}");
            let net_msg = NetworkMessage::ServerRequest(ServerRequest::FromServer(msg));
            if let Err(err) = writer.send(net_msg).await {
                warn!("Couldn't send message to node {to}: {err}");
                warn!("Removing connection to node {to}");
                self.cluster_connections.remove(&to);
            }
        } else {
            warn!("Not connected to node {to}");
            // If HeartbeatRequest msg is what failed, try to reconnect to node.
            if let ServerMessage::OmniPaxosMessage(OmniPaxosMessage::BLE(m)) = msg {
                if let BLEMsg::HeartbeatRequest(_) = m.msg {
                    if m.to == to {
                        self.create_connection_to_node(to);
                    }
                }
            }
        }
    }

    async fn send_to_client(&mut self, to: ClientId, msg: ClientResponse) {
        if let Some(writer) = self.client_connections.get_mut(&to) {
            debug!("Responding to client {to}: {msg:?}");
            let net_msg = NetworkMessage::ClientResponse(msg);
            if let Err(err) = writer.send(net_msg).await {
                warn!("Couldn't send message to client {to}: {err}");
                warn!("Removing connection to client {to}");
                self.cluster_connections.remove(&to);
            }
        } else {
            warn!("Not connected to client {to}");
        }
    }
}
