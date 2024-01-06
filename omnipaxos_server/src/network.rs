use std::collections::{HashMap, VecDeque};
use std::io;
use std::pin::Pin;
use std::sync::{Arc};
use std::task::{Context, Poll};
use std::time::Duration;
use anyhow::{anyhow, Error};
use futures::{Stream, Future};
use tokio::net::tcp::{OwnedWriteHalf, OwnedReadHalf};
use tokio::sync::{Mutex, mpsc};
use tokio::sync::mpsc::{Sender, Receiver};
use tokio_serde::{formats::Cbor, Framed};
use tokio_util::codec::{Framed as CodecFramed, LengthDelimitedCodec};
use std::net::SocketAddr;
use omnipaxos::util::NodeId;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::io::AsyncBufReadExt;
use log::*;

use crate::messages::NetworkMessage;

type NodeConnection = Framed<
    CodecFramed<TcpStream, LengthDelimitedCodec>,
    NetworkMessage,
    NetworkMessage,
    Cbor<NetworkMessage, NetworkMessage>,
>;

pub type ClientId = u64;

pub struct Network {
    id: NodeId,
    peers: Vec<NodeId>,
    cluster_listener: TcpListener,
    client_listener: TcpListener,
    cluster_connections: HashMap<NodeId, OwnedWriteHalf>,
    client_connections: HashMap<ClientId, OwnedWriteHalf>,
    max_client_id: ClientId,
    incoming_messages: Sender<NetworkMessage>,
    outgoing_messages: Receiver<NetworkMessage>,
}

impl Network {
    fn get_node_addr(node: NodeId) -> SocketAddr {
        let port = 8000 + node as u16;
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    fn get_client_addr(node: NodeId) -> SocketAddr {
        let port = 9000 + node as u16;
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    pub async fn new(
        id: NodeId,
        peers: Vec<NodeId>,
        outgoing_messages: Receiver<NetworkMessage>,
        incoming_messages: Sender<NetworkMessage>,
    ) -> Result<Self, Error> {
        Ok(Self {
            id,
            peers,
            cluster_listener: TcpListener::bind(Self::get_node_addr(id)).await?,
            client_listener: TcpListener::bind(Self::get_client_addr(id)).await?,
            cluster_connections: HashMap::new(),
            client_connections: HashMap::new(),
            max_client_id: 0,
            incoming_messages,
            outgoing_messages,
        })
    }

    pub async fn run(&mut self) {
        // Initiate connections to cluster
        tokio::time::sleep(Duration::from_secs(1)).await;
        let (new_node_sender, mut new_node_receiver) = mpsc::channel(3);
        let peers = self.peers.clone();
        for peer in peers {
            let incoming_messages = self.incoming_messages.clone();
            let new_node_sender = new_node_sender.clone();
            tokio::spawn(async move {
                let conn = TcpStream::connect(Self::get_node_addr(peer)).await.unwrap();
                let (reader, writer) = conn.into_split();
                trace!("New outgoing node connection to {peer}");
                new_node_sender.send((peer, writer)).await.unwrap();
                tokio::spawn(Self::handle_connection(reader, incoming_messages));
            });
        }

        // Listeners
        loop {
            tokio::select! {
                biased;
                connection = self.cluster_listener.accept() => { self.handle_node_connection(connection); },
                connection = self.client_listener.accept() => { self.handle_client_connection(connection); },
                Some(msg) = self.outgoing_messages.recv() => { self.send_message(msg).await; },
                Some((node, connection)) = new_node_receiver.recv() => { self.cluster_connections.insert(node, connection); }
            }
        }
    }

    fn handle_client_connection(&mut self, connection: io::Result<(TcpStream, SocketAddr)>) {
        let (socket, _socket_addr) = connection.expect("Failed to accept connection");
        let (reader, writer) = socket.into_split();
        self.max_client_id += 1;
        self.client_connections.insert(self.max_client_id, writer);
        tokio::spawn(Self::handle_connection(reader, self.incoming_messages.clone()));
    }

    fn handle_node_connection(&mut self, connection: io::Result<(TcpStream, SocketAddr)>) {
        let (socket, _socket_addr) = connection.expect("Failed to accept connection");
        let (reader, _writer) = socket.into_split();
        trace!("New incoming node connection from {_socket_addr:?}");
        tokio::spawn(Self::handle_connection(reader, self.incoming_messages.clone()));
    }

    async fn handle_connection(stream: OwnedReadHalf, out_channel: Sender<NetworkMessage>) {
        let mut reader = BufReader::new(stream);
        loop {
            let mut data = vec![];
            let bytes_read = reader.read_until(b'\n', &mut data).await.unwrap();
            if bytes_read == 0 {
                // TODO: send client disconnected on out channel
                // dropped socket EOF
                break;
            }
            if let Ok(msg) = serde_json::from_slice::<NetworkMessage>(&data) {
                trace!("Received: {msg:?}");
                out_channel.send(msg).await.expect("couldn't send on channel");
            }
        }
    }

    async fn send_message(&mut self, message: NetworkMessage) {
        match message {
            NetworkMessage::OmniPaxosMessage(to, msg) => self.send_to_cluster(to, NetworkMessage::OmniPaxosMessage(to, msg)).await,
            NetworkMessage::ClientMessage(to, msg) => self.send_to_client(to, NetworkMessage::ClientMessage(to, msg)).await,
        };
    }

    async fn send_to_cluster(&mut self, node: NodeId, msg: NetworkMessage) -> Result<(), Error> {
        if let Some(writer) = self.cluster_connections.get_mut(&node) {
            let mut data = serde_json::to_vec(&msg).expect("could not serialize msg");
            data.push(b'\n');
            writer.write_all(&data).await?;
        } else {
            warn!("Not connected to node {node}");
        }
        Ok(())
    }

    async fn send_to_client(&mut self, client: ClientId, msg: NetworkMessage) -> Result<(), Error> {
        unimplemented!();
    }
}
