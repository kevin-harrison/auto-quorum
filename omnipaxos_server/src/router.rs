use anyhow::Error;

use futures::{SinkExt, Stream};
use omnipaxos::messages::ballot_leader_election::BLEMsg;
use omnipaxos::messages::Message as OmniPaxosMessage;
use core::panic;
use std::task::{Context, Poll};
use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    pin::Pin,
};
use tokio::net::{TcpListener, TcpStream};

use common::{
    kv::ClientId,
    messages::*,
    util::{get_node_addr, wrap_stream, Connection as NodeConnection},
};
use omnipaxos::util::NodeId;

use log::*;
use std::mem;

pub struct Router {
    id: NodeId,
    is_local: bool,
    congestion_control: bool,
    next_client_id: ClientId,
    listener: TcpListener,
    nodes: HashMap<NodeId, NodeConnection>,
    pending_nodes: Vec<NodeConnection>,
    buffer: VecDeque<Incoming>,
}

impl Router {
    pub async fn new(
        id: NodeId,
        nodes: Vec<NodeId>,
        is_local: bool,
        congestion_control: bool,
    ) -> Result<Self, Error> {
        let port = 8000 + id as u16;
        let listening_address = SocketAddr::from(([0, 0, 0, 0], port));
        let listener = TcpListener::bind(listening_address).await?;
        Ok(Self {
            id,
            is_local,
            congestion_control,
            next_client_id: nodes.into_iter().max().unwrap() + 1,
            listener,
            nodes: HashMap::new(),
            pending_nodes: vec![],
            buffer: VecDeque::new(),
        })
    }

    pub async fn send(&mut self, message: Outgoing) {
        match message {
            Outgoing::ServerMessage(client_id, msg) => self.send_to_client(client_id, msg).await,
            Outgoing::ClusterMessage(server_id, msg) => self.send_to_cluster(server_id, msg).await,
        }
    }

    async fn send_to_cluster(&mut self, to: NodeId, msg: ClusterMessage) {
        if let Some(connection) = self.nodes.get_mut(&to) {
            if let ClusterMessage::OmniPaxosMessage(OmniPaxosMessage::SequencePaxos(s)) = &msg {
                trace!("Sending to node {to}: {s:?}");
            }
            if let Err(err) = connection.send(NetworkMessage::ClusterMessage(msg)).await {
                warn!("Couldn't send message to node {to}: {err}");
                warn!("Removing connection to node {to}");
                self.nodes.remove(&to);
            }
        } else {
            warn!("Not connected to node {to}");
            // TODO: This doesn't work sometimes
            // If HeartbeatRequest msg is what failed, try to reconnect to node.
            if let ClusterMessage::OmniPaxosMessage(OmniPaxosMessage::BLE(m)) = &msg {
                if let BLEMsg::HeartbeatRequest(_) = m.msg {
                    if to < self.id {
                        let resend_msg = Some(NetworkMessage::ClusterMessage(msg));
                        if let Err(err) = self.add_node(to, resend_msg).await {
                            warn!("Couldn't reconnect to node {to}: {err}");
                        };
                    }
                }
            }
        }
    }

    async fn send_to_client(&mut self, to: ClientId, msg: ServerMessage) {
        if let Some(writer) = self.nodes.get_mut(&to) {
            debug!("Responding to client {to}: {msg:?}");
            let net_msg = NetworkMessage::ServerMessage(msg);
            if let Err(err) = writer.send(net_msg).await {
                warn!("Couldn't send message to client {to}: {err}");
                warn!("Removing connection to client {to}");
                self.nodes.remove(&to);
            }
        } else {
            warn!("Not connected to client {to}");
        }
    }

    async fn add_node(&mut self, node: NodeId, send_msg: Option<NetworkMessage>) -> Result<(), Error> {
        let address = get_node_addr(node, self.is_local)?;
        let tcp_stream = TcpStream::connect(address).await?;
        if !self.congestion_control {
            tcp_stream.set_nodelay(true)?;
        }
        let mut framed_stream = wrap_stream(tcp_stream);
        framed_stream
            .send(NetworkMessage::NodeRegister(self.id))
            .await?;
        if let Some(msg) = send_msg {
            framed_stream.send(msg).await?;
        }
        self.nodes.insert(node, framed_stream);
        return Ok(());
    }
}

impl Stream for Router {
    type Item = Result<Incoming, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let self_mut = &mut self.as_mut();

        if let Poll::Ready(val) = Pin::new(&mut self_mut.listener).poll_accept(cx) {
            match val {
                Ok((tcp_stream, socket_addr)) => {
                    debug!("New connection from {}", socket_addr);
                    if !self_mut.congestion_control {
                        tcp_stream.set_nodelay(true)?;
                    }
                    let framed_stream = wrap_stream(tcp_stream);
                    self_mut.pending_nodes.push(framed_stream);
                }
                Err(err) => {
                    error!("Error checking for new requests: {:?}", err);
                    return Poll::Ready(None); // End stream
                }
            }
        }

        let mut new_pending = Vec::new();

        mem::swap(&mut self_mut.pending_nodes, &mut new_pending);

        for mut pending in new_pending.into_iter() {
            if let Poll::Ready(val) = Pin::new(&mut pending).poll_next(cx) {
                match val {
                    Some(Ok(NetworkMessage::NodeRegister(id))) => {
                        debug!("Node {} handshake complete", id);
                        self_mut.nodes.insert(id, pending);
                    }
                    Some(Ok(NetworkMessage::ClientRegister)) => {
                        let id = self_mut.next_client_id;
                        self_mut.next_client_id += 1;
                        debug!("Client {} handshake complete", id);
                        self_mut.nodes.insert(id, pending);
                    }
                    Some(Ok(msg)) => warn!("Received unknown message during handshake: {:?}", msg),
                    Some(Err(err)) => error!("Error checking for new requests: {:?}", err),
                    None => (),
                }
            } else {
                self_mut.pending_nodes.push(pending);
            }
        }

        let mut new_nodes = HashMap::new();

        mem::swap(&mut self_mut.nodes, &mut new_nodes);

        for (id, mut connection) in new_nodes.into_iter() {
            match Pin::new(&mut connection).poll_next(cx) {
                Poll::Ready(Some(Ok(val))) => {
                    match val {
                        NetworkMessage::ClientMessage(m) => {
                            debug!("Received request from client {id}: {m:?}");
                            let request = Incoming::ClientMessage(id, m);
                            self_mut.buffer.push_back(request);
                        },
                        NetworkMessage::ClusterMessage(m) => {
                            if let ClusterMessage::OmniPaxosMessage(
                                OmniPaxosMessage::SequencePaxos(s),
                            ) = &m
                            {
                                trace!("Received: {s:?}");
                            }
                            let request = Incoming::ClusterMessage(id, m);
                            self_mut.buffer.push_back(request);
                        },
                        NetworkMessage::KillServer => {
                            panic!("Received kill signal.")
                        }
                        m => warn!("Received unexpected message: {m:?}"),
                    }
                    self_mut.nodes.insert(id, connection);
                }
                Poll::Ready(None) => {
                    //Finished
                    debug!("Node/Client `{}` disconnecting", id);
                }
                Poll::Ready(Some(Err(err))) => {
                    //Error
                    error!("Error from node `{}`: {} Removing connection.", id, err);
                }
                Poll::Pending => {
                    self_mut.nodes.insert(id, connection);
                }
            }
        }

        if let Some(val) = self_mut.buffer.pop_front() {
            if self_mut.buffer.len() > 0 {
                cx.waker().wake_by_ref();
            }
            return Poll::Ready(Some(Ok(val)));
        }

        return Poll::Pending;
    }
}

pub struct RouterWithLatency {
    router: Router,
    latencies: Vec<Vec<u64>>,
    buffer: VecDeque<Incoming>,
    timers: VecDeque<tokio::time::Instant>,
}

impl RouterWithLatency {
    pub async fn new(
        id: NodeId,
        nodes: Vec<NodeId>,
        is_local: bool,
        congestion_control: bool,
        // latencies: Vec<Vec<u64>>,
    ) -> Result<Self, Error> {
        let mut latencies = vec![];
        latencies.push(vec![0, 10, 20]);
        latencies.push(vec![10, 0, 20]);
        latencies.push(vec![0, 20,  0]);
        Ok(Self {
            router: Router::new(id, nodes, is_local, congestion_control).await?,
            buffer: VecDeque::new(),
            timers: VecDeque::new(),
            latencies,
        })
    }
    
    pub async fn send(&mut self, message: Outgoing) {
        self.router.send(message).await;
    }
}

impl Stream for RouterWithLatency {
    type Item = Incoming;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.router).poll_next(cx) {
            Poll::Ready(Some(Ok(item))) => {
                match item {
                    Incoming::ClusterMessage(from, msg) => {
                        let now = tokio::time::Instant::now();
                        let delay = tokio::time::Duration::from_millis(this.latencies[this.router.id as usize - 1][from as usize - 1]);
                        this.buffer.push_back(Incoming::ClusterMessage(from, msg));
                        this.timers.push_back(now + delay);
                    },
                    client_msg => return Poll::Ready(Some(client_msg)) 
                }
            }
            _other => (),
        };

        match this.timers.front() {
            Some(l) if tokio::time::Instant::now() >= *l => {
                this.timers.pop_front();
                Poll::Ready(this.buffer.pop_front())
            },
            _ => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}
