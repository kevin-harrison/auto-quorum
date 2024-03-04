use anyhow::Error;
use futures::SinkExt; //Stream};
use log::*;
use std::net::ToSocketAddrs;
use std::task::{Context, Poll};
use std::{net::SocketAddr, pin::Pin};
use tokio::net::TcpStream;
use tokio_serde::{formats::Cbor, Framed};
use tokio_stream::{Stream, StreamMap};
use tokio_util::codec::{Framed as CodecFramed, LengthDelimitedCodec};

use common::messages::*;
use omnipaxos::util::NodeId;

type NodeConnection = Framed<
    CodecFramed<TcpStream, LengthDelimitedCodec>,
    NetworkMessage,
    NetworkMessage,
    Cbor<NetworkMessage, NetworkMessage>,
>;

fn wrap_stream(stream: TcpStream) -> NodeConnection {
    let length_delimited = CodecFramed::new(stream, LengthDelimitedCodec::new());
    Framed::new(length_delimited, Cbor::default())
}

pub struct Network {
    is_local: bool,
    connections: StreamMap<u64, NodeConnection>,
}

impl Network {
    fn get_node_addr(node: NodeId, is_local: bool) -> Result<SocketAddr, Error> {
        let dns_name = if is_local {
            // format!("s{node}:800{node}")
            format!("localhost:800{node}")
        } else {
            format!("server-{node}.internal.zone.:800{node}")
        };
        let address = dns_name.to_socket_addrs()?.next().unwrap();
        Ok(address)
    }

    pub async fn new(nodes: Vec<NodeId>, is_local: bool) -> Self {
        let mut network = Self {
            is_local,
            connections: StreamMap::with_capacity(nodes.len()),
        };
        for node in nodes {
            if let Err(err) = network.add_node(node).await {
                error!("Couldn't connect to server {node}: {err}");
            }
        }
        network
    }

    pub async fn send(&mut self, to: NodeId, messge: ClientMessage) {
        let found_connection = self.connections.iter_mut().find(|(id, _conn)| *id == to);
        match found_connection {
            Some((_id, connection)) => {
                trace!("Sending to server {to}: {message:?}");
                if let Err(err) = connection
                    .send(NetworkMessage::ClientMessage(message))
                    .await
                {
                    warn!("Couldn't send message to node {to}: {err}");
                    warn!("Removing connection to node {to}");
                    self.connections.remove(&to);
                }
            }
            None => {
                // TODO: sending to other servers gets blocked on this await
                warn!("Not connected to node {to}");
                if let Err(err) = self.add_node(to).await {
                    error!("Couldn't connect to node {to}: {err}");
                }
            }
        }
    }

    async fn add_node(&mut self, node: NodeId) -> Result<(), Error> {
        let address = Self::get_node_addr(node, self.is_local)?;
        let tcp_stream = TcpStream::connect(address).await?;
        tcp_stream.set_nodelay(true)?;
        let mut framed_stream = wrap_stream(tcp_stream);
        framed_stream.send(NetworkMessage::ClientRegister).await?;
        self.connections.insert(node, framed_stream);
        return Ok(());
    }
}

impl Stream for Network {
    type Item = (NodeId, Result<ClientResponse, std::io::Error>);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.connections).poll_next(cx) {
            Poll::Ready(Some((id, Ok(msg)))) => match msg {
                NetworkMessage::ServerMessage(m) => Poll::Ready(Some((id, Ok(m)))),
                m => {
                    let unexpected_input = std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Unexpected incoming message {m}",
                    );
                    Poll::Ready(Some((id, Err(unexpected_input))))
                }
            },
            Poll::Ready(Some((id, Err(err)))) => Poll::Ready(Some((id, Err(err)))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
