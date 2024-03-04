use std::net::{SocketAddr, ToSocketAddrs};
use tokio::net::TcpStream;
use tokio_serde::{formats::Cbor, Framed};
use tokio_util::codec::{Framed as CodecFramed, LengthDelimitedCodec};

use crate::{messages::NetworkMessage, kv::NodeId};

pub type Connection = Framed<
    CodecFramed<TcpStream, LengthDelimitedCodec>,
    NetworkMessage,
    NetworkMessage,
    Cbor<NetworkMessage, NetworkMessage>,
>;

pub fn wrap_stream(stream: TcpStream) -> Connection {
    let length_delimited = CodecFramed::new(stream, LengthDelimitedCodec::new());
    Framed::new(length_delimited, Cbor::default())
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
