use futures::{SinkExt, StreamExt};
use std::time::Instant;
use std::{
    env,
    io::Error,
    net::{SocketAddr, ToSocketAddrs},
    time::Duration,
};
use tokio::net::TcpStream;
use tokio_serde::{formats::Cbor, Framed};
use tokio_util::codec::{Framed as CodecFramed, LengthDelimitedCodec};
#[macro_use]
extern crate lazy_static;

use common::{kv::*, messages::*};

type NodeId = u64;

type ServerConnection = Framed<
    CodecFramed<TcpStream, LengthDelimitedCodec>,
    NetworkMessage,
    NetworkMessage,
    Cbor<NetworkMessage, NetworkMessage>,
>;

lazy_static! {
    pub static ref NEAREST_SERVER: NodeId = if let Ok(var) = env::var("SERVER_ID") {
        let x = var.parse().expect("Server PIDs must be u64");
        if x == 0 {
            panic!("Server PIDs cannot be 0")
        } else {
            x
        }
    } else {
        panic!("missing server PID")
    };
    pub static ref LOCAL_CLUSTER_DEPLOYMENT: bool = if let Ok(var) = env::var("LOCAL") {
        match var.trim().to_lowercase().as_str() {
            "true" | "t" | "yes" | "y" | "1" => true,
            "false" | "f" | "no" | "n" | "0" => false,
            _ => panic!("Invalid LOCAL argument"),
        }
    } else {
        false
    };
}

fn get_node_addr(node: NodeId) -> Result<SocketAddr, Error> {
    let dns_name = if *LOCAL_CLUSTER_DEPLOYMENT {
        // format!("s{node}:800{node}")
        format!("localhost:800{node}")
    } else {
        format!("server-{node}.internal.zone.:800{node}")
    };
    let address = dns_name.to_socket_addrs()?.next().unwrap();
    Ok(address)
}

struct Client {
    command_id: CommandId,
    server: ServerConnection,
}

impl Client {
    async fn new(server_id: NodeId) -> Self {
        let address = get_node_addr(server_id).expect("Couldn't resolve server IP");
        println!("Trying to connect to {address:?}");
        let tcp_stream = TcpStream::connect(address)
            .await
            .expect("Couldn't connect to server");
        tcp_stream.set_nodelay(true).unwrap();
        let length_delimited = CodecFramed::new(tcp_stream, LengthDelimitedCodec::new());
        let mut framed: ServerConnection = Framed::new(length_delimited, Cbor::default());

        // Register with network
        match framed.send(NetworkMessage::ClientRegister).await {
            Ok(_) => println!("Requesting client ID"),
            Err(err) => println!("Failed to send message: {}", err),
        }
        // // Get assigned ID
        // let id = match framed.next().await {
        //     Some(Ok(NetworkMessage::ClientToMsg(ClientToMsg::AssignedID(id)))) => id,
        //     Some(Ok(m)) => panic!("Unexpected message: {m:?}"),
        //     Some(Err(err)) => panic!("Error deserializing: {err}"),
        //     None => panic!("Connection to server lost."),
        // };
        // println!("Assigned ID: {id}");
        Self {
            command_id: 0,
            server: framed,
        }
    }

    fn get_next_command_id(&mut self) -> CommandId {
        self.command_id += 1;
        self.command_id
    }

    async fn send_request(&mut self, request: ClientRequest) {
        println!("Sending request: {request:?}");
        if let Err(err) = self
            .server
            .send(NetworkMessage::ClientRequest(request))
            .await
        {
            println!("Failed to send message: {}", err);
        }
    }

    async fn get_response(&mut self) -> ClientResponse {
        match self.server.next().await {
            Some(Ok(NetworkMessage::ClientResponse(response))) => response,
            Some(Ok(m)) => panic!("Unexpected message: {m:?}"),
            Some(Err(err)) => panic!("Error deserializing: {err}"),
            None => panic!("Connection to server lost."),
        }
    }

    async fn append(&mut self, kv_command: KVCommand) {
        let request = ClientRequest::Append(self.get_next_command_id(), kv_command);
        self.send_request(request).await;
        let response = self.get_response().await;
        println!("Got response: {response:?}");
    }

    async fn put(&mut self, key: String, value: String) {
        let start = Instant::now();
        self.append(KVCommand::Put(key, value)).await;
        let end = Instant::now();
        println!("Put latency: {:?}", end - start);
    }

    async fn delete(&mut self, key: String) {
        let start = Instant::now();
        self.append(KVCommand::Delete(key)).await;
        let end = Instant::now();
        println!("Delete latency: {:?}", end - start);
    }

    async fn get(&mut self, key: String) {
        let start = Instant::now();
        self.append(KVCommand::Get(key)).await;
        let end = Instant::now();
        println!("Get latency: {:?}", end - start);
    }
}

#[tokio::main]
pub async fn main() {
    let mut client = Client::new(*NEAREST_SERVER).await;
    let mut client2 = Client::new(*NEAREST_SERVER).await;
    let delete = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        client2.delete(5.to_string()).await;
    };
    for i in 0..11 {
        client.put(i.to_string(), (i + 100).to_string()).await;
    }
    tokio::join!(client.get(5.to_string()), delete);
    client.get(5.to_string()).await;
}
