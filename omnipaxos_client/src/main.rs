use futures::{SinkExt, StreamExt};
use std::{env, net::SocketAddr};
use tokio::net::TcpStream;
use tokio_serde::{formats::Cbor, Framed};
use tokio_util::codec::{Framed as CodecFramed, LengthDelimitedCodec};

use common::{kv::*, messages::*};

type NodeId = u64;

type ServerConnection = Framed<
    CodecFramed<TcpStream, LengthDelimitedCodec>,
    NetworkMessage,
    NetworkMessage,
    Cbor<NetworkMessage, NetworkMessage>,
>;

fn get_node_addr(node: NodeId) -> SocketAddr {
    let port = 8000 + node as u16;
    SocketAddr::from(([127, 0, 0, 1], port))
}

struct Client {
    id: ClientId,
    command_id: CommandId,
    server: ServerConnection,
}

impl Client {
    async fn new(server_id: NodeId) -> Self {
        let address = get_node_addr(server_id);
        let tcp_stream = TcpStream::connect(address)
            .await
            .expect("Couldn't connect to server");
        let length_delimited = CodecFramed::new(tcp_stream, LengthDelimitedCodec::new());
        let mut framed: ServerConnection = Framed::new(length_delimited, Cbor::default());

        // Get client id
        match framed.send(NetworkMessage::ClientHandshake).await {
            Ok(_) => println!("Requesting client ID"),
            Err(err) => println!("Failed to send message: {}", err),
        }
        let id = match framed.next().await {
            Some(Ok(NetworkMessage::ClientToMsg(ClientToMsg::AssignedID(id)))) => id,
            Some(Ok(m)) => panic!("Unexpected message: {m:?}"),
            Some(Err(err)) => panic!("Error deserializing: {err}"),
            None => panic!("Connection to server lost."),
        };
        println!("Assigned ID: {id}");
        Self {
            id,
            command_id: 0,
            server: framed,
        }
    }

    fn get_next_command_id(&mut self) -> CommandId {
        self.command_id += 1;
        self.command_id
    }

    async fn send_request(&mut self, request: ClientFromMsg) {
        println!("Sending request: {request:?}");
        if let Err(err) = self
            .server
            .send(NetworkMessage::ClientFromMsg(request))
            .await
        {
            println!("Failed to send message: {}", err);
        }
    }

    async fn get_response(&mut self) -> ClientToMsg {
        match self.server.next().await {
            Some(Ok(NetworkMessage::ClientToMsg(response))) => response,
            Some(Ok(m)) => panic!("Unexpected message: {m:?}"),
            Some(Err(err)) => panic!("Error deserializing: {err}"),
            None => panic!("Connection to server lost."),
        }
    }

    async fn append(&mut self, kv_command: KVCommand) {
        let command = Command {
            client_id: self.id,
            id: self.get_next_command_id(),
            coordinator_id: NEAREST_SERVER,
            command: kv_command,
        };
        let request = ClientFromMsg::Append(command);
        self.send_request(request).await;
        let response = self.get_response().await;
        println!("Got response: {response:?}");
    }

    async fn put(&mut self, key: String, value: String) {
        self.append(KVCommand::Put(key, value)).await;
    }

    async fn delete(&mut self, key: String) {
        self.append(KVCommand::Delete(key)).await;
    }

    async fn get(&mut self, key: String) {
        unimplemented!();
        // self.append(KVCommand::Get(key)).await;
    }
}

const NEAREST_SERVER: NodeId = 1;

#[tokio::main]
pub async fn main() {
    // // Parse args
    // let args: Vec<String> = env::args().collect();
    // let command = args[1].clone();
    //
    // if command == "append" {
    //     let node: NodeId = args[2].parse().expect("Couldn't parse node ID arg");
    //     let key = args[3].clone();
    //     let value = args[4].parse().expect("Couldn't parse value arg");
    //     append(node, key, value).await;
    // }
    let mut client = Client::new(NEAREST_SERVER).await;
    for i in 0..13 {
        client.put(i.to_string(), (i + 100).to_string()).await;
    }
}
