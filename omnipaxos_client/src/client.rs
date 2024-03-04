use chrono::Utc;
use futures::SinkExt;
use rand::Rng;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio_stream::StreamExt;

use common::util::{get_node_addr, wrap_stream, Connection as ServerConnection};
use common::{kv::*, messages::*};

#[derive(Debug)]
struct RequestData {
    time_sent_utc: i64,
    response: Option<(i64, ServerMessage)>,
}

pub struct Client {
    server: ServerConnection,
    command_id: CommandId,
    request_data: Vec<RequestData>,
    // sent_data: Vec<Utc>,
    // recieved_data: Vec<Option<(Duration, ClientResponse)>>,
}

impl Client {
    pub async fn new(server_id: NodeId, is_local: bool) -> Self {
        let server_address =
            get_node_addr(server_id, is_local).expect("Couldn't resolve server IP");
        let server_stream = TcpStream::connect(server_address)
            .await
            .expect("Couldn't connect to server {server_id}");
        server_stream.set_nodelay(true).unwrap();
        let mut client = Self {
            server: wrap_stream(server_stream),
            command_id: 0,
            request_data: Vec::with_capacity(8000),
            // sent_data: Vec::with_capacity(8000),
            // recieved_data: Vec::with_capacity(8000),
        };
        client.send_registration().await;
        client
    }

    pub async fn put(&mut self, key: String, value: String) {
        self.send_command(KVCommand::Put(key, value)).await;
    }

    pub async fn delete(&mut self, key: String) {
        self.send_command(KVCommand::Delete(key)).await;
    }

    pub async fn get(&mut self, key: String) {
        self.send_command(KVCommand::Get(key)).await;
    }

    pub async fn run_simple(
        &mut self,
        run_duration: Duration,
        initial_request_delay: Duration,
        end_request_delay: Duration,
    ) {
        let mut rng = rand::thread_rng();
        let mut end_run = tokio::time::interval(run_duration);
        let mut halfway = tokio::time::interval(run_duration / 2);
        let mut request_interval = tokio::time::interval(initial_request_delay);
        end_run.tick().await;
        halfway.tick().await;

        loop {
            tokio::select! {
                biased;
                Some(msg) = self.server.next() => self.handle_response(msg.unwrap()),
                _ = request_interval.tick() => {
                    let key = self.command_id.to_string();
                    match rng.gen_range(0..=2) {
                        0 => self.put(key.clone(), key).await,
                        1 => self.delete(key).await,
                        _ => self.get(key).await,
                    }
                },
                _ = halfway.tick() => {
                    request_interval = tokio::time::interval(end_request_delay);
                }
                _ = end_run.tick() => break,
            }
        }
        self.print_results();
    }

    async fn send_command(&mut self, command: KVCommand) {
        let request = ClientMessage::Append(self.command_id, command);
        let data = RequestData {
            time_sent_utc: Utc::now().timestamp_millis(),
            response: None,
        };
        self.request_data.push(data);
        self.command_id += 1;
        self.server
            .send(NetworkMessage::ClientMessage(request))
            .await
            .expect("Couldn't send message to server");
    }

    fn handle_response(&mut self, msg: NetworkMessage) {
        match msg {
            NetworkMessage::ServerMessage(response) => {
                let cmd_id = response.command_id();
                let response_time = Utc::now().timestamp_millis();
                self.request_data[cmd_id].response = Some((response_time, response));
            }
            _ => panic!("Recieved unexpected message: {msg:?}"),
        }
    }

    async fn send_registration(&mut self) {
        self.server
            .send(NetworkMessage::ClientRegister)
            .await
            .expect("Couldn't send message to server");
    }

    fn print_results(&self) {
        for request_data in &self.request_data {
            println!(
                "{:?} {:?}",
                request_data.time_sent_utc, request_data.response
            );
        }
    }
}
