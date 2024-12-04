use auto_quorum::common::{
    kv::*,
    messages::*,
    utils::{
        frame_clients_connection, frame_registration_connection, get_node_addr,
        FromServerConnection, ToServerConnection,
    },
};
use chrono::Utc;
use csv::Writer;
use futures::{SinkExt, StreamExt};
use log::*;
use rand::{rngs::StdRng, Rng};
use serde::{Deserialize, Serialize};
use std::{fs::File, net::SocketAddr};
use std::{io::Write, time::Duration};
use tokio::{net::TcpStream, select, sync::oneshot, time::interval};

#[derive(Debug, Serialize, Deserialize)]
pub struct ClientConfig {
    cluster_name: String,
    location: String,
    server_id: NodeId,
    requests: Vec<RequestInterval>,
    local_deployment: Option<bool>,
    kill_signal_sec: Option<(u64, NodeId)>,
    sync_time: Option<Timestamp>,
    summary_filepath: String,
    output_filepath: String,
}

pub struct Client;
type Timestamp = i64;
const REQUEST_DATA_BUFFER_SIZE: usize = 8000;
const INCOMING_MESSAGE_BUFFER_SIZE: usize = 100;
const RETRY_INITIAL_CONNECTION_TIMEOUT: Duration = Duration::from_secs(1);

impl Client {
    pub async fn run(mut config: ClientConfig) {
        // Get connection to server
        let is_local = config.local_deployment.unwrap_or(false);
        let server_address = get_node_addr(&config.cluster_name, config.server_id, is_local)
            .expect("Couldn't resolve server IP");
        let (mut from_server_conn, to_server_conn) =
            Client::get_server_connection(server_address).await;
        // Wait for server to signal start
        info!("Waiting for start signal from server");
        match from_server_conn.next().await {
            Some(Ok(ServerMessage::StartSignal(start_time))) => {
                Self::wait_until_sync_time(&mut config, start_time).await;
            }
            _ => panic!("Error waiting for handshake message"),
        }
        // Spawn reader and writer actors
        info!("Starting requests");
        let (total_requests_tx, total_requests_rx) = tokio::sync::oneshot::channel();
        let reader_task = tokio::spawn(Self::reader_actor(from_server_conn, total_requests_rx));
        let writer_task = tokio::spawn(Self::writer_actor(
            to_server_conn,
            total_requests_tx,
            config.requests.clone(),
        ));
        // Collect request data and shutdown cluster
        let (request_data, response_data) = tokio::join!(writer_task, reader_task);
        let (request_data, mut server_writer) = request_data.expect("Error collecting requests");
        server_writer.send(ClientMessage::Done).await.unwrap();
        let response_data = response_data.expect("Error collecting responses");
        info!(
            "Client finished: collected {} responses",
            response_data.len()
        );
        Self::print_results(config, request_data, response_data).expect("Failed to write to file");
    }

    async fn get_server_connection(
        server_address: SocketAddr,
    ) -> (FromServerConnection, ToServerConnection) {
        let mut retry_connection = interval(RETRY_INITIAL_CONNECTION_TIMEOUT);
        loop {
            retry_connection.tick().await;
            match TcpStream::connect(server_address).await {
                Ok(stream) => {
                    stream.set_nodelay(true).unwrap();
                    let mut registration_connection = frame_registration_connection(stream);
                    registration_connection
                        .send(RegistrationMessage::ClientRegister)
                        .await
                        .expect("Couldn't send registration to server");
                    let underlying_stream = registration_connection.into_inner().into_inner();
                    break frame_clients_connection(underlying_stream);
                }
                Err(e) => error!("Unable to connect to server: {e}"),
            }
        }
    }

    // Wait until the scheduled start time to synchronize client starts.
    // If start time has already passed, start immediately.
    async fn wait_until_sync_time(config: &mut ClientConfig, scheduled_start_utc_ms: i64) {
        let now = Utc::now();
        let milliseconds_until_sync = scheduled_start_utc_ms - now.timestamp_millis();
        config.sync_time = Some(milliseconds_until_sync);
        // let sync_time = Utc::now() + chrono::Duration::milliseconds(milliseconds_until_sync);
        if milliseconds_until_sync > 0 {
            tokio::time::sleep(Duration::from_millis(milliseconds_until_sync as u64)).await;
        } else {
            warn!("Started after synchronization point!");
        }
    }

    async fn reader_actor(
        from_server_conn: FromServerConnection,
        mut total_responses_tx: oneshot::Receiver<usize>,
    ) -> Vec<(CommandId, Timestamp)> {
        let mut response_data = Vec::with_capacity(REQUEST_DATA_BUFFER_SIZE);
        let mut buf_reader = from_server_conn.ready_chunks(INCOMING_MESSAGE_BUFFER_SIZE);
        // Collect responses and wait for number of responses to be established
        let total_responses = loop {
            select! {
                biased;
                Some(messages) = buf_reader.next() => Self::handle_response(&mut response_data, messages),
                Ok(num_requests) = &mut total_responses_tx => break num_requests,
            }
        };
        // Collect rest of responses
        if response_data.len() < total_responses {
            while let Some(messages) = buf_reader.next().await {
                Client::handle_response(&mut response_data, messages);
                if response_data.len() >= total_responses {
                    break;
                }
            }
        }
        return response_data;
    }

    #[inline]
    fn handle_response(
        response_data: &mut Vec<(CommandId, Timestamp)>,
        incoming_messages: Vec<Result<ServerMessage, std::io::Error>>,
    ) {
        for msg in incoming_messages {
            match msg {
                Ok(ServerMessage::StartSignal(_)) => panic!("Recieved unexpected message: {msg:?}"),
                Ok(server_response) => {
                    let cmd_id = server_response.command_id();
                    let response_time = Utc::now().timestamp_millis();
                    response_data.push((cmd_id, response_time));
                }
                Err(err) => panic!("Error deserializing message: {err:?}"),
            }
        }
    }

    async fn writer_actor(
        mut to_server_conn: ToServerConnection,
        total_requests_tx: oneshot::Sender<usize>,
        intervals: Vec<RequestInterval>,
    ) -> (Vec<RequestData>, ToServerConnection) {
        let mut request_data = Vec::with_capacity(REQUEST_DATA_BUFFER_SIZE);
        if intervals.is_empty() {
            // No intervals, nothing to send
            if let Err(_) = total_requests_tx.send(0) {
                error!("Failed to notify reader of total number of requests.")
            }
            return (request_data, to_server_conn);
        }

        let mut request_id = 0;
        let mut rng: StdRng = rand::SeedableRng::from_entropy();
        let mut intervals = intervals.iter();

        // Initialize first interval settings
        let first_interval = intervals.next().unwrap();
        let mut read_ratio = first_interval.read_ratio;
        let mut request_interval = interval(first_interval.get_request_delay());
        let mut next_interval = interval(first_interval.get_interval_duration());
        let _ = next_interval.tick().await;

        // Actor event loop
        loop {
            select! {
                _ = request_interval.tick() => {
                    let key = request_id.to_string();
                    let is_write = rng.gen::<f64>() > read_ratio;
                    let cmd = match is_write {
                        true => KVCommand::Put(key.clone(), key),
                        false => KVCommand::Get(key),
                    };
                    let request = ClientMessage::Append(request_id, cmd);
                    match to_server_conn.send(request).await {
                        Ok(_) => {
                            request_data.push(RequestData::with(is_write));
                            request_id += 1;
                        }
                        Err(e) => error!("Couldn't send command to server: {e}"),
                    }
                },
                _ = next_interval.tick() => {
                    match intervals.next() {
                        Some(new_interval) => {
                            read_ratio = new_interval.read_ratio;
                            next_interval = interval(new_interval.get_interval_duration());
                            next_interval.tick().await;
                            request_interval = interval(new_interval.get_request_delay());
                        },
                        None => break,
                    }
                },
            }
        }
        info!("Finished sending {} requests", request_data.len());
        if let Err(_) = total_requests_tx.send(request_data.len()) {
            error!("Failed to notify reader of total number of requests.")
        }
        return (request_data, to_server_conn);
    }

    fn print_results(
        config: ClientConfig,
        mut request_data: Vec<RequestData>,
        response_data: Vec<(CommandId, Timestamp)>,
    ) -> Result<(), std::io::Error> {
        let config_json = serde_json::to_string_pretty(&config)?;
        let mut summary_file = File::create(config.summary_filepath)?;
        summary_file.write_all(config_json.as_bytes())?;
        summary_file.flush()?;

        for (command_id, response_time) in response_data {
            request_data[command_id].response_time = Some(response_time);
        }
        let file = File::create(config.output_filepath)?;
        let mut writer = Writer::from_writer(file);
        for data in &request_data {
            writer.serialize(data)?;
        }
        writer.flush()?;
        Ok(())
    }
}

#[derive(Debug, Serialize)]
struct RequestData {
    request_time: Timestamp,
    write: bool,
    response_time: Option<Timestamp>,
}

impl RequestData {
    fn with(is_write: bool) -> Self {
        RequestData {
            request_time: Utc::now().timestamp_millis(),
            write: is_write,
            response_time: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct RequestInterval {
    duration_sec: u64,
    requests_per_sec: u64,
    read_ratio: f64,
}

impl RequestInterval {
    fn get_interval_duration(self) -> Duration {
        Duration::from_secs(self.duration_sec)
    }

    fn get_request_delay(self) -> Duration {
        if self.requests_per_sec == 0 {
            return Duration::from_secs(999999);
        }
        let delay_ms = 1000 / self.requests_per_sec;
        assert!(delay_ms != 0);
        Duration::from_millis(delay_ms)
    }
}
