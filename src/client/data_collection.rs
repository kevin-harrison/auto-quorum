use std::{fs::File, io::Write};

use auto_quorum::common::{kv::CommandId, utils::Timestamp};
use chrono::Utc;
use csv::Writer;
use serde::Serialize;

use crate::configs::ClientConfig;

#[derive(Debug, Serialize, Clone, Copy)]
struct RequestData {
    request_time: Timestamp,
    write: bool,
    response_time: Option<Timestamp>,
}

pub struct ClientData {
    request_data: Vec<RequestData>,
    response_count: usize,
}

impl ClientData {
    pub fn new() -> Self {
        ClientData {
            // TODO: capacity
            request_data: Vec::new(),
            response_count: 0,
        }
    }

    pub fn new_request(&mut self, is_write: bool) {
        let data = RequestData {
            request_time: Utc::now().timestamp_millis(),
            write: is_write,
            response_time: None,
        };
        self.request_data.push(data);
    }

    pub fn new_response(&mut self, command_id: CommandId) {
        let response_time = Utc::now().timestamp_millis();
        self.request_data[command_id].response_time = Some(response_time);
        self.response_count += 1;
    }

    pub fn response_count(&self) -> usize {
        self.response_count
    }

    pub fn request_count(&self) -> usize {
        self.request_data.len()
    }

    pub fn save_summary(&self, config: ClientConfig) -> Result<(), std::io::Error> {
        let config_json = serde_json::to_string_pretty(&config)?;
        let mut summary_file = File::create(config.summary_filepath)?;
        summary_file.write_all(config_json.as_bytes())?;
        summary_file.flush()?;
        Ok(())
    }

    pub fn to_csv(&self, file_path: String) -> Result<(), std::io::Error> {
        let file = File::create(file_path)?;
        let mut writer = Writer::from_writer(file);
        for data in &self.request_data {
            writer.serialize(data)?;
        }
        writer.flush()?;
        Ok(())
    }
}
