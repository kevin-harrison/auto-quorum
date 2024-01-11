use std::collections::HashMap;

pub struct Database {
    db: HashMap<String, String>,
}

impl Database {
    pub fn new() -> Self {
        Self { db: HashMap::new() }
    }

    // pub fn handle_command(&mut self, command: KVCommand) -> Option<&String> {
    //     match command {
    //         KVCommand::Put(key, value) => {
    //             self.db.insert(key, value);
    //             None
    //         }
    //         KVCommand::Delete(key) => {
    //             self.db.remove(&key);
    //             None
    //         }
    //         KVCommand::Get(key) => self.db.get(&key),
    //     }
    // }

    pub fn get(&self, key: &String) -> Option<&String> {
        self.db.get(key)
    }

    pub fn delete(&mut self, key: &String) {
        self.db.remove(key);
    }

    pub fn put(&mut self, key: String, value: String) {
        self.db.insert(key, value);
    }
}
