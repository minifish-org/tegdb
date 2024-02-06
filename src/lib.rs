mod engine;

use engine::Engine;

const OK: &str = "OK\n";
const INVALID_ARGS: &str = "Invalid number of arguments\n";

pub struct DB {
    kv_data: Engine,
}

impl DB {
    pub fn new(kv_data: Engine) -> Self {
        Self { kv_data }
    }

    pub fn handle_get_command(&mut self, data: Vec<&str>) -> String {
        if data.len() != 1 {
            return INVALID_ARGS.to_string();
        }
        let key = data[0];
        String::from_utf8(self.kv_data.get(&key.as_bytes().to_vec())).unwrap()
    }

    pub fn handle_set_command(&mut self, data: Vec<&str>) -> String {
        if data.len() != 2 {
            return INVALID_ARGS.to_string();
        }
        let key = data[0].as_bytes().to_vec();
        let value = data[1].as_bytes().to_vec();
        self.kv_data.set(&key, value);
        OK.to_string()
    }
    
    pub fn handle_del_command(&mut self, data: Vec<&str>) -> String {
        if data.len() != 1 {
            return INVALID_ARGS.to_string();
        }
        let key = data[0].as_bytes().to_vec();
        self.kv_data.delete(&key);
        OK.to_string()
    }
}
