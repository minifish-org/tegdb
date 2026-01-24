use std::rc::Rc;

use tegdb::storage_engine::{EngineConfig, StorageEngine};

fn main() -> tegdb::Result<()> {
    let config = EngineConfig {
        auto_compact: false,
        ..Default::default()
    };

    let mut engine =
        StorageEngine::with_config_and_identifier("rpc://127.0.0.1:9000".to_string(), config)?;

    engine.set(b"hello", b"rpc".to_vec())?;
    let value: Rc<[u8]> = engine
        .get(b"hello")
        .ok_or_else(|| tegdb::Error::Other("missing value after set".to_string()))?;
    println!("value={}", String::from_utf8_lossy(&value));

    Ok(())
}
