//! Storage backends for TegDB

pub mod file_log_backend;

#[cfg(feature = "rpc")]
pub mod rpc_log_backend;

use crate::error::{Error, Result};
use crate::log::{LogBackend, LogConfig};
use crate::protocol_utils::{parse_storage_identifier, PROTOCOL_NAME_FILE};

pub use file_log_backend::FileLogBackend;

#[cfg(feature = "rpc")]
pub use rpc_log_backend::RpcLogBackend;

pub fn create_log_backend(identifier: String, config: &LogConfig) -> Result<Box<dyn LogBackend>> {
    let (protocol, _) = parse_storage_identifier(&identifier);
    if protocol == PROTOCOL_NAME_FILE {
        return Ok(Box::new(FileLogBackend::new(identifier, config)?));
    }

    #[cfg(feature = "rpc")]
    {
        if protocol == crate::protocol_utils::PROTOCOL_NAME_RPC {
            return Ok(Box::new(RpcLogBackend::new(identifier, config)?));
        }
    }

    Err(Error::Other(format!(
        "Unsupported log backend protocol: {protocol}"
    )))
}
