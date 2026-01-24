use std::net::ToSocketAddrs;

use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::io::{BufReader, BufWriter};
use futures::AsyncReadExt;
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::error::{Error, Result};
use crate::log::{
    KeyMap, LogBackend, LogConfig, ValuePointer, WriteOutcome, LENGTH_FIELD_BYTES,
    STORAGE_FORMAT_VERSION, STORAGE_HEADER_SIZE, STORAGE_MAGIC, TX_COMMIT_MARKER,
};
use crate::log_capnp::log_service;
use crate::protocol_utils::{parse_storage_identifier, PROTOCOL_NAME_RPC};

type ChangeRecord = (Vec<u8>, Option<ValuePointer>);

struct KeyBufferPool {
    buffers: Vec<Vec<u8>>,
    max_key_size: usize,
}

impl KeyBufferPool {
    fn new(capacity: usize, max_key_size: usize) -> Self {
        let mut buffers = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            buffers.push(Vec::with_capacity(max_key_size));
        }
        Self {
            buffers,
            max_key_size,
        }
    }

    fn take(&mut self, min_len: usize) -> Vec<u8> {
        let mut buf = self
            .buffers
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(self.max_key_size.max(min_len)));
        if buf.capacity() < min_len {
            buf.reserve(min_len - buf.capacity());
        }
        buf.clear();
        buf
    }

    fn clone_from(&mut self, data: &[u8]) -> Vec<u8> {
        let mut buf = self.take(data.len());
        buf.extend_from_slice(data);
        buf
    }

    fn recycle(&mut self, mut buf: Vec<u8>) {
        buf.clear();
        self.buffers.push(buf);
    }
}

pub struct RpcLogBackend {
    runtime: tokio::runtime::Runtime,
    local_set: tokio::task::LocalSet,
    client: log_service::Client,
}

impl RpcLogBackend {
    fn connect(
        address: &str,
        local_set: &tokio::task::LocalSet,
        runtime: &tokio::runtime::Runtime,
    ) -> Result<log_service::Client> {
        let address = address.to_string();
        local_set.block_on(runtime, async move {
            let addr = address
                .to_socket_addrs()
                .map_err(|err| Error::Other(format!("RPC address parse error: {err}")))?
                .next()
                .ok_or_else(|| Error::Other("RPC address missing".to_string()))?;

            let stream = tokio::net::TcpStream::connect(&addr)
                .await
                .map_err(|err| Error::Other(format!("RPC connect error: {err}")))?;
            stream
                .set_nodelay(true)
                .map_err(|err| Error::Other(format!("RPC socket error: {err}")))?;

            let (reader, writer) = stream.compat().split();
            let rpc_network = Box::new(twoparty::VatNetwork::new(
                BufReader::new(reader),
                BufWriter::new(writer),
                rpc_twoparty_capnp::Side::Client,
                Default::default(),
            ));

            let mut rpc_system = RpcSystem::new(rpc_network, None);
            let client: log_service::Client =
                rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);
            tokio::task::spawn_local(rpc_system);

            Ok(client)
        })
    }

    fn rpc_error(err: capnp::Error) -> Error {
        Error::Other(format!("RPC error: {err}"))
    }

    fn parse_header(data: &[u8]) -> Result<u64> {
        if data.len() < STORAGE_HEADER_SIZE {
            return Err(Error::CorruptHeader("header too small"));
        }

        if &data[0..STORAGE_MAGIC.len()] != STORAGE_MAGIC {
            return Err(Error::InvalidMagic);
        }

        let version = u16::from_be_bytes([data[6], data[7]]);
        if version != STORAGE_FORMAT_VERSION {
            return Err(Error::UnsupportedVersion(version));
        }

        if data[20] != 1u8 {
            return Err(Error::CorruptHeader("unsupported endianness"));
        }

        let valid_data_end = u64::from_be_bytes([
            data[21], data[22], data[23], data[24], data[25], data[26], data[27], data[28],
        ]);

        Ok(valid_data_end)
    }

    fn build_key_map_from_bytes(
        data: &[u8],
        valid_data_end: u64,
        config: &LogConfig,
    ) -> Result<(KeyMap, u64)> {
        let initial_capacity = config.initial_capacity.unwrap_or(0);
        let mut key_map = KeyMap::new();
        let mut key_pool = KeyBufferPool::new(initial_capacity, config.max_key_size);
        let mut uncommitted_changes: Vec<ChangeRecord> = Vec::with_capacity(initial_capacity);
        let inline_threshold = config.inline_value_threshold;

        let mut pos = STORAGE_HEADER_SIZE as u64;
        let mut last_good_pos = pos;
        let mut commit_marker_seen = false;

        while (pos as usize) < data.len() {
            let entry_start = pos;
            if pos as usize + LENGTH_FIELD_BYTES * 2 > data.len() {
                break;
            }

            let key_len = u32::from_be_bytes([
                data[pos as usize],
                data[pos as usize + 1],
                data[pos as usize + 2],
                data[pos as usize + 3],
            ]);
            pos += LENGTH_FIELD_BYTES as u64;

            let value_len = u32::from_be_bytes([
                data[pos as usize],
                data[pos as usize + 1],
                data[pos as usize + 2],
                data[pos as usize + 3],
            ]);
            pos += LENGTH_FIELD_BYTES as u64;

            if pos >= valid_data_end && key_len == 0 && value_len == 0 {
                break;
            }

            if key_len as usize > config.max_key_size || value_len as usize > config.max_value_size
            {
                break;
            }

            let key_len_usize = key_len as usize;
            if pos as usize + key_len_usize > data.len() {
                break;
            }
            let key_slice = &data[pos as usize..pos as usize + key_len_usize];
            let key_vec = key_pool.clone_from(key_slice);
            pos += key_len as u64;

            if key_vec.as_slice() == TX_COMMIT_MARKER {
                uncommitted_changes.clear();
                commit_marker_seen = true;
                let skip = value_len as u64;
                if pos + skip > data.len() as u64 {
                    break;
                }
                pos += skip;
                last_good_pos = pos;
                continue;
            }

            let key_for_map = key_vec;
            let key_for_undo = key_pool.clone_from(&key_for_map);
            let mut old_value: Option<ValuePointer> = None;

            if value_len == 0 {
                if let Some((old_key, old_val)) = key_map.remove_entry(key_for_map.as_slice()) {
                    key_pool.recycle(old_key);
                    old_value = Some(old_val);
                }
                key_pool.recycle(key_for_map);
            } else {
                let value_offset = entry_start + (LENGTH_FIELD_BYTES * 2) as u64 + key_len as u64;
                let value_len_usize = value_len as usize;
                if pos as usize + value_len_usize > data.len() {
                    key_pool.recycle(key_for_map);
                    key_pool.recycle(key_for_undo);
                    break;
                }

                if value_len_usize <= inline_threshold {
                    let value_buf = data[pos as usize..pos as usize + value_len_usize].to_vec();
                    if let Some((old_key, old_val)) = key_map.remove_entry(key_for_map.as_slice()) {
                        key_pool.recycle(old_key);
                        old_value = Some(old_val);
                    }
                    let value_rc = std::rc::Rc::from(value_buf.into_boxed_slice());
                    key_map.insert(
                        key_for_map,
                        ValuePointer::with_inline(value_offset, value_len, value_rc),
                    );
                } else {
                    if let Some((old_key, old_val)) = key_map.remove_entry(key_for_map.as_slice()) {
                        key_pool.recycle(old_key);
                        old_value = Some(old_val);
                    }
                    key_map.insert(
                        key_for_map,
                        ValuePointer::new_on_disk(value_offset, value_len),
                    );
                }

                pos += value_len as u64;
            }

            uncommitted_changes.push((key_for_undo, old_value));
            last_good_pos = pos;
        }

        if commit_marker_seen {
            for (key, old_value) in uncommitted_changes.into_iter().rev() {
                if let Some(value) = old_value {
                    key_map.insert(key, value);
                } else {
                    key_map.remove(&key);
                }
            }
        }

        let mut active_data_size: u64 = 0;
        for (key, value) in &key_map {
            active_data_size += (LENGTH_FIELD_BYTES * 2) as u64;
            active_data_size += key.len() as u64;
            active_data_size += value.len() as u64;
        }

        if last_good_pos < STORAGE_HEADER_SIZE as u64 {
            return Err(Error::Corrupted("log scan failed".to_string()));
        }

        Ok((key_map, active_data_size))
    }
}

impl LogBackend for RpcLogBackend {
    fn new(identifier: String, _config: &LogConfig) -> Result<Self> {
        let (protocol, address) = parse_storage_identifier(&identifier);
        if protocol != PROTOCOL_NAME_RPC {
            return Err(Error::Other(format!(
                "RpcLogBackend only supports 'rpc://' protocol, got '{protocol}://'"
            )));
        }

        if address.is_empty() {
            return Err(Error::Other("RPC address missing".to_string()));
        }

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| Error::Other(format!("RPC runtime error: {err}")))?;
        let local_set = tokio::task::LocalSet::new();
        let client = Self::connect(address, &local_set, &runtime)?;

        Ok(Self {
            runtime,
            local_set,
            client,
        })
    }

    fn build_key_map(&mut self, config: &LogConfig) -> Result<(KeyMap, u64)> {
        let size = self.current_size()?;
        if size < STORAGE_HEADER_SIZE as u64 {
            return Err(Error::CorruptHeader("storage shorter than header"));
        }

        let mut data = Vec::with_capacity(size as usize);
        let mut offset = 0u64;
        while offset < size {
            let remaining = size.saturating_sub(offset);
            let chunk = remaining.min(u32::MAX as u64) as u32;
            let bytes = self.read_value(offset, chunk)?;
            if bytes.is_empty() {
                break;
            }
            data.extend_from_slice(&bytes);
            offset = offset.saturating_add(bytes.len() as u64);
        }

        let valid_data_end = Self::parse_header(&data)?;
        Self::build_key_map_from_bytes(&data, valid_data_end, config)
    }

    fn write_entry(&mut self, key: &[u8], value: &[u8]) -> Result<WriteOutcome> {
        self.local_set.block_on(&self.runtime, async {
            let mut request = self.client.append_request();
            {
                let mut params = request.get();
                params.set_key(key);
                params.set_value(value);
            }
            let response = request.send().promise.await.map_err(Self::rpc_error)?;
            let result = response.get().map_err(Self::rpc_error)?;
            Ok(WriteOutcome {
                entry_len: result.get_entry_len(),
                value_offset: result.get_value_offset(),
                value_len: result.get_value_len(),
            })
        })
    }

    fn read_value(&mut self, offset: u64, len: u32) -> Result<Vec<u8>> {
        self.local_set.block_on(&self.runtime, async {
            let mut request = self.client.read_value_request();
            {
                let mut params = request.get();
                params.set_offset(offset);
                params.set_len(len);
            }
            let response = request.send().promise.await.map_err(Self::rpc_error)?;
            let result = response.get().map_err(Self::rpc_error)?;
            let data = result.get_data().map_err(Self::rpc_error)?;
            Ok(data.to_vec())
        })
    }

    fn sync_all(&mut self) -> Result<()> {
        self.local_set.block_on(&self.runtime, async {
            let request = self.client.flush_request();
            request.send().promise.await.map_err(Self::rpc_error)?;
            Ok(())
        })
    }

    fn set_len(&mut self, _size: u64) -> Result<()> {
        Err(Error::Other(
            "RPC log backend does not support set_len".to_string(),
        ))
    }

    fn rename_to(&mut self, _new_identifier: String) -> Result<()> {
        Err(Error::Other(
            "RPC log backend does not support rename_to".to_string(),
        ))
    }

    fn current_size(&self) -> Result<u64> {
        self.local_set.block_on(&self.runtime, async {
            let request = self.client.current_size_request();
            let response = request.send().promise.await.map_err(Self::rpc_error)?;
            let result = response.get().map_err(Self::rpc_error)?;
            Ok(result.get_size())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::{DEFAULT_MAX_KEY_SIZE, DEFAULT_MAX_VALUE_SIZE};
    use crate::storage_engine::DEFAULT_INLINE_VALUE_THRESHOLD;

    #[test]
    fn build_key_map_from_empty_log() {
        let mut data = vec![0u8; STORAGE_HEADER_SIZE];
        data[0..STORAGE_MAGIC.len()].copy_from_slice(STORAGE_MAGIC);
        data[6..8].copy_from_slice(&STORAGE_FORMAT_VERSION.to_be_bytes());
        data[20] = 1u8;
        let valid_end = STORAGE_HEADER_SIZE as u64;
        data[21..29].copy_from_slice(&valid_end.to_be_bytes());

        let config = LogConfig {
            max_key_size: DEFAULT_MAX_KEY_SIZE,
            max_value_size: DEFAULT_MAX_VALUE_SIZE,
            initial_capacity: None,
            preallocate_size: None,
            inline_value_threshold: DEFAULT_INLINE_VALUE_THRESHOLD,
            group_commit_interval: std::time::Duration::from_millis(0),
        };

        let valid_data_end = RpcLogBackend::parse_header(&data).unwrap();
        let (key_map, active_size) =
            RpcLogBackend::build_key_map_from_bytes(&data, valid_data_end, &config).unwrap();

        assert!(key_map.is_empty());
        assert_eq!(active_size, 0);
    }
}
