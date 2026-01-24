use std::cell::RefCell;
use std::env;
use std::net::ToSocketAddrs;
use std::time::Duration;

use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::io::{BufReader, BufWriter};
use futures::AsyncReadExt;
use tokio_util::compat::TokioAsyncReadCompatExt;

use capnp::capability::Promise;
use tegdb::backends::FileLogBackend;
use tegdb::error::Result as TegResult;
use tegdb::log::{LogBackend, LogConfig};
use tegdb::log_capnp::log_service;
use tegdb::protocol_utils::{has_protocol, PROTOCOL_FILE, PROTOCOL_NAME_FILE};
use tegdb::storage_engine::DEFAULT_INLINE_VALUE_THRESHOLD;

struct LogServiceImpl {
    backend: RefCell<FileLogBackend>,
}

impl log_service::Server for LogServiceImpl {
    fn append(
        &mut self,
        params: log_service::AppendParams,
        mut results: log_service::AppendResults,
    ) -> Promise<(), capnp::Error> {
        let params = match params.get() {
            Ok(params) => params,
            Err(err) => return Promise::err(err),
        };
        let key = match params.get_key() {
            Ok(key) => key,
            Err(err) => return Promise::err(err),
        };
        let value = match params.get_value() {
            Ok(value) => value,
            Err(err) => return Promise::err(err),
        };
        let outcome = match self.backend.borrow_mut().write_entry(key, value) {
            Ok(outcome) => outcome,
            Err(err) => return Promise::err(capnp::Error::failed(err.to_string())),
        };

        let mut response = results.get();
        response.set_entry_len(outcome.entry_len);
        response.set_value_offset(outcome.value_offset);
        response.set_value_len(outcome.value_len);
        Promise::ok(())
    }

    fn read_value(
        &mut self,
        params: log_service::ReadValueParams,
        mut results: log_service::ReadValueResults,
    ) -> Promise<(), capnp::Error> {
        let params = match params.get() {
            Ok(params) => params,
            Err(err) => return Promise::err(err),
        };
        let data = match self
            .backend
            .borrow_mut()
            .read_value(params.get_offset(), params.get_len())
        {
            Ok(data) => data,
            Err(err) => return Promise::err(capnp::Error::failed(err.to_string())),
        };
        results.get().set_data(&data);
        Promise::ok(())
    }

    fn flush(
        &mut self,
        _params: log_service::FlushParams,
        _results: log_service::FlushResults,
    ) -> Promise<(), capnp::Error> {
        match self.backend.borrow_mut().sync_all() {
            Ok(()) => Promise::ok(()),
            Err(err) => Promise::err(capnp::Error::failed(err.to_string())),
        }
    }

    fn current_size(
        &mut self,
        _params: log_service::CurrentSizeParams,
        mut results: log_service::CurrentSizeResults,
    ) -> Promise<(), capnp::Error> {
        let size = match self.backend.borrow().current_size() {
            Ok(size) => size,
            Err(err) => return Promise::err(capnp::Error::failed(err.to_string())),
        };
        results.get().set_size(size);
        Promise::ok(())
    }
}

fn parse_args() -> TegResult<(String, String)> {
    let mut listen = None;
    let mut db_path = None;

    let mut iter = env::args().skip(1);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--listen" => {
                listen = iter.next();
            }
            "--db" => {
                db_path = iter.next();
            }
            _ => {}
        }
    }

    let listen =
        listen.ok_or_else(|| tegdb::error::Error::Other("Missing --listen address".to_string()))?;
    let db_path =
        db_path.ok_or_else(|| tegdb::error::Error::Other("Missing --db path".to_string()))?;

    Ok((listen, db_path))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> TegResult<()> {
    let (listen, mut db_path) = parse_args()?;

    if has_protocol(&db_path, PROTOCOL_NAME_FILE) {
        // keep as-is
    } else {
        db_path = format!("{PROTOCOL_FILE}{db_path}");
    }

    let config = LogConfig {
        max_key_size: tegdb::log::DEFAULT_MAX_KEY_SIZE,
        max_value_size: tegdb::log::DEFAULT_MAX_VALUE_SIZE,
        initial_capacity: None,
        preallocate_size: None,
        inline_value_threshold: DEFAULT_INLINE_VALUE_THRESHOLD,
        group_commit_interval: Duration::from_millis(0),
    };

    let backend = FileLogBackend::new(db_path, &config)?;
    let service = LogServiceImpl {
        backend: RefCell::new(backend),
    };

    let addr = listen
        .to_socket_addrs()
        .map_err(|err| tegdb::error::Error::Other(format!("Invalid listen address: {err}")))?
        .next()
        .ok_or_else(|| tegdb::error::Error::Other("Invalid listen address".to_string()))?;

    let serve = tokio::task::LocalSet::new()
        .run_until(async move {
            let listener = tokio::net::TcpListener::bind(&addr).await?;
            let log_client: log_service::Client = capnp_rpc::new_client(service);

            loop {
                let (stream, _) = listener.accept().await?;
                stream.set_nodelay(true)?;
                let (reader, writer) = stream.compat().split();
                let network = twoparty::VatNetwork::new(
                    BufReader::new(reader),
                    BufWriter::new(writer),
                    rpc_twoparty_capnp::Side::Server,
                    Default::default(),
                );
                let rpc_system = RpcSystem::new(Box::new(network), Some(log_client.clone().client));
                tokio::task::spawn_local(rpc_system);
            }
            #[allow(unreachable_code)]
            Ok::<(), std::io::Error>(())
        })
        .await;
    serve.map_err(tegdb::error::Error::from)?;

    Ok(())
}
