#[cfg(feature = "rpc")]
mod rpc_tests {
    use std::net::{TcpListener, TcpStream};
    use std::process::{Child, Command, Stdio};
    use std::thread;
    use std::time::{Duration, Instant};

    use tempfile::TempDir;

    use tegdb::storage_engine::{EngineConfig, StorageEngine};
    use tegdb::Database;

    fn pick_port() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        listener.local_addr().expect("local addr").port()
    }

    fn wait_for_port(port: u16, timeout: Duration) {
        let addr = format!("127.0.0.1:{port}");
        let start = Instant::now();
        while start.elapsed() < timeout {
            if TcpStream::connect(&addr).is_ok() {
                return;
            }
            thread::sleep(Duration::from_millis(50));
        }
        panic!("tglogd did not open port {port} in time");
    }

    fn tglogd_command(listen: &str, db_path: &str) -> Command {
        if let Ok(binary) = std::env::var("CARGO_BIN_EXE_tglogd") {
            let mut cmd = Command::new(binary);
            cmd.args(["--listen", listen, "--db", db_path]);
            return cmd;
        }

        let target_dir = if cfg!(debug_assertions) {
            "target/debug/tglogd"
        } else {
            "target/release/tglogd"
        };

        if std::path::Path::new(target_dir).exists() {
            let mut cmd = Command::new(target_dir);
            cmd.args(["--listen", listen, "--db", db_path]);
            return cmd;
        }

        let mut cmd = Command::new("cargo");
        cmd.args([
            "run",
            "--features",
            "rpc",
            "--bin",
            "tglogd",
            "--",
            "--listen",
            listen,
            "--db",
            db_path,
        ]);
        cmd
    }

    fn spawn_tglogd(listen: &str, db_path: &str) -> Child {
        let mut cmd = tglogd_command(listen, db_path);
        cmd.stdout(Stdio::null()).stderr(Stdio::null());
        cmd.spawn().expect("spawn tglogd")
    }

    fn stop_tglogd(mut child: Child) {
        let _ = child.kill();
        let _ = child.wait();
    }

    #[test]
    fn rpc_backend_smoke() {
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path().join("rpc.teg");
        let port = pick_port();
        let listen = format!("127.0.0.1:{port}");

        let child = spawn_tglogd(&listen, db_path.to_str().expect("db path"));
        wait_for_port(port, Duration::from_secs(3));

        let config = EngineConfig {
            auto_compact: false,
            ..Default::default()
        };
        let mut engine =
            StorageEngine::with_config_and_identifier(format!("rpc://{listen}"), config)
                .expect("create rpc engine");

        engine.set(b"hello", b"rpc".to_vec()).expect("set value");
        let value = engine.get(b"hello").expect("get value");
        assert_eq!(value.as_ref(), b"rpc");

        stop_tglogd(child);
    }

    #[test]
    fn rpc_backend_multiple_writes() {
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path().join("rpc_multi.teg");
        let port = pick_port();
        let listen = format!("127.0.0.1:{port}");

        let child = spawn_tglogd(&listen, db_path.to_str().expect("db path"));
        wait_for_port(port, Duration::from_secs(3));

        let config = EngineConfig {
            auto_compact: false,
            ..Default::default()
        };
        let mut engine =
            StorageEngine::with_config_and_identifier(format!("rpc://{listen}"), config)
                .expect("create rpc engine");

        for i in 0..20u8 {
            let key = [b'k', i];
            let value = vec![b'v', i, i];
            engine.set(&key, value).expect("set value");
        }

        for i in 0..20u8 {
            let key = [b'k', i];
            let expected = vec![b'v', i, i];
            let value = engine.get(&key).expect("get value");
            assert_eq!(value.as_ref(), expected.as_slice());
        }

        stop_tglogd(child);
    }

    #[test]
    fn rpc_backend_unreachable() {
        let port = pick_port();
        let config = EngineConfig {
            auto_compact: false,
            ..Default::default()
        };
        let result =
            StorageEngine::with_config_and_identifier(format!("rpc://127.0.0.1:{port}"), config);

        assert!(result.is_err());
    }

    #[test]
    fn rpc_database_basic_sql() {
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path().join("rpc_db.teg");
        let port = pick_port();
        let listen = format!("127.0.0.1:{port}");

        let child = spawn_tglogd(&listen, db_path.to_str().expect("db path"));
        wait_for_port(port, Duration::from_secs(3));

        let mut db = Database::open(format!("rpc://{listen}")).expect("open database");
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT(10))")
            .expect("create table");
        db.execute("INSERT INTO t (id, name) VALUES (1, 'rpc')")
            .expect("insert row");

        let result = db.query("SELECT name FROM t WHERE id = 1").expect("query");
        let row = result.rows().first().expect("row");
        let value = row
            .first()
            .and_then(|entry| entry.as_text())
            .expect("string");
        assert_eq!(value, "rpc");

        stop_tglogd(child);
    }
}
