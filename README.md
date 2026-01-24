# TegDB

TegDB is a lightweight, embedded database engine with a SQL-like interface designed for simplicity, performance, and reliability. It provides ACID transactions, crash recovery, and a compact on-disk format.

Design philosophy: TegDB prioritizes simplicity and reliability over complexity. It uses a single-threaded design to eliminate concurrency bugs, reduce memory overhead, and provide predictable performance.

## Key Features

- Key->offset B+tree index with values stored on disk (small values can inline).
- Bounded caches for predictable memory use and observability.
- Strong durability by default (fsync per commit, configurable group commit).
- ACID transactions with crash recovery and rollback-on-drop.
- Minimal dependencies and clear separation of parser, planner, and storage.
- Extension system for custom scalar and aggregate functions.

## Documentation

- [Documentation Index](docs/README.md)
- [SQL Reference](docs/sql.md)
- [CLI Usage](docs/cli.md)
- [Operations and Durability](docs/operations.md)
- [Extensions](docs/extensions.md)
- [Limits](docs/limits.md)
- [FAQ](docs/faq.md)

## Quick Start

Build the CLIs and run a query:

```bash
git clone https://github.com/minifish-org/tegdb.git
cd tegdb
cargo build --release

cp target/release/tg ~/.cargo/bin/
export PATH="$HOME/.cargo/bin:$PATH"

DB=file:///$(pwd)/quickstart.teg
tg "$DB" --command "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(32));"
tg "$DB" --command "INSERT INTO users (id, name) VALUES (1, 'Alice');"
tg "$DB" --command "SELECT * FROM users;"
```

For `tgstream` setup (continuous backup, MinIO/S3), see `docs/cli.md`.

## Library Usage

```rust
use tegdb::Database;

fn main() -> tegdb::Result<()> {
    let mut db = Database::open("file:///tmp/app.teg")?;
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(32))")?;
    db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")?;
    let rows = db.query("SELECT name FROM users WHERE id = 1")?;
    println!("{} rows", rows.len());
    Ok(())
}
```

## Development

```bash
cargo build
cargo test
./ci_precheck.sh
```

## License

Licensed under AGPL-3.0. See [LICENSE](LICENSE).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
