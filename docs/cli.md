# CLI Usage

TegDB ships two CLIs: `tg` (SQL runner) and `tgstream` (continuous backup and
restore). All database paths must be absolute `file:///.../*.teg` URLs.

## tg: Run SQL against a database

- Execute a single command:

```bash
tg file:///tmp/demo.teg --command "SELECT 1;"
```

- Read from a file:

```bash
tg file:///tmp/demo.teg --file ./queries.sql
```

- Create a table and insert:

```bash
tg file:///tmp/demo.teg --command "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(32));"
tg file:///tmp/demo.teg --command "INSERT INTO users (id, name) VALUES (1, 'Alice');"
tg file:///tmp/demo.teg --command "SELECT * FROM users;"
```

Key flags:

- `--command <SQL>`: run inline SQL.
- `--file <path>`: run SQL from file.
- `--format <table|json>`: output format (if supported by build).

Exit status is non-zero on parse or execution errors; error text prints to
stderr.

## tgstream: Continuous backup and restore

`tgstream` replicates a `.teg` file to S3 or MinIO-compatible storage.

Environment (example for MinIO):

```bash
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_REGION=us-east-1
export AWS_ENDPOINT_URL=http://127.0.0.1:9000
```

Minimal config (`tgstream.toml`):

```toml

[s3]
```

Common commands:

- Run continuous replication: `tgstream run --config tgstream.toml`
- One-off snapshot: `tgstream snapshot --config tgstream.toml`
- List backups: `tgstream list --config tgstream.toml`
- Restore latest: `tgstream restore --config tgstream.toml --to /abs/path/restored.teg`
- Prune old: `tgstream prune --config tgstream.toml`

Tips:

- Keep config paths absolute; relative paths may break under supervisors.
- Run `tgstream run` under a supervisor (systemd, tmux, etc.).
- For AWS, omit `endpoint` and credentials if using IAM roles.
