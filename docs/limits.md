# Limits

Current constraints and known gaps:

- Single-threaded engine; no concurrent writers.
- No joins; only single-table queries.
- No foreign keys or subqueries.
- Only primary key is indexed; other predicates scan.
- `TEXT` requires a length bound (e.g., `TEXT(64)`); inserts exceeding the bound error.
- No secondary indexes; no views or triggers.
- Backup/replication via `tgstream` is file-level, not logical.

Planned/possible enhancements (non-breaking allowed):

- Secondary indexes; join support.
- Additional SQL features (aggregates beyond built-ins, subqueries).
- Larger result streaming for huge scans.
- Encryption and advanced retention for backups.
