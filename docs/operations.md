# Operations & Durability

TegDB favors correctness and durability. This page captures operational defaults and tuning points.

## Durability Defaults

- Write-ahead logging with commit markers.
- Per-transaction fsync by default (`DurabilityLevel::Immediate`).
- File locking to prevent concurrent writers.

## Tuning Durability

- Group commit: set `DurabilityLevel::GroupCommit` with `group_commit_interval > 0` to coalesce fsyncs at the cost of slightly higher RPO.
- Preallocation: use `preallocate_size` to reduce fragmentation on first writes.
- Inline threshold: `inline_value_threshold` keeps small values on-page to avoid extra IO.

## Compaction

- Append-only log with periodic compaction.
- Triggering (defaults):
  - Absolute threshold: 10 MiB new bytes since last compact.
  - Ratio threshold: 2.0 fragmentation ratio.
  - Minimum delta: 2 MiB since last compact.
- Set `auto_compact` to true (default) to compact on open when thresholds are met.

## Cache and Memory

- Byte-capped value/page cache (`cache_size_bytes`); tune up for read-heavy workloads, down for small footprints.
- B+tree index is in memory; large key counts increase resident set.

## Recovery

- Crash recovery replays the WAL using commit markers.
- Partial writes are handled by discarding uncommitted segments.

## Observability

- Metrics include bytes read/written, cache hits/misses, fsync count.
- Expose via your own logging/metrics sink; see `engine.metrics()` for raw counters.

## Deployment Tips

- Always use absolute paths for the database file.
- Place the database on durable storage; avoid tmpfs if durability matters.
- For SSDs, group commit can reduce write amplification.
- Back up via `tgstream`; verify restores regularly.
