# FAQ

## Why do I get “TEXT length exceeded”?

`TEXT` columns require a length bound (`TEXT(n)`); insert/update values must be ≤ `n` characters. Increase `n` or shorten the input.

## Can I run JOINs or foreign keys?

Not yet. TegDB currently supports single-table queries with primary-key indexing only.

## Why must my DB path be absolute?

TegDB requires absolute `file:///.../*.teg` URLs to avoid ambiguity under supervisors/cron.

## How do I improve write throughput?

Use group commit (`DurabilityLevel::GroupCommit` with a non-zero interval) to batch fsyncs; expect a small RPO window.

## How do I back up and restore?

Use `tgstream run` for continuous backup; restore with `tgstream restore --to /abs/path/restored.teg`. Verify restores regularly.

## Where are my extension functions?

Ensure the extension is created: `CREATE EXTENSION tegdb_string;` or your custom extension. For custom libs, confirm the `.so/.dylib` path is reachable.
