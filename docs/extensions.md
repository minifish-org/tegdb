# Extensions

TegDB supports PostgreSQL-style extensions plus a Rust API for registering
functions.

## Built-in Extensions

- `tegdb_string`: string functions (`UPPER`, `LOWER`, `LENGTH`, `TRIM`,
  `SUBSTR`, `REPLACE`, `CONCAT`, `REVERSE`).
- `tegdb_math`: math functions (`ABS`, `CEIL`, `FLOOR`, `ROUND`, `SQRT`, `POW`,
  `MOD`, `SIGN`).

Load via SQL:

```sql
CREATE EXTENSION tegdb_string;
CREATE EXTENSION tegdb_math;
```

## Loading Custom Extensions (SQL)

```sql
CREATE EXTENSION my_extension;
-- or specify path
CREATE EXTENSION my_extension WITH PATH '/abs/path/libmy_extension.so';

-- Remove
DROP EXTENSION my_extension;
```

Extensions persist and auto-load on next open.

## Rust API Registration

```rust
use tegdb::{Database, MathFunctionsExtension, StringFunctionsExtension};

let mut db = Database::open("file:///tmp/demo.teg")?;
db.register_extension(Box::new(StringFunctionsExtension))?;
db.register_extension(Box::new(MathFunctionsExtension))?;
```

## Creating a Loadable Extension (Sketch)

1) New library crate with `crate-type = ["cdylib"]`.
2) Depend on `tegdb`.
3) Implement `Extension` and `ScalarFunction`/`AggregateFunction` as needed.
4) Export `create_extension` returning `ExtensionWrapper` (see
   `examples/extension_template.rs`).
5) Build and place the shared lib where the database can load it, then
   `CREATE EXTENSION your_name;`.

## Listing and Managing

- List: `SELECT * FROM tg_extensions;` (if exposed) or via Rust
  `db.list_extensions()`.
- Remove: `DROP EXTENSION name;` or `db.unregister_extension("name")?`.
