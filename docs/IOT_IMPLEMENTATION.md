# Row ID Generation Strategies in TegDB

## Current Implementation vs IOT Approach

### 1. Current Timestamp-Based Approach

**Implementation:**
```rust
let row_id = format!("row_{}", chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0));
let key = format!("{}:{}", insert.table, row_id);
```

**Pros:**
- Simple implementation
- No schema dependency
- Works with any table structure

**Cons:**
- Not SQL standard compliant
- Artificial row IDs waste storage
- No natural clustering
- Potential timestamp collisions
- Doesn't enforce data integrity

### 2. IOT (Index-Organized Table) Approach ⭐ RECOMMENDED

**Implementation:**
```rust
// Force every table to have a PRIMARY KEY
let pk_columns = self.get_primary_key_columns(table_name)?;
let row_key = self.generate_row_key(table_name, &row_data)?; // Uses PK values
// Store: "table:pk_value1:pk_value2:..." -> row_data
```

**Pros:**
- ✅ SQL standard compliance (every table has PK)
- ✅ Natural data clustering by primary key
- ✅ Efficient primary key lookups O(log n)
- ✅ Automatic constraint enforcement
- ✅ No storage waste on artificial IDs
- ✅ Better cache locality for related data
- ✅ Supports composite primary keys
- ✅ Range scans are naturally ordered

**Cons:**
- Requires schema enforcement
- Slightly more complex implementation

## Alternative Approaches Considered

### 3. UUID-Based Row IDs

```rust
let row_id = format!("row_{}", uuid::Uuid::new_v4());
```

**Pros:**
- Globally unique
- No collision risk
- Good for distributed systems

**Cons:**
- 36-byte overhead per row
- No natural ordering
- Random access patterns (poor cache locality)

### 4. Auto-Increment Counters

```rust
let counter = table_counters.get_mut(table_name).unwrap();
let row_id = format!("row_{}", counter.fetch_add(1, Ordering::SeqCst));
```

**Pros:**
- Sequential IDs
- Compact storage
- Predictable ordering

**Cons:**
- Requires persistent counter state
- Concurrency bottleneck
- Still artificial (not using natural PK)

### 5. Content-Hash Based IDs

```rust
let content_hash = hash_row_content(&row_data);
let row_id = format!("row_{:x}", content_hash);
```

**Pros:**
- Deterministic
- Content-addressable storage
- Deduplication potential

**Cons:**
- Hash collision risk
- Complex update semantics
- Not intuitive for SQL users

## Why IOT is the Best Choice

### 1. **SQL Standard Compliance**
Every table should have a primary key according to SQL standards. This enforces good database design practices.

### 2. **Performance Benefits**
```sql
-- Efficient primary key lookup (direct key access)
SELECT * FROM users WHERE id = 123;

-- Efficient range scans (naturally ordered)
SELECT * FROM users WHERE id BETWEEN 100 AND 200;

-- Efficient joins (primary key joins are faster)
SELECT u.name, o.total 
FROM users u JOIN orders o ON u.id = o.user_id;
```

### 3. **Storage Efficiency**
```
Traditional approach:
  Key: "users:row_1640995200000000000"
  Value: {"id": 123, "name": "Alice", "age": 30}
  → Duplicate storage of ID in both key and value

IOT approach:
  Key: "users:000000000000000123"  // Zero-padded for sorting
  Value: {"name": "Alice", "age": 30}
  → No duplicate storage, primary key is implicit in key
```

### 4. **Data Integrity**
```rust
// Automatic primary key constraint enforcement
if self.primary_key_exists(&table_name, &row_data)? {
    return Err(Error::PrimaryKeyViolation);
}
```

## Implementation Details

### Key Format for IOT

```rust
// Single primary key
"users:000000000000000123" → {"name": "Alice", "age": 30}

// Composite primary key  
"order_items:000000000000000123:000000000000000456" 
  → {"quantity": 2, "price": 29.99}
  // order_id: 123, product_id: 456
```

### Sorting Considerations

```rust
fn format_pk_value(value: &SqlValue) -> String {
    match value {
        SqlValue::Integer(i) => format!("{:020}", i), // Zero-padded for lexicographic sorting
        SqlValue::Text(s) => s.clone(),               // Natural string sorting
        SqlValue::Real(r) => format!("{:020.10}", r), // Fixed precision for sorting
        SqlValue::Null => panic!("Primary key cannot be NULL"),
    }
}
```

## Migration Strategy

1. **Phase 1**: Implement IOT alongside current system
2. **Phase 2**: Add validation requiring PRIMARY KEY on new tables
3. **Phase 3**: Migrate existing tables to add PRIMARY KEY constraints
4. **Phase 4**: Remove old timestamp-based system

## Conclusion

The IOT (Index-Organized Table) approach using primary keys as row identifiers is superior because it:

- Follows SQL standards
- Provides better performance
- Uses storage more efficiently  
- Enforces data integrity
- Enables natural clustering
- Supports standard SQL optimization techniques

This approach transforms TegDB from a simple key-value store into a proper relational database with clustered storage.
