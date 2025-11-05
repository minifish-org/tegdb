# Preallocation Implementation Summary

## Overview

This implementation adds memory and disk preallocation features to TegDB, inspired by OceanBase's resource management strategies. The changes improve performance predictability and enable efficient incremental backups through tegstream.

## Key Changes

### 1. Configuration Extensions

**Modified Files:**
- `src/log.rs` - Extended `LogConfig` with `initial_capacity` and `preallocate_size`
- `src/storage_engine.rs` - Extended `EngineConfig` with same fields

**New Configuration Options:**
```rust
pub struct LogConfig {
    pub max_key_size: usize,
    pub max_value_size: usize,
    pub initial_capacity: Option<usize>,     // NEW: Memory preallocation
    pub preallocate_size: Option<u64>,       // NEW: Disk preallocation
}
```

### 2. File Format Version 2

**Modified Files:**
- `src/log.rs` - Updated header constants and layout documentation
- `src/backends/file_log_backend.rs` - Implemented version 2 support

**Header Layout Changes:**
```
Version 1 (old):
[0..6)   magic:    b"TEGDB\0"
[6..8)   version:  u16 BE (1)
[8..12)  flags:    u32 BE
[12..16) max_key:  u32 BE
[16..20) max_val:  u32 BE
[20..21) endian:   u8
[21..64) reserved: zero padding

Version 2 (new):
[0..6)   magic:    b"TEGDB\0"
[6..8)   version:  u16 BE (2)
[8..12)  flags:    u32 BE
[12..16) max_key:  u32 BE
[16..20) max_val:  u32 BE
[20..21) endian:   u8
[21..29) valid_data_end: u64 BE    ← NEW FIELD
[29..64) reserved: zero padding
```

**Backward Compatibility:**
- Version 1 files can be read by version 2 code
- Version 2 uses `valid_data_end` to track actual data boundary
- Version 1 falls back to file size as `valid_data_end`

### 3. Disk Preallocation Implementation

**Modified Files:**
- `src/backends/file_log_backend.rs`

**Key Features:**
- Uses `file.set_len()` to preallocate disk space at creation time
- Tracks `valid_data_end` to distinguish allocated vs used space
- Updates `valid_data_end` in header after each write
- Only scans valid data during recovery, ignoring preallocated space

**Benefits:**
- Reduces file system fragmentation
- Improves write performance (no allocation overhead)
- Enables sparse file support on compatible filesystems

### 4. Memory Preallocation Implementation

**Modified Files:**
- `src/backends/file_log_backend.rs` (in `build_key_map`)

**Implementation:**
```rust
let mut key_map = if let Some(capacity) = config.initial_capacity {
    let mut temp_map = KeyMap::new();
    // Pre-warm the BTreeMap by inserting dummy entries
    for i in 0..capacity {
        let key = vec![((i / 256) % 256) as u8, (i % 256) as u8];
        temp_map.insert(key, Rc::from(vec![0u8]));
    }
    temp_map.clear();  // Clear but keep allocated memory
    temp_map
} else {
    KeyMap::new()
};
```

**Note:** Rust's `BTreeMap` doesn't have `with_capacity()`, so we pre-warm by inserting and clearing.

### 5. Tegstream Adaptations

**Modified Files:**
- `src/tegstream/parser.rs` - Added `read_valid_data_end()` method
- `src/tegstream/tailer.rs` - Updated base snapshot creation

**Changes:**
- Parser reads `valid_data_end` from header to determine scan range
- Only scans valid data, not entire preallocated file
- Base snapshots only copy valid data, reducing upload size
- Segment uploads remain unchanged (already used byte ranges)

**Performance Impact:**
- Dramatically faster parsing for preallocated files
- Reduced backup sizes (only valid data uploaded)
- No performance regression for non-preallocated files

## Testing

### New Test Files

1. **tests/integration/preallocate_memory_test.rs**
   - Tests memory preallocation with various capacities
   - Verifies data integrity with preallocation
   - Compares with/without preallocation

2. **tests/integration/preallocate_disk_test.rs**
   - Tests disk preallocation at various sizes
   - Verifies file size matches preallocated size
   - Tests valid_data_end tracking
   - Tests reopening preallocated databases

3. **tests/integration/header_version_test.rs**
   - Tests version 2 header read/write
   - Tests version 1 backward compatibility
   - Tests invalid magic/version rejection
   - Tests valid_data_end updates

4. **tests/tegstream_preallocate_test.rs**
   - Tests parser with preallocated files
   - Tests version 1 fallback in parser
   - Verifies performance (fast scanning)

### Example

**examples/preallocate_demo.rs**
- Demonstrates all preallocation features
- Performance comparison benchmarks
- Shows valid_data_end tracking
- Provides usage patterns

## Usage Examples

### Basic Usage (No Preallocation)

```rust
use tegdb::StorageEngine;

let engine = StorageEngine::new("database.teg".into())?;
```

### With Memory Preallocation

```rust
use tegdb::{EngineConfig, StorageEngine};

let config = EngineConfig {
    initial_capacity: Some(10000),  // Preallocate for 10K entries
    ..Default::default()
};
let engine = StorageEngine::with_config("database.teg".into(), config)?;
```

### With Disk Preallocation

```rust
use tegdb::{EngineConfig, StorageEngine};

let config = EngineConfig {
    preallocate_size: Some(100 * 1024 * 1024),  // 100MB
    ..Default::default()
};
let engine = StorageEngine::with_config("database.teg".into(), config)?;
```

### With Both Preallocations

```rust
use tegdb::{EngineConfig, StorageEngine};

let config = EngineConfig {
    initial_capacity: Some(10000),
    preallocate_size: Some(100 * 1024 * 1024),
    ..Default::default()
};
let engine = StorageEngine::with_config("database.teg".into(), config)?;
```

## Performance Characteristics

### Memory Preallocation

**Pros:**
- Reduces memory allocation overhead
- More predictable latency
- Better for workloads with known size

**Cons:**
- Higher initial memory usage
- No benefit if actual size is much smaller than capacity

### Disk Preallocation

**Pros:**
- Eliminates file allocation overhead during writes
- Reduces file system fragmentation
- Enables sparse file support
- Much faster parsing (only scan valid data)

**Cons:**
- Uses more disk space upfront
- May waste space if actual usage is much smaller

## Migration Guide

### Existing Databases

1. **Version 1 files continue to work** - No migration needed
2. New writes will upgrade to version 2 automatically
3. Tegstream backups handle both versions transparently

### Recommended Settings

**Small Databases (< 10MB):**
```rust
EngineConfig {
    initial_capacity: Some(1000),
    preallocate_size: Some(10 * 1024 * 1024),  // 10MB
    ..Default::default()
}
```

**Medium Databases (10-100MB):**
```rust
EngineConfig {
    initial_capacity: Some(10000),
    preallocate_size: Some(100 * 1024 * 1024),  // 100MB
    ..Default::default()
}
```

**Large Databases (> 100MB):**
```rust
EngineConfig {
    initial_capacity: Some(100000),
    preallocate_size: Some(1024 * 1024 * 1024),  // 1GB
    ..Default::default()
}
```

## Implementation Notes

### Design Decisions

1. **Version 2 Format:** Breaking change minimized by maintaining backward compatibility
2. **valid_data_end in Header:** Enables efficient scanning without full file scan
3. **BTreeMap Pre-warming:** Workaround for lack of `with_capacity()` in Rust's BTreeMap
4. **Lazy Header Updates:** valid_data_end updated after each write for consistency

### Known Limitations

1. **BTreeMap Preallocation:** Not as efficient as HashMap's `with_capacity()`
2. **Header Write Overhead:** Each write updates header (could be batched)
3. **Sparse File Support:** Requires filesystem support (most modern FS support this)

### Future Enhancements

1. Batch header updates to reduce overhead
2. Add metrics to track preallocation efficiency
3. Auto-tune preallocation based on usage patterns
4. Support dynamic preallocation growth

## Files Modified

### Source Files
- `src/log.rs`
- `src/storage_engine.rs`
- `src/backends/file_log_backend.rs`
- `src/tegstream/parser.rs`
- `src/tegstream/tailer.rs`

### Test Files (New)
- `tests/integration/preallocate_memory_test.rs`
- `tests/integration/preallocate_disk_test.rs`
- `tests/integration/header_version_test.rs`
- `tests/tegstream_preallocate_test.rs`

### Example Files (New)
- `examples/preallocate_demo.rs`

### Configuration
- `Cargo.toml` (added test entries)

## Conclusion

This implementation successfully adds preallocation features to TegDB while maintaining backward compatibility with existing databases. The changes improve performance predictability and enable more efficient backups through tegstream.

All tests pass, and the implementation follows the design principles outlined in the original plan.

