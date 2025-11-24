# Compaction Interactive Testing Guide

This guide will help you manually test TegDB's compaction functionality.

## Prerequisites

1. Ensure the project is compiled:
```bash
cargo build --release
```

2. Verify the new parameters are available:
```bash
./target/release/tg --help | grep compaction
```

You should see:
- `--compaction-threshold <BYTES>` - Compaction threshold in bytes (default: 10MB)
- `--compaction-ratio <RATIO>` - Compaction ratio threshold (default: 2.0)

## Interactive Testing Steps

### Step 1: Start REPL (Terminal 1)

```bash
# Use a small threshold for easy testing: 50KB threshold, 1.5 ratio
./target/release/tg test_compaction.teg --compaction-threshold 51200 --compaction-ratio 1.5
```

### Step 2: Monitor File Size (Terminal 2)

Open a new terminal and run:
```bash
watch -n 0.5 'ls -lh test_compaction.teg 2>/dev/null && echo "---" && stat -f%z test_compaction.teg 2>/dev/null || stat -c%s test_compaction.teg 2>/dev/null'
```

### Step 3: Execute Operations in REPL

In Terminal 1's REPL, execute the following in order:

```sql
-- 1. Create table
CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT(500));

-- 2. Insert initial data
INSERT INTO test (id, data) VALUES (1, 'initial_data_1');
INSERT INTO test (id, data) VALUES (2, 'initial_data_2');
INSERT INTO test (id, data) VALUES (3, 'initial_data_3');

-- 3. View initial state
SELECT * FROM test;
```

### Step 4: Trigger Compaction

Continue executing many update operations in the REPL:

```sql
-- Repeatedly update the same record to create log fragmentation
UPDATE test SET data = 'updated_1' WHERE id = 1;
UPDATE test SET data = 'updated_2' WHERE id = 1;
UPDATE test SET data = 'updated_3' WHERE id = 1;
UPDATE test SET data = 'updated_4' WHERE id = 1;
UPDATE test SET data = 'updated_5' WHERE id = 1;
-- ... Continue executing more updates
```

**Observe Terminal 2**: You will see the file size grow first, then suddenly decrease (compaction triggered).

### Step 5: Verify Data

```sql
-- Verify data integrity
SELECT * FROM test;

-- View specific record
SELECT * FROM test WHERE id = 1;
```

## Understanding Compaction Trigger Conditions

Compaction will trigger when **both of the following conditions** are met:

1. **Absolute threshold**: `log_size > compaction_threshold_bytes` (default: 10MB)
2. **Fragmentation ratio**: `log_size / active_data_size > compaction_ratio` (default: 2.0)

Where:
- `log_size`: Total size of the WAL log file (includes deleted/overwritten old data)
- `active_data_size`: Data size of all current valid key-value pairs

## Test Parameter Recommendations

### Quick Testing (Recommended)
- `--compaction-threshold 51200` (50KB)
- `--compaction-ratio 1.5`

### Medium Testing
- `--compaction-threshold 1048576` (1MB)
- `--compaction-ratio 2.0`

### Default Configuration (Production)
- Don't specify parameters (uses default values: 10MB threshold, 2.0 ratio)

## Expected Behavior

1. **File size growth**: Each update writes a new log entry
2. **Compaction trigger**: When conditions are met, file size will suddenly decrease
3. **Data integrity**: All data should remain correct after compaction

## Troubleshooting

If compaction doesn't trigger:
1. Check if both conditions are met (threshold and ratio)
2. Try a smaller threshold: `--compaction-threshold 10240` (10KB)
3. Try a smaller ratio: `--compaction-ratio 1.2`
4. Execute more update operations

## View Help

```bash
./target/release/tg --help
```
