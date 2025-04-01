# TegDB Design Rationale

## Overview

This document explains the key design decisions and architectural choices made in TegDB, along with their trade-offs and rationale.

## Core Design Principles

### 1. Simplicity and Reliability

**Decision**: Keep the design simple and focused on reliability.

**Rationale**:

- Simpler systems are easier to understand and maintain
- Reduced complexity leads to fewer potential failure points
- Easier to verify correctness and test thoroughly

**Trade-offs**:

- May miss some advanced features
- Less flexibility in certain scenarios
- Potentially lower performance in edge cases

### 2. Two-Layer Architecture

**Decision**: Separate the system into Database and Engine layers.

**Rationale**:

- Clear separation of concerns
- Better maintainability
- Easier to test and modify each layer independently
- Allows for different implementations of each layer

**Trade-offs**:

- Slight overhead from layer communication
- More complex interface between layers
- Need to maintain two separate APIs

### 3. Snapshot Isolation

**Decision**: Implement snapshot isolation for transaction consistency.

**Rationale**:

- Provides good balance between consistency and performance
- Allows concurrent reads without blocking
- Prevents dirty reads and non-repeatable reads
- Simpler to implement than full serializability

**Trade-offs**:

- Write-write conflicts still possible
- May need to handle serialization failures
- Memory overhead from maintaining snapshots

## Component Design Decisions

### 1. SkipList for In-Memory Storage

**Decision**: Use SkipList as the in-memory data structure.

**Rationale**:

- Efficient for both reads and writes
- Supports concurrent operations
- Maintains sorted order naturally
- Good balance of performance and complexity

**Trade-offs**:

- Memory overhead from skip pointers
- Not as cache-friendly as B-trees
- More complex than simple hash tables

### 2. Write-Ahead Logging (WAL)

**Decision**: Implement WAL for durability and recovery.

**Rationale**:

- Ensures durability of committed transactions
- Enables crash recovery
- Allows for efficient log compaction
- Standard approach in database systems

**Trade-offs**:

- Additional disk I/O for writes
- Need to manage log files
- Recovery time depends on log size

### 3. MVCC Implementation

**Decision**: Use MVCC for transaction isolation.

**Rationale**:

- Enables concurrent access
- Provides consistent reads
- Allows for efficient snapshot creation
- Well-understood approach

**Trade-offs**:

- Storage overhead for multiple versions
- Need for garbage collection
- More complex than simple locking

## Performance Considerations

### 1. Memory Management

**Decision**: Implement dynamic memory management with garbage collection.

**Rationale**:

- Prevents memory leaks
- Handles long-running operations
- Automatically manages resources
- Adapts to workload patterns

**Trade-offs**:

- GC overhead
- Potential pauses during collection
- More complex than static allocation

### 2. Disk Management

**Decision**: Use log compaction for disk space management.

**Rationale**:

- Reclaims space from deleted/updated records
- Maintains write performance
- Reduces disk usage
- Background operation

**Trade-offs**:

- Additional I/O during compaction
- Temporary disk space needed
- May impact read performance during compaction

### 3. Concurrency Control

**Decision**: Implement fine-grained locking with deadlock detection.

**Rationale**:

- Maximizes concurrency
- Prevents deadlocks
- Efficient for most workloads
- Well-understood approach

**Trade-offs**:

- Lock overhead
- Potential for deadlocks
- More complex than coarse-grained locking

## Alternative Approaches Considered

### 1. B-Tree vs SkipList

**Why SkipList was chosen**:

- Better concurrency support
- Simpler implementation
- More flexible for concurrent modifications
- No rebalancing needed

**Why B-Tree was not chosen**:

- More complex to implement
- Harder to maintain concurrent access
- Requires rebalancing
- Less flexible for modifications

### 2. Full Serializability vs Snapshot Isolation

**Why Serializable Isolation was chosen**:

- Provides strongest consistency guarantees
- Prevents all types of anomalies (dirty reads, non-repeatable reads, phantom reads)
- Well-suited for a wide range of use cases
- MVCC implementation helps mitigate performance impact

**Why Other Isolation Levels were not chosen**:

- Read Committed: Too weak, allows non-repeatable reads and phantom reads
- Snapshot Isolation: Allows write-write conflicts, not sufficient for all use cases
- Repeatable Read: Still allows phantom reads, not sufficient for all use cases

### 3. Simple Locking vs MVCC

**Why MVCC was chosen**:

- Better concurrency
- No blocking reads
- Consistent snapshots
- Industry standard approach

**Why Simple Locking was not chosen**:

- Lower concurrency
- Potential for deadlocks
- Blocking reads
- Less flexible

## Future Considerations

### 1. Scalability

**Planned Improvements**:

- Distributed support
- Sharding capabilities
- Better horizontal scaling
- Improved concurrency

**Challenges**:

- Maintaining consistency
- Network overhead
- Complex coordination
- Resource management

### 2. Performance

**Planned Improvements**:

- Better memory management
- Optimized disk I/O
- Improved concurrency
- Better caching

**Challenges**:

- Maintaining simplicity
- Resource constraints
- Complex optimizations
- Testing and verification

### 3. Features

**Planned Improvements**:

- Additional index types
- Query optimization
- Better monitoring
- More configuration options

**Challenges**:

- Feature creep
- Maintaining reliability
- Testing complexity
- Documentation needs

## Implementation Guidelines

### 1. Code Organization

**Principles**:

- Clear module boundaries
- Consistent naming
- Comprehensive documentation
- Thorough testing

**Benefits**:

- Easier maintenance
- Better understanding
- Faster development
- Reliable changes

### 2. Error Handling

**Principles**:

- Clear error types
- Proper propagation
- Recovery mechanisms
- User-friendly messages

**Benefits**:

- Better debugging
- Easier recovery
- Clear feedback
- Reliable operation

### 3. Testing

**Principles**:

- Comprehensive unit tests
- Integration tests
- Performance tests
- Stress tests

**Benefits**:

- Early bug detection
- Confidence in changes
- Performance verification
- Reliability assurance

## Conclusion

The design decisions in TegDB prioritize:

1. Reliability and correctness
2. Simplicity and maintainability
3. Performance and scalability
4. Standard approaches and best practices

These choices have resulted in a database that is:

- Easy to understand and maintain
- Reliable and consistent
- Performant for common use cases
- Well-documented and tested

Future improvements will focus on:

- Enhanced scalability
- Better performance
- Additional features
- Improved tooling
