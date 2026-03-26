# Write-Ahead Log Engine — Design Document
# Component: laminar-core | Phase: 1 | Status: Complete

---

## 1. Purpose

laminar-db is a Bitcask-inspired embedded key-value store. The Write-Ahead Log (WAL)
is its durability substrate: every mutation is appended to an immutable, sequentially
ordered log before the engine acknowledges the write. On crash, the engine replays the
log to reconstruct in-memory state and truncates any corrupt tail.

This document covers the architecture, binary format, operational guarantees, failure
modes, and observability surface of the WAL engine as delivered in Phase 1.

**Out of scope for Phase 1:** the read path, live index updates, compaction, and
range scans. Those are addressed in Phases 2–4.

---

## 2. Architecture

### 2.1 Component Map

```
┌──────────────────────────────────────────────────┐
│  WriteAheadLog  (public API — single entry point)│
│  put(key, value)  /  delete(key)  /  close()     │
└────────────┬─────────────────────────────────────┘
             │ submits LogTask (key + value + Future)
             ▼
┌──────────────────────────────────────────────────┐
│  LogAppender  (single writer thread)             │
│  LinkedBlockingQueue<LogTask>  — MPSC            │
│  group commit:  poll(10ms) → batch → fdatasync   │
└────────────┬─────────────────────────────────────┘
             │ serialized ByteBuffer
             ▼
┌──────────────────────────────────────────────────┐
│  SegmentManager                                  │
│  rotateIfFull() → WALSegment.append()            │
└────────────┬─────────────────────────────────────┘
             │ FileChannel.write()
             ▼
     data-000000001.log  …  data-NNNNNNNNN.log
```

**Concurrency model:** Single-Writer, Multi-Reader. All writes are serialised through
`LogAppender`'s queue. Readers (Phase 3) open independent `FileChannel` instances for
concurrent positional reads — no lock contention with the writer.

### 2.2 Write Path

```
caller thread          appender thread
─────────────          ───────────────
put(key, value)
  validateKey/Value
  new LogTask(...)
  queue.put(task)  ──► poll(10ms timeout)
  future.get()         if task: serialize → append → complete(task)
     (blocks)          if timeout: segmentManager.flush()
  returns              if sentinel: drain + final flush + exit
```

### 2.3 Startup Sequence

```
1. WalRecovery.recover(dir)
      scan .log files in ascending order
      for each record: verify CRC32C → apply PUT/DELETE to index
      on first corrupt record: truncate file, stop scanning
      returns Map<BytesKey, RecoveryEntry>

2. SegmentManager(dir)
      open last .log file as active segment
      if full: rotate immediately

3. LogAppender(segmentManager)
      start writer thread
      engine ready to accept writes
```

### 2.4 Shutdown Sequence

```
1. close() adds SHUTDOWN_SENTINEL to queue
2. Appender drains remaining tasks from queue
3. Final fdatasync on active segment
4. SegmentManager.close() → WALSegment.close() → FileChannel.close()
```

---

## 3. Binary Format

All multi-byte fields are **little-endian**. The format is a fixed-size header followed
by a variable-length payload. The header layout is frozen — changing it breaks all
existing segment files.

```
Offset  Size  Field         Description
──────  ────  ─────         ───────────────────────────────────────────────
0       4     CRC32C        Checksum covering bytes [4, end-of-record]
4       1     TYPE          0x01 = PUT  |  0x00 = DELETE (tombstone)
5       3     RESERVED      Explicit zeros — aligns LSN to 8-byte boundary
8       8     LSN           Log Sequence Number, monotonically increasing
16      8     TIMESTAMP     Unix epoch milliseconds
24      4     KEY_SIZE      Length of key in bytes  [1, 64]
28      4     VAL_SIZE      Length of value in bytes [0, 8096]
32      *     KEY           Raw key bytes
32+KEY  *     VALUE         Raw value bytes (absent for DELETE records)
```

**Size constraints:**

| Field     | Min | Max    | Rationale |
|-----------|-----|--------|-----------|
| KEY_SIZE  | 1   | 64     | 10M keys × ~180 B/entry ≈ 1.8 GB index — fits M2 Air RAM |
| VAL_SIZE  | 0   | 8,096  | 2 × (32+64+8096) = 16,192 ≤ 16,384 (one M2 Air OS page) |

**CRC coverage:** bytes 4 through end-of-record. The checksum field itself (bytes 0–3)
is excluded so it can be backfilled after serialisation without rewinding the buffer.

---

## 4. Operational Guarantees

### 4.1 Durability

| Guarantee | Value | Condition |
|-----------|-------|-----------|
| Durability window | ≤ 10 ms | Writes acknowledged in the last group-commit window before a hard crash may be lost |
| Crash safety | All prior windows durable | Any write acknowledged before the last window survived |
| Partial-write safety | Corrupt tail truncated | Power loss mid-record leaves no corrupt data in the index |

The 10 ms window (`FLUSH_INTERVAL_MS`) is a deliberate trade-off: per-write `fdatasync`
caps throughput at ~100–1,000 ops/sec on NVMe. Group commit enables ~100,000 ops/sec
while bounding data loss to one flush window.

### 4.2 Throughput

| Scenario | Sustained Rate |
|----------|----------------|
| Sequential PUT (512 B avg value) | ~100,000 ops/sec |
| Peak within one group-commit window | Bounded by `FLUSH_BUFFER_SIZE_BYTES` = 16 KB |
| Single write latency (P99) | < 15 ms (one commit window + fdatasync latency) |

These figures assume NVMe storage (fdatasync ≈ 1–10 ms) and the M2 Air 16 GB
reference machine. Rotational disk will degrade to ~200–500 ops/sec.

### 4.3 Recovery Time

| Dataset | Expected Recovery Time |
|---------|------------------------|
| 1 GB WAL (16 × 64 MB segments) | < 2 s on NVMe |
| 10 GB WAL (160 segments) | < 20 s |

Recovery is a single sequential read pass over all segment files. It is bounded by
`O(total_WAL_bytes / disk_read_bandwidth)`. No random I/O occurs during recovery.

### 4.4 Segment Lifecycle

- Segment rolls when `currentSizeBytes >= 64 MB` (`MAX_SEGMENT_SIZE_BYTES`).
- Filename format: `data-000000001.log` … `data-999999999.log` (9 zero-padded decimal
  digits). Alphabetical sort equals numerical sort — no separate index file needed.
- At sustained 100k ops/sec with 512 B average record size, one segment lasts ~1.3 s.
  The engine can sustain ~2 billion segments before ID space exhaustion (see §5).

---

## 5. Failure Mode Catalog

| Failure | Detection | Engine Behaviour | Operator Action |
|---------|-----------|-----------------|-----------------|
| **ENOSPC** (disk full) | `DiskFullException` thrown in appender | In-flight task completes exceptionally; caller receives `IOException` | Free disk space, restart process |
| **Partial write / power loss** | CRC32C mismatch during `recover()` | Corrupt tail truncated; all prior records intact; recovery continues | None — automatic on next `open()` |
| **Corrupt header field** (e.g. KEY_SIZE > 64) | Bounds guard in `recoverSegment()` | Treated as corruption; file truncated at corrupt record offset | None — automatic |
| **Corruption in non-last segment** | `DataCorruptionException` during `recover()` | Scanning stops at first corrupt segment; subsequent segments not replayed | Investigate data integrity; restore from backup if records in skipped segments are required |
| **All segments corrupt** | `recover()` returns empty index | Engine opens normally; all prior data treated as absent | Restore from backup |
| **LogAppender thread dies unexpectedly** | `Thread.isAlive() == false` (not yet monitored in Phase 1) | Queue fills; `put()`/`delete()` callers block indefinitely on `future.get()` | Restart process; add thread-death monitoring in Phase 2 |
| **Segment ID overflow** | `nextSegmentId` wraps to negative `int` | `String.format("data-%09d", negativeId)` produces an invalid filename; `SegmentManager` open fails | Not expected in operational lifetime (~2 billion segments). Mitigated in Phase 2 by using `long`. |
| **Stalled FileChannel** | `channel.write()` returns 0 for `MAX_WRITE_STALLS=3` consecutive calls | `IOException` thrown; task completes exceptionally | Investigate OS-level I/O stall; check memory pressure and kernel logs |
| **Interrupted write thread** | `ClosedByInterruptException` permanently closes `FileChannel` | **Not possible in Phase 1** — `close()` uses poison-pill sentinel, never calls `Thread.interrupt()` | N/A |

---

## 6. Observability

The following metrics should be emitted in a production deployment. Phase 1 does not
instrument them; this section is the contract for Phase 2 instrumentation.

### 6.1 Write Path

| Metric | Type | Description |
|--------|------|-------------|
| `wal.writes.total` | Counter | Total write operations (PUT + DELETE) accepted |
| `wal.writes.errors` | Counter | Write operations that completed exceptionally |
| `wal.write.latency_ms` | Histogram | End-to-end latency from `put()`/`delete()` call to future completion. Alert if P99 > 50 ms. |
| `wal.queue.depth` | Gauge | `LinkedBlockingQueue.size()` — sustained elevation indicates appender can't keep up |
| `wal.lsn` | Gauge | Current LSN; rate-of-change = write throughput in ops/sec |

### 6.2 Group Commit

| Metric | Type | Description |
|--------|------|-------------|
| `wal.flush.latency_ms` | Histogram | Duration of each `fdatasync` call. Alert if P99 > 50 ms on NVMe. |
| `wal.flush.bytes_per_commit` | Histogram | Bytes flushed per group-commit window — low values indicate low utilisation |
| `wal.flush.count` | Counter | Total flush operations |

### 6.3 Segment Lifecycle

| Metric | Type | Description |
|--------|------|-------------|
| `wal.segment.count` | Gauge | Total segment files on disk |
| `wal.active_segment.size_bytes` | Gauge | Current active segment size — approaches `MAX_SEGMENT_SIZE_BYTES` before rotation |
| `wal.segment.rotations` | Counter | Number of segment rotations since startup |
| `wal.disk.used_bytes` | Gauge | Total bytes consumed by all segment files |

### 6.4 Recovery (emitted once at startup)

| Metric | Type | Description |
|--------|------|-------------|
| `wal.recovery.duration_ms` | Timer | Total time spent in `WalRecovery.recover()` |
| `wal.recovery.segments_scanned` | Counter | Number of segment files read during recovery |
| `wal.recovery.records_recovered` | Counter | Valid records replayed into the index |
| `wal.recovery.truncations` | Counter | Number of files truncated due to corrupt tail. Non-zero value warrants investigation. |

### 6.5 Alerts

| Alert | Condition | Severity |
|-------|-----------|----------|
| Write latency degraded | P99 `wal.write.latency_ms` > 50 ms for 2 min | Warning |
| Queue saturation | `wal.queue.depth` > 1,000 for 30 s | Critical |
| Disk pressure | `wal.disk.used_bytes` > 80% of partition | Warning |
| Recovery truncation | `wal.recovery.truncations` > 0 on startup | Warning |
| Flush stall | P99 `wal.flush.latency_ms` > 100 ms | Warning |

---

## 7. Configuration Reference

All constants are defined in `WALConfig.java`. No magic numbers exist in the codebase.

| Constant | Value | Derivation |
|----------|-------|-----------|
| `MAX_SEGMENT_SIZE_BYTES` | 64 MB | Bitcask default; aligns with common SSD erase block boundaries; balances recovery scan time vs. file descriptor count |
| `MAX_KEY_SIZE_BYTES` | 64 | 10M keys × ~180 B/entry ≈ 1.8 GB index on M2 Air 16 GB; keys > 64 B should be hashed by the client |
| `MAX_VALUE_SIZE_BYTES` | 8,096 | 2 × (32 + 64 + 8,096) = 16,192 ≤ 16,384 (one M2 Air OS page); two max-size records fit in one page flush |
| `FLUSH_INTERVAL_MS` | 10 | fdatasync ≈ 1–10 ms on NVMe; 10 ms window batches ~1,000 writes per commit at 100k ops/sec |
| `FLUSH_BUFFER_SIZE_BYTES` | 16 KB | Matches M2 Air OS page size; two max records (16,192 B) fill this cleanly before a forced flush |
| `HEADER_SIZE_BYTES` | 32 | 64-bit aligned; fits in half a CPU cache line (64 B); accommodates LSN, timestamp, and size fields with explicit alignment padding |

---

## 8. Known Limitations (Phase 1)

| Limitation | Impact | Resolution Phase |
|------------|--------|-----------------|
| No read path | `get(key)` not implemented | Phase 3 |
| Index not updated on `put()`/`delete()` | `getIndex()` reflects startup state only | Phase 3 |
| No compaction | Deleted keys consume disk indefinitely | Phase 4 |
| No range scans | Hash index supports point lookups only | Out of scope (Bitcask design) |
| Segment ID is `int` | Overflows at ~2 billion segments | Phase 2 (upgrade to `long`) |
| No thread-death detection | Dead appender thread causes callers to block | Phase 2 |
| No checksumming of index in memory | Corrupted index goes undetected until read | Phase 3 |

---

## 9. Component Inventory

| Class | Role | Thread Safety |
|-------|------|---------------|
| `WriteAheadLog` | Public API facade; input validation boundary | Thread-safe for `put`/`delete`; `close` by owner only |
| `LogAppender` | Single writer thread; queue drain; group commit | Single writer; `submit` is thread-safe |
| `LogTask` | Immutable write request; `CompletableFuture` carrier | Immutable after construction |
| `SegmentManager` | Segment naming, creation, rotation | Not thread-safe; appender thread only |
| `WALSegment` | Single `.log` file; `FileChannel` owner | Not thread-safe for writes; concurrent reads via separate channels |
| `WalRecovery` | Startup-only WAL replay; index rebuild | Single-threaded; startup use only |
| `LogEntry` | Binary serialization / deserialization | Stateless static utility; thread-safe |
| `BytesKey` | `byte[]` wrapper with content equality for `HashMap` | Immutable record; thread-safe |
| `RecoveryEntry` | Index entry: key + file location metadata | Immutable record; thread-safe |
| `DeserializedEntry` | Full record for read path (Phase 3) | Immutable record; thread-safe |
| `WALConfig` | Central constants; no instances | Static constants only |

---

*Document version: Phase 1 final. Update when Phase 2 instrumentation or read path changes the contracts in Phase 4, Phase 5, or Phase 6.*
