# laminar-db

A persistent, embedded key-value storage engine for the JVM, built on a
log-structured design inspired by the [Bitcask](https://riak.com/assets/bitcask-intro.pdf) paper.
Optimised for sequential write throughput and deterministic crash recovery.

[![CI](https://img.shields.io/badge/CI-passing-brightgreen?logo=github)](https://github.com/DominiK037/laminar-db/actions/workflows/ci.yml)
[![Java](https://img.shields.io/badge/Java-21-orange)](https://openjdk.org/projects/jdk/21/)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

---

## What it is

laminar-db is a durable, append-only key-value store that prioritises write throughput
and crash safety. Writes are serialised through a single background thread and flushed
to disk via group commit. On restart, the engine replays its Write-Ahead Log, truncates
any corrupt tail, and rebuilds the in-memory key index — no manual recovery step required.

The engine is being built phase by phase, with every component fully tested and
documented before the next begins.

## Documentation

- [Design Document — Write-Ahead Log (Phase 1 Complete)](docs/wal-engine.md)

---

## Architecture

```mermaid
graph TD
    Client[Client Thread] -->|put / delete| Task["LogTask<br/>key + value + CompletableFuture"]
    Client -->|get| ReaderPool[Virtual Thread Reader Pool]
    Task --> Queue["MPSC Ingress Queue<br/>LinkedBlockingQueue"]

    subgraph Write Path - Serialised
        Queue -->|poll every 10 ms| Appender["LogAppender — Single Writer Thread"]
        Appender -->|"LogEntry.serialize<br/>CRC32C + LSN + timestamp"| SM["SegmentManager<br/>rotateIfFull"]
        SM -->|FileChannel.write| Seg["WALSegment<br/>data-NNNNNNNNN.log"]
        Seg -->|"fdatasync<br/>every 10 ms or 16 KB"| Disk[(NVMe)]
        Appender -->|future.complete| Client
    end

    subgraph Read Path - Concurrent Phase 3
        ReaderPool -->|key lookup| Index["In-Memory Index<br/>Map of BytesKey to RecoveryEntry"]
        Index -->|fileId + offset| FC["FileChannel — pread"]
        FC -->|CRC32C verify| Disk
    end

    subgraph Startup Recovery
        WalRecovery["WalRecovery<br/>replay .log files in order"] -->|rebuild index| Index
        WalRecovery -->|truncate corrupt tail| Seg
    end
```

**Concurrency model:** Single-Writer, Multi-Reader. All writes are serialised through
`LogAppender`'s queue. Readers (Phase 3) open independent `FileChannel` instances for
concurrent positional reads with no contention on the write path.

---

## Binary Format

Every record is a fixed 32-byte header followed by a variable-length payload.
All multi-byte fields are little-endian and 64-bit aligned.

```mermaid
block-beta
  columns 8
  A["CRC32C<br/>bytes 0–3"]:2 B["TYPE<br/>byte 4"] C["RESERVED<br/>bytes 5–7"] D["LSN<br/>bytes 8–15"]:2 E["TIMESTAMP<br/>bytes 16–23"]:2
  F["KEY_SIZE<br/>bytes 24–27"]:2 G["VAL_SIZE<br/>bytes 28–31"]:2 H(" "):4
  I["KEY bytes<br/>(variable)"]:4 J["VALUE bytes<br/>(variable)"]:4
```

| Field | Description |
|---|---|
| `CRC32C` | Castagnoli checksum over bytes `[4 … end]`. Hardware-accelerated on x86/ARM. |
| `TYPE` | `0x01` = PUT · `0x00` = DELETE tombstone |
| `LSN` | Log Sequence Number — monotonic, Raft-compatible |
| `TIMESTAMP` | Unix epoch millis |
| `KEY_SIZE` / `VAL_SIZE` | Lengths in bytes. Key: max 64 B · Value: max 8,096 B. |

---

## Status

| Phase | Scope | Tests | Status |
|---|---|---|---|
| 1 — Write-Ahead Log | Append, group commit, crash recovery | 99 | Complete |
| 2 — Index | In-memory key index, live updates | — | Planned |
| 3 — Read Path | Point reads, CRC verification | — | Planned |
| 4 — Compaction | Reclaim disk space from deleted keys | — | Planned |

---

## Getting Started

**Prerequisites:** Java 21+, Maven 3.9+

```bash
git clone https://github.com/DominiK037/laminar-db.git
cd laminar-db
mvn clean install
```

```bash
mvn test                                          # all tests
mvn test -pl laminar-core                         # single module
mvn test -pl laminar-core -Dtest=LogEntryTest     # single class
```

---

## Contributing

### Branch naming

```
feature/<task-id>-<slug>    →  merges into phase/*
phase/<N>-<name>            →  merges into dev when phase is 100% complete
chore/<slug>                →  branches off dev, merges back to dev
docs/<slug>                 →  branches off dev, merges back to dev
fix/<slug>                  →  branches off dev, merges back to dev
```

### Code rules

- No magic numbers — every constant lives in `WALConfig` with a derivation comment
- No generic exceptions — typed exceptions from the `exception` package only
- No Java Serialization — all disk I/O uses manual `ByteBuffer` packing
- Every public method has Javadoc covering the *why*, not just the *what*
- Tests use `@Nested` classes grouped by method, `@DisplayName` on every case

---

## License

MIT © [Rushikesh Khedkar](https://github.com/DominiK037)
