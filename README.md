# Laminar DB

![Build Status](https://img.shields.io/badge/build-passing-brightgreen)
![License](https://img.shields.io/badge/license-Apache%202.0-blue)
![Java](https://img.shields.io/badge/java-21-orange)

**Laminar DB** is a high-performance, embedded key-value store optimized for sequential write throughput on SSDs. It implements a crash-safe **Write-Ahead Log (WAL)** using Java 21 Virtual Threads and Memory-Mapped I/O (NIO).

## Key Features

* **Sequential IO:** 100% append-only writes to minimize disk seek latency.
* **Virtual Threads:** Non-blocking concurrency model handling thousands of concurrent writers.
* **Crash Safety:** CRC32 checksums and batch-flushed durability.
* **Zero-Copy:** DirectByteBuffer alignment for optimized kernel-to-disk transfer.

## Architecture

Laminar uses a **Single-Writer Ring Buffer** pattern to serialize concurrent writes into a linear log.

```text
+-------------+       +------------------+       +--------------+
| Virtual     |       | Concurrent       |       | Log          |
| Threads     | ----> | Queue            | ----> | Appender     |
+-------------+       +------------------+       +--------------+
   (Propose)               (Batch)                      |
                                                        | (Write)
                                                        v
                                                 +--------------+
                                                 | OS Page      |
                                                 | Cache        |
                                                 +--------------+
                                                        |
                                                        | (Fsync)
                                                        v
                                                  +-----------+
                                                  | NVMe SSD  |
                                                  +-----------+
```

## Modules

* `laminar-core`: The storage engine and WAL implementation.
* `laminar-server`: (Planned) Network layer for remote access.
* `laminar-cli`: (Planned) Command-line administration tool.

## Getting Started

### Prerequisites

* Java 21 or higher
* Maven 3.8+

### Build

```bash
mvn clean install

```

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](https://www.google.com/search?q=LICENSE) file for details.
