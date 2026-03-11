package io.laminar.core.config;

/**
 * Central configuration constants for the Write-Ahead Log engine.
 *
 * <p>No magic numbers are permitted in the codebase. Every tuneable value
 * lives here with an explicit justification for its default.
 */
public final class WALConfig {

    private WALConfig() {}

    // -------------------------------------------------------------------------
    // Segment Limits
    // -------------------------------------------------------------------------

    /**
     * Maximum size of a single WAL segment file before it is rolled over.
     *
     * <p>64MB balances two forces:
     * - Too large: recovery on restart must scan a huge file (slow startup).
     * - Too small: too many file descriptors open, compaction overhead increases.
     * 64MB is the Bitcask default and matches common SSD erase block boundaries.
     */
    public static final long MAX_SEGMENT_SIZE_BYTES = 64L * 1024 * 1024;

    /**
     * Maximum permitted key size in bytes.
     *
     * <p>64 bytes enforced at ingress. Composite keys with UUIDs and namespace
     * separators fit within this bound in practice. Keys exceeding 64 bytes
     * should be hashed (SHA-256 truncated to 32 bytes) by the client before
     * submission — the engine does not hash implicitly.
     *
     * <p>RAM budget: at ~180 bytes per index entry (key + IndexEntry + HashMap node),
     * this engine supports ~10M keys within a 2GB index footprint on this machine.
     * 100M keys would require ~18GB — beyond the M2 Air ceiling.
     */
    public static final int MAX_KEY_SIZE_BYTES = 64;

    /**
     * Maximum permitted value size in bytes.
     *
     * <p>Derived from the Apple Silicon 16KB OS page size (M2 Air) with the
     * goal of fitting exactly 2 records within one page — avoiding internal
     * fragmentation and preventing any single flush from touching a second page.
     *
     * <p>The math:
     * <pre>
     *   One record  = HEADER_SIZE + MAX_KEY_SIZE + VALUE_SIZE
     *               = 32 + 64 + VALUE_SIZE
     *
     *   Two records ≤ 16,384 bytes (one OS page)
     *   2 × (96 + VALUE_SIZE) ≤ 16,384
     *   VALUE_SIZE            ≤ 8,096
     * </pre>
     *
     * <p>At maximum key and value size, two records consume 16,192 bytes —
     * leaving 192 bytes of slack, ensuring the second page is never touched.
     */
    public static final int MAX_VALUE_SIZE_BYTES = 8_096;

    // -------------------------------------------------------------------------
    // Group Commit (Durability vs Throughput)
    // -------------------------------------------------------------------------

    /**
     * How often the LogAppender calls fsync, in milliseconds.
     *
     * <p>fsync on NVMe takes ~1-10ms. Calling it per-write caps throughput
     * at ~100-1000 ops/sec. At 10ms intervals we batch writes and pay the
     * fsync cost once per window — enabling ~100,000 ops/sec throughput.
     *
     * <p>Trade-off: data written in the last FLUSH_INTERVAL_MS before a
     * hard crash may be lost. This is acknowledged and documented behaviour.
     */
    public static final long FLUSH_INTERVAL_MS = 10L;

    /**
     * Flush triggered when the write buffer reaches this size.
     *
     * <p>16KB matches Apple Silicon's OS page size (M2 Air).
     * Handing the kernel exactly one page per flush avoids partial-page
     * write penalties and aligns with the 8,096 byte max value size —
     * two maximum records (16,192 bytes) fill this buffer cleanly
     * before a forced fsync, leaving 192 bytes of slack.
     */
    public static final int FLUSH_BUFFER_SIZE_BYTES = 16 * 1024;

    // -------------------------------------------------------------------------
    // Binary Format — Header Layout
    // These offsets are the contract between the writer and the reader.
    // Change these and every existing .log file becomes unreadable.
    // -------------------------------------------------------------------------

    /** Total size of the fixed record header in bytes. */
    public static final int HEADER_SIZE_BYTES = 32;

    /** Byte offset of the CRC32C checksum field within the header. */
    public static final int OFFSET_CRC       = 0;

    /** Byte offset of the record type field (PUT / DELETE). */
    public static final int OFFSET_TYPE      = 4;

    /** Byte offset of the LSN (Log Sequence Number) field. */
    public static final int OFFSET_LSN       = 8;

    /** Byte offset of the Unix epoch timestamp field. */
    public static final int OFFSET_TIMESTAMP = 16;

    /** Byte offset of the key length field. */
    public static final int OFFSET_KEY_SIZE  = 24;

    /** Byte offset of the value length field. */
    public static final int OFFSET_VAL_SIZE  = 28;

    // -------------------------------------------------------------------------
    // Record Type Markers
    // -------------------------------------------------------------------------

    /** Marker byte indicating a PUT (write) operation. */
    public static final byte RECORD_TYPE_PUT    = 0x01;

    /** Marker byte indicating a DELETE (tombstone) operation. */
    public static final byte RECORD_TYPE_DELETE = 0x00;
}