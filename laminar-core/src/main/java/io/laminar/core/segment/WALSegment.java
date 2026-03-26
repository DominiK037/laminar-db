package io.laminar.core.segment;

import io.laminar.core.config.WALConfig;
import io.laminar.core.exception.DiskFullException;
import io.laminar.core.exception.SegmentFullException;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * A single append-only WAL segment file on disk.
 *
 * <p> The Write-Ahead Log is split into fixed-size {@code .log} files called segments.
 * This class wraps one {@link FileChannel} and owns all I/O on that file.
 * It is not responsible for rotation, batching, or fsync scheduling —
 * those belong to {@code SegmentManager} and {@code LogAppender} respectively.
 *
 * <p> Why {@link FileChannel} over {@link java.io.FileOutputStream}:
 * {@code force(false)} maps directly to {@code fdatasync()}, positional reads
 * support concurrent access without a shared file pointer, and blocking ops
 * unmount Virtual Threads from their carrier threads cleanly.
 *
 * <p> Why not {@link StandardOpenOption#APPEND}:
 * {@code O_APPEND} atomically seeks to end before every write, overriding
 * explicit positions. This conflicts with the absolute CRC backfill write in
 * {@link io.laminar.core.entry.LogEntry#serialize}. We open with {@code WRITE},
 * seek to end at construction, and enforce append-only discipline ourselves
 * via the Single-Writer rule.
 *
 * <p> Thread safety: not thread-safe for writes. Only {@code LogAppender}
 * may call {@link #append}. Concurrent reads via separate {@code FileChannel}
 * instances are safe.
 *
 * <p> Lifecycle: {@code open → append* → flush* → close}.
 * After {@link #close()}, the segment is permanently immutable.
 */
final class WALSegment implements Closeable {

    /** Number of consecutive zero-byte writes before declaring a channel stall. */
    private static final int MAX_WRITE_STALLS = 3;

    /** File extension for all WAL segment files — e.g. {@code data-003.log}. */
    static final String EXTENSION = WALConfig.SEGMENT_FILE_EXTENSION;

    /** Full path of this segment file — e.g. {@code /var/laminar/data/data-003.log}. */
    private final Path path;

    /**
     * Monotonically increasing segment identifier.
     *
     * <p> The index stores {@code {segmentId, offset}} per key. The reader uses
     * {@code segmentId} to find the correct file, then seeks to {@code offset}.
     * IDs increment by one and never repeat.
     */
    private final int segmentId;

    /** NIO channel for this file — opened once, closed in {@link #close()}. */
    private final FileChannel channel;

    /**
     * Current file size in bytes, maintained internally.
     *
     * <p> {@code channel.size()} is a syscall. At 100k writes/sec, calling it
     * per-write wastes 100k syscalls/sec on data we already track ourselves.
     * The OS is consulted exactly once at construction to seed this value;
     * after that we increment it on every {@link #append} call.
     */
    private long currentSizeBytes;

    /**
     * Set to {@code true} once {@link #close()} is called.
     *
     * <p> Declared {@code volatile} because {@code LogAppender} writes this field
     * while {@code SegmentManager} may read it from a different thread.
     * Without {@code volatile}, the JVM may cache the value in a register
     * and the reading thread may never observe the update.
     */
    private volatile boolean closed = false;

    /**
     * Opens or creates the segment file at {@code path}.
     *
     * <p> If the file already exists (restart into an existing segment), the channel
     * position is set to the end of the file so no existing records are overwritten.
     * For a new file, {@code channel.size()} returns {@code 0} and writing starts
     * at byte {@code 0}.
     *
     * @param path      path to the {@code .log} file
     * @param segmentId unique monotonically increasing segment identifier
     * @throws IOException if the file cannot be opened or created
     */
    WALSegment(Path path, int segmentId) throws IOException {
        this.path      = path;
        this.segmentId = segmentId;
        this.channel   = FileChannel.open(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ
        );

        // One syscall at startup to seed the internal size counter.
        this.currentSizeBytes = channel.size();

        // Seek to end — no-op for new files, prevents overwriting on restart.
        channel.position(currentSizeBytes);
    }

    /**
     * Appends a serialized record to this segment.
     *
     * <p> Buffer contract: {@code buffer} must be flipped before calling
     * ({@code position = 0}, {@code limit = record size}). If not flipped,
     * {@code hasRemaining()} is immediately {@code false} and zero bytes are
     * written silently — no exception is thrown.
     *
     * <p> Return value: the file offset where this record starts,
     * captured before writing. The index stores this value for future reads.
     * The post-write position is not returned — it is not useful for seeking.
     *
     * <p> Partial writes: {@code channel.write()} may write fewer bytes
     * than requested in a single call. We loop until the buffer is drained.
     * If the channel stalls (returns {@code 0} bytes) for {@value #MAX_WRITE_STALLS}
     * consecutive attempts, an {@link IOException} is thrown to prevent an
     * infinite busy-spin under high memory pressure.
     *
     * <p> Durability: bytes land in the OS Page Cache after this call,
     * not on the physical device. Call {@link #flush()} to make them durable.
     *
     * @param buffer serialized record in read mode — must include full payload
     *               so CRC verification in recovery remains intact
     * @return byte offset where this record starts in the file
     * @throws SegmentFullException  if the segment is at {@link WALConfig#MAX_SEGMENT_SIZE_BYTES}
     * @throws DiskFullException     if the OS reports no space (ENOSPC)
     * @throws IllegalStateException if this segment is already closed
     * @throws IOException           if the channel stalls or an I/O error occurs
     */
    long append(ByteBuffer buffer) throws IOException {
        if (closed) {
            throw new IllegalStateException(
                "Attempt to write to closed segment: " + path
            );
        }

        if (isFull()) {
            throw new SegmentFullException(currentSizeBytes);
        }

        // Capture start offset before writing — this is what the index stores.
        final long writeOffset = currentSizeBytes;

        int stallCount = 0;

        try {
            while (buffer.hasRemaining()) {
                int bytesWritten = channel.write(buffer);

                if (bytesWritten < 0) {
                    throw new DiskFullException(
                        buffer.capacity(),
                        path.toFile().getUsableSpace()
                    );
                }

                if (bytesWritten == 0) {
                    // channel.write() returned 0 — no progress made.
                    // Tolerate up to MAX_WRITE_STALLS consecutive stalls;
                    // beyond that, fail fast rather than spin indefinitely.
                    if (++stallCount >= MAX_WRITE_STALLS) {
                        throw new IOException(
                            "FileChannel stalled — 0 bytes written on " +
                            MAX_WRITE_STALLS + " consecutive attempts " +
                            "at segment offset " + writeOffset +
                            " for segment " + path
                        );
                    }
                    continue;
                }

                stallCount = 0;
                currentSizeBytes += bytesWritten;
            }

        } catch (IOException e) {
            if (e.getMessage() != null &&
                e.getMessage().contains("No space left on device")) {
                throw new DiskFullException(
                    buffer.capacity(),
                    path.toFile().getUsableSpace()
                );
            }
            throw e;
        }

        return writeOffset;
    }

    /**
     * Forces all pending writes to reach the physical storage device.
     *
     * <p> Maps to {@code fdatasync()} — flushes file content only, skipping
     * metadata (inode, last-modified time). This saves one I/O operation
     * compared to {@code fsync()} per flush cycle.
     *
     * <p> Called by {@code LogAppender} on the group commit schedule:
     * every {@link WALConfig#FLUSH_INTERVAL_MS} ms or every
     * {@link WALConfig#FLUSH_BUFFER_SIZE_BYTES} bytes, whichever comes first.
     * Also called inside {@link #close()} as a final safety flush.
     *
     * @throws IOException if the flush fails
     */
    void flush() throws IOException {
        if (!closed) {
            channel.force(false);
        }
    }

    /**
     * Returns {@code true} if this segment has reached {@link WALConfig#MAX_SEGMENT_SIZE_BYTES}.
     * <p> When full, {@code SegmentManager} closes this segment and opens a new one.
     */
    boolean isFull() {
        return currentSizeBytes >= WALConfig.MAX_SEGMENT_SIZE_BYTES;
    }

    /** Full file path — e.g. {@code /var/laminar/data/data-003.log}. */
    Path getPath() { return path; }

    /** Monotonically increasing segment ID used by the index to locate records. */
    int getSegmentId() { return segmentId; }

    /** Current file size in bytes from the internal counter — no syscall. */
    long getCurrentSizeBytes() { return currentSizeBytes; }

    /** {@code true} once {@link #close()} has been called. */
    boolean isClosed() { return closed; }

    /**
     * Closes this segment permanently after a final flush.
     *
     * <p> {@code closed} is set to {@code true} inside a {@code finally} block
     * before {@code channel.close()} — if {@code channel.close()} throws,
     * the segment is still permanently locked against further writes.
     * Subsequent calls to {@link #append} will throw {@link IllegalStateException}.
     *
     * <p> Idempotent — safe to call multiple times.
     *
     * @throws IOException if the final flush or channel close fails
     */
    @Override
    public void close() throws IOException {
        if (closed) return;

        try {
            flush();
        } finally {
            closed = true;
            channel.close();
        }
    }
}
