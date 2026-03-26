package io.laminar.core;

import io.laminar.core.append.LogAppender;
import io.laminar.core.append.LogTask;
import io.laminar.core.config.WALConfig;
import io.laminar.core.model.BytesKey;
import io.laminar.core.model.RecoveryEntry;
import io.laminar.core.recovery.WalRecovery;
import io.laminar.core.segment.SegmentManager;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Public API facade for the Write-Ahead Log engine.
 *
 * <p>This is the only public class in Phase 1. All other classes are
 * internal implementation details. Callers interact exclusively through
 * {@link #put}, {@link #delete}, and {@link #close}.
 *
 * <h3>Startup sequence</h3>
 * <ol>
 *   <li>{@link WalRecovery#recover} scans all segment files and rebuilds
 *       the key index, truncating any corrupt tail from a prior crash.</li>
 *   <li>{@link SegmentManager} opens, resuming from the last clean segment.</li>
 *   <li>{@link LogAppender} starts its writer thread, ready to accept writes.</li>
 * </ol>
 *
 * <h3>Write contract</h3>
 * <p>{@link #put} and {@link #delete} block until the record has been appended
 * to the active segment and acknowledged by the appender thread. Durability
 * (fsync) is governed by the group commit schedule — every
 * {@link WALConfig#FLUSH_INTERVAL_MS} ms or {@link WALConfig#FLUSH_BUFFER_SIZE_BYTES}
 * bytes, whichever comes first.
 *
 * <h3>Validation boundary</h3>
 * <p>All input validation happens here. {@link LogAppender} and
 * {@link io.laminar.core.entry.LogEntry} perform no validation — by the time
 * a {@link LogTask} enters the queue it is guaranteed to be within bounds.
 *
 * <p><b>Thread safety:</b> {@link #put} and {@link #delete} are safe to call
 * from any thread concurrently. {@link #close} must be called once by the
 * owner thread.
 *
 * <p><b>Lifecycle:</b> {@code open -> put* / delete* -> close}.
 */
public final class WriteAheadLog implements Closeable {

    private final SegmentManager            segmentManager;
    private final LogAppender               appender;
    private final Map<BytesKey, RecoveryEntry> index;

    /**
     * Opens the Write-Ahead Log at {@code dataDirectory}.
     *
     * <p>Runs crash recovery before accepting writes — any corrupt tail
     * from a prior crash is truncated and the key index is rebuilt.
     *
     * @param dataDirectory directory where segment files are stored;
     *                      created if it does not exist
     * @throws IOException if recovery, segment open, or directory creation fails
     */
    public WriteAheadLog(java.nio.file.Path dataDirectory) throws IOException {
        this.index          = WalRecovery.recover(dataDirectory);
        this.segmentManager = new SegmentManager(dataDirectory);
        this.appender       = new LogAppender(segmentManager);
    }

    /**
     * Appends a PUT record and blocks until the write is acknowledged.
     *
     * <p>Validation is performed before the task enters the queue:
     * {@code key} and {@code value} must be non-null, {@code key} must be
     * non-empty, and both must be within the configured size limits.
     *
     * @param key   record key — non-null, non-empty,
     *              at most {@link WALConfig#MAX_KEY_SIZE_BYTES} bytes
     * @param value record value — non-null,
     *              at most {@link WALConfig#MAX_VALUE_SIZE_BYTES} bytes
     * @throws IllegalArgumentException if any size constraint is violated
     * @throws IOException              if the append or flush fails
     */
    public void put(byte[] key, byte[] value) throws IOException {
        validateKey(key);
        validateValue(value);
        submit(key, value, WALConfig.RECORD_TYPE_PUT);
    }

    /**
     * Appends a DELETE tombstone and blocks until the write is acknowledged.
     *
     * <p>A tombstone records that the key no longer exists. The value is
     * always empty — the caller does not supply one.
     *
     * @param key record key to delete — non-null, non-empty,
     *            at most {@link WALConfig#MAX_KEY_SIZE_BYTES} bytes
     * @throws IllegalArgumentException if any size constraint is violated
     * @throws IOException              if the append or flush fails
     */
    public void delete(byte[] key) throws IOException {
        validateKey(key);
        submit(key, new byte[0], WALConfig.RECORD_TYPE_DELETE);
    }

    /**
     * Creates a {@link LogTask}, submits it to the appender queue, and blocks
     * on the future until the appender thread completes or fails the write.
     *
     * <p>{@link ExecutionException} is unwrapped — the underlying cause is
     * rethrown directly. If the cause is an {@link IOException} it is rethrown
     * as-is. Any other cause is wrapped in {@link IOException} so callers
     * only need to handle one checked exception type.
     *
     * @throws IOException if the appender thread failed the write
     */
    private void submit(byte[] key, byte[] value, byte recordType) throws IOException {
        LogTask task = new LogTask(key, value, recordType);
        appender.submit(task);
        try {
            task.getFuture().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Write interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException ioe) {
                throw ioe;
            }
            throw new IOException("Write failed", cause);
        }
    }

    /**
     * Validates a key at the public API boundary.
     *
     * @throws IllegalArgumentException if {@code key} is null, empty, or
     *                                  exceeds {@link WALConfig#MAX_KEY_SIZE_BYTES}
     */
    private static void validateKey(byte[] key) {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        if (key.length == 0) {
            throw new IllegalArgumentException("key must not be empty");
        }
        if (key.length > WALConfig.MAX_KEY_SIZE_BYTES) {
            throw new IllegalArgumentException(
                "key length " + key.length +
                " exceeds MAX_KEY_SIZE_BYTES (" + WALConfig.MAX_KEY_SIZE_BYTES + ")"
            );
        }
    }

    /**
     * Validates a value at the public API boundary.
     *
     * @throws IllegalArgumentException if {@code value} is null or exceeds
     *                                  {@link WALConfig#MAX_VALUE_SIZE_BYTES}
     */
    private static void validateValue(byte[] value) {
        if (value == null) {
            throw new IllegalArgumentException("value must not be null");
        }
        if (value.length > WALConfig.MAX_VALUE_SIZE_BYTES) {
            throw new IllegalArgumentException(
                "value length " + value.length +
                " exceeds MAX_VALUE_SIZE_BYTES (" + WALConfig.MAX_VALUE_SIZE_BYTES + ")"
            );
        }
    }

    /**
     * Returns the key index rebuilt at startup.
     *
     * <p>The index reflects the state of the WAL at the time of the last
     * clean shutdown or truncated recovery. It is not updated by subsequent
     * {@link #put} or {@link #delete} calls in Phase 1 — index maintenance
     * is added in Phase 3 alongside the read path.
     *
     * @return live key index — keys with the most recent DELETE are absent
     */
    public Map<BytesKey, RecoveryEntry> getIndex() {
        return index;
    }

    /**
     * Stops the appender thread, drains remaining writes, and closes the
     * active segment.
     *
     * <p>After this call returns, all writes submitted before {@code close()}
     * have been appended and a final flush has been issued.
     *
     * @throws IOException if the appender or segment manager fails to close
     */
    @Override
    public void close() throws IOException {
        try {
            appender.close();
        } finally {
            segmentManager.close();
        }
    }
}
