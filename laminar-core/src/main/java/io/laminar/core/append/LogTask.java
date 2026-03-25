package io.laminar.core.append;

import java.util.concurrent.CompletableFuture;

/**
 * A single write request travelling from a caller to {@link LogAppender} via the MPSC queue.
 *
 * <p>{@code LogTask} is a pure message — it carries intent and payload, nothing else.
 * All validation (key size, value size, null checks) happens in {@code WriteAheadLog}
 * before the task is created. By the time a task enters the queue it is always valid.
 *
 * <p><b>Why {@code recordType} is an explicit field:</b> record type is caller intent,
 * not a property of the payload. Deriving DELETE from {@code value.length == 0} would
 * make zero-length PUTs impossible and couple the serialization layer to business
 * semantics. The caller declares the verb; the task carries it unchanged.
 *
 * <p><b>Why {@code CompletableFuture}:</b> the caller submits the task and immediately
 * blocks on {@code future.get()}. {@link LogAppender} calls {@link #complete()} after
 * {@code WALSegment.append()} returns, or {@link #completeExceptionally(Throwable)} if
 * the write fails. This gives the caller precise, per-write failure notification without
 * polling or shared mutable state.
 *
 * <p><b>Thread safety:</b> immutable after construction. Safe to read from any thread.
 * {@link CompletableFuture} handles its own synchronisation.
 */
public final class LogTask {

    private final byte[]                    key;
    private final byte[]                    value;
    private final byte                      recordType;
    private final CompletableFuture<Void>   future;

    /**
     * Creates a new write request.
     *
     * @param key        key bytes — must satisfy {@code WALConfig.MAX_KEY_SIZE_BYTES}
     *                   (validated by the caller before construction)
     * @param value      value bytes — must satisfy {@code WALConfig.MAX_VALUE_SIZE_BYTES}
     *                   (validated by the caller before construction)
     * @param recordType {@code WALConfig.RECORD_TYPE_PUT} or
     *                   {@code WALConfig.RECORD_TYPE_DELETE}
     */
    public LogTask(byte[] key, byte[] value, byte recordType) {
        this.key        = key;
        this.value      = value;
        this.recordType = recordType;
        this.future     = new CompletableFuture<>();
    }

    /**
     * Signals the caller that the write was appended to the WAL successfully.
     *
     * <p>Called by {@link LogAppender} after {@code WALSegment.append()} returns.
     * Unblocks any thread waiting on {@link #getFuture()}.
     */
    public void complete() {
        this.future.complete(null);
    }

    /**
     * Signals the caller that the write failed.
     *
     * <p>Called by {@link LogAppender} when an exception is caught during append
     * or flush. The exception propagates to the caller via {@link java.util.concurrent.Future#get()}.
     * Unblocks any thread waiting on {@link #getFuture()}.
     *
     * @param cause the exception that caused the write to fail
     */
    public void completeExceptionally(Throwable cause) {
        this.future.completeExceptionally(cause);
    }

    /** Key bytes as supplied by the caller. */
    public byte[] getKey() { return key; }

    /** Value bytes as supplied by the caller. */
    public byte[] getValue() { return value; }

    /**
     * Record type marker — {@code WALConfig.RECORD_TYPE_PUT} or
     * {@code WALConfig.RECORD_TYPE_DELETE}.
     */
    public byte getRecordType() { return recordType; }

    /**
     * The future the caller blocks on.
     *
     * <p>Exposed so {@link LogAppender} can pass it back to the caller and
     * so tests can inspect completion state directly.
     *
     * @return the {@link CompletableFuture} created at construction time
     */
    public CompletableFuture<Void> getFuture() { return future; }
}
