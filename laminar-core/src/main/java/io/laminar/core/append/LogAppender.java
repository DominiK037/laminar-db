package io.laminar.core.append;

import io.laminar.core.config.WALConfig;
import io.laminar.core.entry.LogEntry;
import io.laminar.core.segment.SegmentManager;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Single writer thread that drains the MPSC queue and appends records to the WAL.
 *
 * <p>All writes from all callers funnel through one {@link LinkedBlockingQueue}.
 * A single background thread owned by this class drains the queue, serializes
 * each record, and appends it to the active segment via {@link SegmentManager}.
 *
 * <p><b>Why single writer:</b> lock contention on {@link java.nio.channels.FileChannel}
 * at 100k writes/sec costs 100–1000ms/sec in overhead. A queue push costs ~50ns.
 * One writer eliminates contention; the queue is the coordination point.
 *
 * <p><b>Why {@code Thread} owned internally:</b> the caller interacts only with
 * {@link #submit(LogTask)} and {@link #close()}. Lifecycle is fully encapsulated —
 * no external {@code ExecutorService} to manage, no risk of the thread being
 * submitted twice or run on the wrong pool.
 *
 * <p><b>Group commit:</b> {@link java.util.concurrent.BlockingQueue#poll(long, TimeUnit)}
 * with a {@link WALConfig#FLUSH_INTERVAL_MS} timeout serves as the commit clock.
 * When the timeout fires with no task, the buffered writes are flushed.
 * When {@link WALConfig#FLUSH_BUFFER_SIZE_BYTES} bytes accumulate before the
 * timeout, a flush is triggered early. One thread, one loop, no separate scheduler.
 *
 * <p><b>Buffer reuse:</b> a single {@link ByteBuffer} of {@value #MAX_RECORD_SIZE}
 * bytes is allocated once and reused for every record. {@code LogAppender} is the
 * single writer, so no synchronisation is needed on the buffer. This eliminates
 * {@code ByteBuffer.allocateDirect()} from the hot path — the same principle as
 * {@code ThreadLocal<CRC32C>} in {@link io.laminar.core.entry.LogEntry}.
 *
 * <p><b>LSN:</b> a plain {@code long} field, incremented with {@code ++} before
 * each {@link LogEntry#serialize} call. {@code AtomicLong} would pay for CAS
 * synchronisation that is structurally impossible to need — the Single-Writer
 * rule guarantees only this thread ever touches the LSN counter.
 *
 * <p><b>Shutdown:</b> {@link #close()} sets {@code running = false} and interrupts
 * the thread. The drain loop processes all tasks remaining in the queue before
 * exiting, then issues a final flush. No submitted task is abandoned.
 *
 * <p><b>Thread safety:</b> {@link #submit(LogTask)} is safe to call from any thread.
 * All other methods must be called from the owner thread.
 *
 * <p><b>Lifecycle:</b> {@code construct → submit* → close}.
 * After {@link #close()}, the appender thread has exited and the queue is drained.
 */
public final class LogAppender implements Closeable {

    /**
     * Maximum size of a single serialized record.
     *
     * <p>Equals {@code HEADER_SIZE + MAX_KEY_SIZE + MAX_VALUE_SIZE}.
     * The pre-allocated reuse buffer is sized to this — large enough for
     * any valid record, never reallocated.
     */
    private static final int MAX_RECORD_SIZE =
            WALConfig.HEADER_SIZE_BYTES  +
            WALConfig.MAX_KEY_SIZE_BYTES +
            WALConfig.MAX_VALUE_SIZE_BYTES;

    /**
     * Poison pill placed on the queue by {@link #close()} to wake the
     * {@code poll()} call immediately without interrupting the thread.
     *
     * <p><b>Why not interrupt:</b> {@link java.nio.channels.FileChannel#force(boolean)}
     * throws {@link java.nio.channels.ClosedByInterruptException} if the calling
     * thread's interrupt flag is set, permanently closing the channel as a side effect.
     * A sentinel task avoids this race entirely — the drain loop exits cleanly
     * without any I/O being disrupted.
     */
    private static final LogTask SHUTDOWN_SENTINEL =
            new LogTask(new byte[0], new byte[0], WALConfig.RECORD_TYPE_PUT);

    /** Unbounded MPSC queue — callers push, the appender thread drains. */
    private final LinkedBlockingQueue<LogTask> queue;

    /** Segment lifecycle manager — owns rotation and the active segment. */
    private final SegmentManager segmentManager;

    /** The single writer thread. Created and started at construction time. */
    private final Thread appenderThread;

    /**
     * Drain loop exit signal.
     *
     * <p>Declared {@code volatile} because {@link #close()} writes it from
     * the caller's thread while the appender thread reads it every iteration.
     */
    private volatile boolean running = true;

    /**
     * Monotonically increasing Log Sequence Number.
     *
     * <p>Plain {@code long} — not {@code AtomicLong}. Only the appender thread
     * ever reads or writes this field; CAS synchronisation would be wasted overhead.
     * Incremented with pre-increment ({@code ++lsn}) before each
     * {@link LogEntry#serialize} call so the first record receives LSN 1.
     */
    private long lsn = 0L;

    /**
     * Creates a new appender and starts the background writer thread immediately.
     *
     * <p>The thread is named {@code "log-appender"} and is non-daemon — the JVM
     * will not exit while it is running, giving the shutdown path a chance to
     * drain the queue. Call {@link #close()} to stop it cleanly.
     *
     * @param segmentManager the segment manager to write records into
     */
    public LogAppender(SegmentManager segmentManager) {
        this.segmentManager = segmentManager;
        this.queue          = new LinkedBlockingQueue<>();
        this.appenderThread = new Thread(this::drainLoop, "log-appender");
        this.appenderThread.setDaemon(false);
        this.appenderThread.start();
    }

    /**
     * Submits a write request to the appender queue.
     *
     * <p>Returns immediately — the caller blocks on {@link LogTask#getFuture()}
     * until the appender thread completes or fails the write.
     *
     * <p>Safe to call from any thread concurrently.
     *
     * @param task the write request carrying key, value, type, and future
     */
    public void submit(LogTask task) {
        queue.add(task);
    }

    /**
     * Main drain loop — runs on the appender thread until {@link #close()} is called.
     *
     * <p>Each iteration either:
     * <ul>
     *   <li>Processes a task from the queue and checks the byte threshold, or</li>
     *   <li>Times out after {@link WALConfig#FLUSH_INTERVAL_MS} ms and flushes.</li>
     * </ul>
     *
     * <p>On shutdown ({@code running = false}), the loop exits and all remaining
     * tasks in the queue are drained synchronously before the final flush.
     */
    private void drainLoop() {
        // Pre-allocated reuse buffer — single writer means no contention on this object.
        ByteBuffer buffer = ByteBuffer.allocateDirect(MAX_RECORD_SIZE);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        long unflushedBytes = 0L;

        while (running) {
            try {
                LogTask task = queue.poll(WALConfig.FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);

                if (task == null) {
                    // Group commit window expired — flush whatever is in the OS page cache.
                    flushQuietly();
                    unflushedBytes = 0L;
                    continue;
                }

                if (task == SHUTDOWN_SENTINEL) {
                    // Poison pill from close() — exit the loop and drain remaining real tasks.
                    break;
                }

                unflushedBytes += processTask(task, buffer);

                if (unflushedBytes >= WALConfig.FLUSH_BUFFER_SIZE_BYTES) {
                    flushQuietly();
                    unflushedBytes = 0L;
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        // Drain remaining tasks gracefully — no submitted task is abandoned.
        // Skip any extra sentinels queued by concurrent close() calls.
        LogTask remaining;
        while ((remaining = queue.poll()) != null) {
            if (remaining == SHUTDOWN_SENTINEL) continue;
            unflushedBytes += processTask(remaining, buffer);
            if (unflushedBytes >= WALConfig.FLUSH_BUFFER_SIZE_BYTES) {
                flushQuietly();
                unflushedBytes = 0L;
            }
        }

        // Final flush — channel is open because no interrupt was sent to this thread.
        flushQuietly();
    }

    /**
     * Serializes one task and appends it to the active segment.
     *
     * <p>The reuse buffer is cleared before each record and flipped before
     * passing to {@link SegmentManager#append(ByteBuffer)} — the buffer never
     * escapes this method.
     *
     * <p>LSN is incremented with pre-increment before {@link LogEntry#serialize}
     * so it is embedded in the record before the CRC is computed.
     * If the append fails, the LSN is not rolled back — a gap is acceptable
     * in recovery; a duplicate would not be.
     *
     * @param task   the write request to process
     * @param buffer the reuse buffer — cleared and reused on every call
     * @return bytes successfully written, or {@code 0} if the write failed
     */
    private int processTask(LogTask task, ByteBuffer buffer) {
        try {
            buffer.clear();
            LogEntry.serialize(
                    buffer,
                    task.getKey(),
                    task.getValue(),
                    ++lsn,
                    System.currentTimeMillis(),
                    task.getRecordType()
            );
            buffer.flip();

            segmentManager.append(buffer);
            int bytesWritten = buffer.limit();
            task.complete();
            return bytesWritten;

        } catch (Exception e) {
            task.completeExceptionally(e);
            return 0;
        }
    }

    /**
     * Flushes the active segment, swallowing {@link IOException}.
     *
     * <p>Flush failures are non-fatal per-record — previously acknowledged
     * records are not lost from memory, but may not be durable on disk.
     * Called from the drain loop where exceptions cannot propagate to a caller.
     */
    private void flushQuietly() {
        try {
            segmentManager.flush();
        } catch (IOException ignored) {
        }
    }

    /**
     * Stops the appender thread and drains all remaining tasks.
     *
     * <p>Sets {@code running = false} and enqueues {@link #SHUTDOWN_SENTINEL} to
     * wake the {@code poll()} call immediately. Waits for the thread to exit via
     * {@link Thread#join()} — by the time {@code close()} returns, all tasks that
     * were in the queue at shutdown time have been completed or failed, and a final
     * flush has been issued.
     *
     * <p><b>Why no interrupt:</b> interrupting the appender thread while it is inside
     * {@link java.nio.channels.FileChannel#force(boolean)} causes a
     * {@link java.nio.channels.ClosedByInterruptException} that permanently closes the
     * channel. A poison pill avoids this race entirely — the thread exits cleanly
     * after all I/O completes.
     *
     * <p>Does not close {@link SegmentManager} — the owner of the segment manager
     * (typically {@code WriteAheadLog}) is responsible for closing it.
     *
     * @throws IOException never thrown directly; declared for {@link Closeable}
     */
    @Override
    public void close() throws IOException {
        running = false;
        queue.add(SHUTDOWN_SENTINEL);
        try {
            appenderThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
