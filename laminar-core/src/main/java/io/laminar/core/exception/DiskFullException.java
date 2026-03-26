package io.laminar.core.exception;

/**
 * Thrown when the underlying storage cannot accept new writes due to
 * insufficient disk space (ENOSPC at the OS level).
 *
 * <p> Unlike {@link SegmentFullException}, this exception is unrecoverable
 * without operator intervention — compaction may free space, but requires
 * space to run. The engine stops accepting writes until disk space is freed.
 *
 * <p> This exception crosses the public API boundary and must be handled
 * by callers of {@link io.laminar.core.WriteAheadLog}.
 */
public final class DiskFullException extends RuntimeException {

    private final long requiredBytes;
    private final long availableBytes;

    public DiskFullException(long requiredBytes, long availableBytes) {
        super(String.format(
            "Disk full — required %d bytes, available %d bytes",
            requiredBytes,
            availableBytes
        ));
        this.requiredBytes   = requiredBytes;
        this.availableBytes  = availableBytes;
    }

    /** Number of bytes the engine attempted to write. */
    public long getRequiredBytes()  { return requiredBytes; }

    /** Number of bytes available on disk at time of failure. */
    public long getAvailableBytes() { return availableBytes; }
}
