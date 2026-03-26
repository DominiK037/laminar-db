package io.laminar.core.exception;

/**
 * Thrown when a write is attempted on a WAL segment that has reached
 * {@link io.laminar.core.config.WALConfig#MAX_SEGMENT_SIZE_BYTES}.
 *
 * <p> This is an internal signal — callers of {@link io.laminar.core.WriteAheadLog}
 * never see this exception. The {@code SegmentManager} catches it and rolls
 * the active segment before retrying the write transparently.
 */
public final class SegmentFullException extends RuntimeException {

    private final long segmentSizeBytes;

    public SegmentFullException(long segmentSizeBytes) {
        super(String.format(
            "Segment is full at %d bytes — rotation required",
            segmentSizeBytes
        ));
        this.segmentSizeBytes = segmentSizeBytes;
    }

    /** The size in bytes at which the segment was declared full. */
    public long getSegmentSizeBytes() { return segmentSizeBytes; }
}
