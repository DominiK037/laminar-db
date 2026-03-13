package io.laminar.core.exception;

/**
 * Thrown when a WAL record fails CRC32C integrity verification.
 *
 * <p> This indicates one of three root causes:
 * <ul>
 *   <li> Torn write — power loss mid-record left a partial entry on disk </li>
 *   <li> Bit rot — a storage medium silently flipped one or more bits </li>
 *   <li> File corruption — external modification of a segment file </li>
 * </ul>
 *
 * <p> Carries the file offset where corruption was detected and the
 * expected vs computed checksum for diagnostic logging.
 */
public final class DataCorruptionException extends RuntimeException {

    private final long offset;
    private final int  expectedCrc;
    private final int  computedCrc;

    public DataCorruptionException(long offset, int expectedCrc, int computedCrc) {
        super(String.format(
            "CRC32C mismatch at offset %d — expected %s, computed %s",
            offset,
            Integer.toUnsignedString(expectedCrc),
            Integer.toUnsignedString(computedCrc)
        ));
        this.offset      = offset;
        this.expectedCrc = expectedCrc;
        this.computedCrc = computedCrc;
    }

    /** Byte offset in the segment file where corruption was detected. */
    public long getOffset()      { return offset; }

    /** CRC32C value read from the record header. */
    public int  getExpectedCrc() { return expectedCrc; }

    /** CRC32C value recomputed from the actual bytes on disk. */
    public int  getComputedCrc() { return computedCrc; }
}
