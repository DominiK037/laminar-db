package io.laminar.core.model;

/**
 * Immutable representation of a WAL record header and key, used
 * exclusively during startup index recovery.
 *
 * <p> Value bytes are intentionally absent — the recovery path skips
 * them entirely to minimise startup I/O. Only the data needed to
 * rebuild the in-memory index is captured here.
 *
 * <p> Carries {@code fileId} and {@code valueOffset} because the
 * recovery path's sole job is populating the index with disk locations.
 *
 * @param type        record type — PUT or DELETE
 * @param lsn         log sequence number
 * @param timestamp   Unix epoch millis
 * @param key         raw key bytes
 * @param fileId      segment file this record belongs to
 * @param valueOffset byte offset of the value within the segment file
 * @param valueSize   size of the value in bytes
 *
 * Note: int over short — avoids unsigned gymnastics;
 * short saves 200MB at 100M keys in production
 * but adds toUnsignedInt() noise throughout codebase
 */
public record RecoveryEntry(
        byte   type,
        long   lsn,
        long   timestamp,
        byte[] key,
        int    fileId,
        long   valueOffset,
        int    valueSize) {

    /**
     * Returns {@code true} if this record is a DELETE tombstone.
     *
     * <p> During recovery, a tombstone means the key must be removed
     * from the index rather than inserted.
     */
    public boolean isTombstone() {
        return type == io.laminar.core.config.WALConfig.RECORD_TYPE_DELETE;
    }
}
