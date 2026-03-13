package io.laminar.core.model;

/**
 * Immutable representation of a fully deserialized WAL record.
 *
 * <p> Returned by the read path after CRC32C verification passes.
 * Carries the complete record including the value payload.
 *
 * @param type      record type — {@link io.laminar.core.config.WALConfig#RECORD_TYPE_PUT}
 *                  or {@link io.laminar.core.config.WALConfig#RECORD_TYPE_DELETE}
 * @param lsn       monotonically increasing log sequence number
 * @param timestamp Unix epoch millis — used for Last-Write-Wins resolution
 * @param key       raw key bytes
 * @param value     raw value bytes — empty array for DELETE records
 */
public record DeserializedEntry(
        byte   type,
        long   lsn,
        long   timestamp,
        byte[] key,
        byte[] value) {

    /**
     * Returns {@code true} if this record is a DELETE tombstone.
     *
     * <p> Prefer this over checking {@code value.length == 0} directly —
     * a PUT record with an empty value would produce a false positive.
     */
    public boolean isTombstone() {
        return type == io.laminar.core.config.WALConfig.RECORD_TYPE_DELETE;
    }
}
