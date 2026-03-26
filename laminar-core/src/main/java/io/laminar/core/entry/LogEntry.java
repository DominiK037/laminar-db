package io.laminar.core.entry;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32C;

import io.laminar.core.config.WALConfig;
import io.laminar.core.model.DeserializedEntry;
import io.laminar.core.model.RecoveryEntry;
import io.laminar.core.exception.DataCorruptionException;

/**
 * Defines the binary wire format for a WAL record and owns the
 * serialization and deserialization logic.
 *
 * <p> This class is a pure behaviour container — it holds no instance
 * state. All methods are static. Never instantiate this class.
 *
 * <p> Record layout: 32-byte fixed header followed by variable payload.
 * See {@link WALConfig} for exact field offsets and sizes.
 *
 * <p> Thread safety: all methods are stateless. The {@link CRC32C}
 * instance is {@link ThreadLocal} — one per thread, zero allocation
 * on the hot path.
 */
public final class LogEntry {

    private LogEntry() {}

    /**
     * One CRC32C instance per thread — avoids allocating a new object
     * per record on the write hot path.
     *
     * <p> Always call {@link CRC32C#reset()} before use.
     * Never share across threads — ThreadLocal guarantees isolation.
     */
    private static final ThreadLocal<CRC32C> CRC =
            ThreadLocal.withInitial(CRC32C::new);

    /**
     * Returns the exact byte count required to serialize one record.
     *
     * <p> Callers must invoke this before allocating or claiming space
     * in the write buffer — no over-allocation, no guessing.
     *
     * @param keyLength   length of the key in bytes
     * @param valueLength length of the value in bytes
     * @return exact serialized size in bytes
     */
    public static int serializedSize(int keyLength, int valueLength) {
        return WALConfig.HEADER_SIZE_BYTES + keyLength + valueLength;
    }

    /**
     * Serializes a WAL record directly into {@code dest}.
     *
     * <p> The buffer must have at least {@link #serializedSize} bytes
     * of remaining capacity. Position advances by exactly
     * {@code serializedSize(key.length, value.length)} bytes on return.
     *
     * <p> CRC32C is computed over bytes {@code [recordStart + 4 ... end]}
     * via a zero-copy buffer slice — no intermediate heap allocation.
     *
     * @param dest      destination buffer — must be LITTLE_ENDIAN
     * @param key       raw key bytes
     * @param value     raw value bytes
     * @param lsn       monotonically increasing log sequence number
     * @param timestamp Unix epoch millis
     * @param type      {@link WALConfig#RECORD_TYPE_PUT} or
     *                  {@link WALConfig#RECORD_TYPE_DELETE}
     */
    public static void serialize(
            ByteBuffer dest,
            byte[]     key,
            byte[]     value,
            long       lsn,
            long       timestamp,
            byte       type) {

        dest.order(ByteOrder.LITTLE_ENDIAN);

        // Cache the start position — needed for the absolute CRC write
        // at the end. Buffer position moves forward as we write each field;
        // we cannot recover recordStart from the buffer state later.
        final int recordStart = dest.position();

        // --- Header ---

        // CRC slot (bytes 0-3): write zero as placeholder.
        // Cannot compute checksum yet — the data it covers doesn't exist.
        // We return here via absolute putInt() after writing everything.
        dest.putInt(0);

        // TYPE (byte 4): PUT or DELETE
        dest.put(type);

        // RESERVED (bytes 5-7): explicit zero padding.
        // Aligns LSN to byte offset 8 (8-byte boundary).
        // Never leave padding undefined — future readers may inspect these bytes.
        dest.put((byte) 0);
        dest.put((byte) 0);
        dest.put((byte) 0);

        // LSN (bytes 8-15): starts on 8-byte boundary — single CPU fetch
        dest.putLong(lsn);

        // TIMESTAMP (bytes 16-23)
        dest.putLong(timestamp);

        // KEY_SIZE (bytes 24-27)
        dest.putInt(key.length);

        // VAL_SIZE (bytes 28-31)
        dest.putInt(value.length);

        // --- Payload ---

        // KEY: raw bytes starting at byte 32
        dest.put(key);

        // VALUE: immediately follows key
        dest.put(value);

        // --- CRC ---

        // Capture end position before the slice operation
        final int recordEnd = dest.position();

        // Zero-copy slice covering [recordStart + 4 ... recordEnd].
        // duplicate() shares the underlying data without copying bytes.
        // We move the window to exclude the CRC field itself.
        ByteBuffer crcView = dest.duplicate();
        crcView.position(recordStart + WALConfig.OFFSET_TYPE);
        crcView.limit(recordEnd);

        CRC32C crc32c = CRC.get();
        crc32c.reset();
        crc32c.update(crcView);
        final int checksum = (int) crc32c.getValue();

        // Absolute position write — writes at recordStart + 0 without
        // disturbing the buffer's current position.
        // Rewinding would be wrong: recordStart may not be 0 if the
        // buffer holds multiple batched records.
        dest.putInt(recordStart + WALConfig.OFFSET_CRC, checksum);
    }

    /**
     * Deserializes and validates a full WAL record from {@code src}.
     *
     * <p> Buffer position must be at the start of a record. Advances
     * by exactly {@code serializedSize(keySize, valSize)} bytes on return.
     *
     * <p> CRC32C is verified before returning — a mismatch throws
     * {@link DataCorruptionException} with the offset and both checksum
     * values for structured diagnostics.
     *
     * @param src source buffer positioned at record start
     * @return validated {@link DeserializedEntry}
     * @throws DataCorruptionException if CRC32C does not match
     */
    public static DeserializedEntry deserialize(ByteBuffer src) {

        src.order(ByteOrder.LITTLE_ENDIAN);

        final int recordStart = src.position();

        // Read header fields in wire order
        final int  storedCrc = src.getInt();
        final byte type      = src.get();

        // Skip RESERVED padding (bytes 5-7) explicitly.
        // getInt() here would be wrong — it reads 4 bytes and interprets
        // them as one integer, merging TYPE and padding into garbage.
        src.position(src.position() + 3);

        final long lsn       = src.getLong();
        final long timestamp = src.getLong();
        final int  keySize   = src.getInt();
        final int  valSize   = src.getInt();

        // Read payload
        final byte[] key   = new byte[keySize];
        final byte[] value = new byte[valSize];
        src.get(key);
        src.get(value);

        final int recordEnd = src.position();

        // Verify CRC over same range as serialize() — [recordStart + 4 ... recordEnd]
        ByteBuffer crcView = src.duplicate();
        crcView.position(recordStart + WALConfig.OFFSET_TYPE);
        crcView.limit(recordEnd);

        CRC32C crc32c = CRC.get();
        crc32c.reset();
        crc32c.update(crcView);
        final int computedCrc = (int) crc32c.getValue();

        if (computedCrc != storedCrc) {
            throw new DataCorruptionException(recordStart, storedCrc, computedCrc);
        }

        return new DeserializedEntry(type, lsn, timestamp, key, value);
    }

    /**
     * Deserializes only the header and key of a WAL record — skipping
     * the value payload entirely.
     *
     * <p> Used exclusively by {@link io.laminar.core.recovery.WalRecovery}
     * during startup index rebuild. Reading value bytes during recovery
     * would double the I/O cost with no benefit — the index only needs
     * disk locations, not values.
     *
     * <p> CRC32C is still verified — a corrupt record during recovery
     * means the WAL tail must be truncated to the last valid entry.
     *
     * @param src source buffer positioned at record start —
     *            must contain the full record including value bytes.
     *            Value bytes are not read into heap memory but must
     *            be present in the buffer for CRC verification.
     * @param fileId segment file this record belongs to
     * @return validated {@link RecoveryEntry} without value bytes
     * @throws DataCorruptionException if CRC32C does not match
     */
    public static RecoveryEntry deserializeForRecovery(ByteBuffer src, int fileId) {

        src.order(ByteOrder.LITTLE_ENDIAN);

        final int recordStart = src.position();

        final int  storedCrc = src.getInt();
        final byte type      = src.get();
        src.position(src.position() + 3); // skip RESERVED

        final long lsn       = src.getLong();
        final long timestamp = src.getLong();
        final int  keySize   = src.getInt();
        final int  valSize   = src.getInt();

        final byte[] key = new byte[keySize];
        src.get(key);

        // valueOffset is the position immediately after the header —
        // this is where the value starts on disk, which the index needs.
        final long valueOffset = src.position();

        // Skip value bytes — position advances past them without reading.
        // This is the performance win: no allocation, no I/O for value data.
        src.position(src.position() + valSize);

        final int recordEnd = src.position();

        // CRC still covers the full record including the skipped value bytes.
        // We must verify the entire record — not just the header — to detect
        // torn writes at the tail of the WAL.
        ByteBuffer crcView = src.duplicate();
        crcView.position(recordStart + WALConfig.OFFSET_TYPE);
        crcView.limit(recordEnd);

        CRC32C crc32c = CRC.get();
        crc32c.reset();
        crc32c.update(crcView);
        final int computedCrc = (int) crc32c.getValue();

        if (computedCrc != storedCrc) {
            throw new DataCorruptionException(recordStart, storedCrc, computedCrc);
        }

        return new RecoveryEntry(type, lsn, timestamp, key, fileId, valueOffset, valSize);
    }
}
