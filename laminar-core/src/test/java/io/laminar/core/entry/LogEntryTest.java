package io.laminar.core.entry;

import io.laminar.core.config.WALConfig;
import io.laminar.core.exception.DataCorruptionException;
import io.laminar.core.model.DeserializedEntry;
import io.laminar.core.model.RecoveryEntry;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("LogEntry binary serialization")
class LogEntryTest {

    // -------------------------------------------------------------------------
    // Shared test data — small enough to be readable, realistic enough to matter
    // -------------------------------------------------------------------------

    private static final byte[] KEY   = "user:101".getBytes();
    private static final byte[] VALUE = "{'name':'rushikesh'}".getBytes();
    private static final long   LSN   = 1L;
    private static final long   TS    = System.currentTimeMillis();

    /**
     * Allocates a direct buffer sized exactly for one record.
     * Mirrors what LogAppender will do on the real write path.
     */
    private ByteBuffer allocateBuffer(byte[] key, byte[] value) {
        int size = LogEntry.serializedSize(key.length, value.length);
        ByteBuffer buf = ByteBuffer.allocateDirect(size);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        return buf;
    }

    @Nested
    @DisplayName("corruption detection")
    class CorruptionDetection {

        @Test
        @DisplayName("flipping one bit in value payload throws DataCorruptionException")
        void singleBitFlipInValueDetected() {
            ByteBuffer buf = allocateBuffer(KEY, VALUE);
            LogEntry.serialize(buf, KEY, VALUE, LSN, TS, WALConfig.RECORD_TYPE_PUT);
            buf.flip();

            // Flip one bit in the value region
            // Value starts at: HEADER_SIZE + KEY.length
            int valueStart = WALConfig.HEADER_SIZE_BYTES + KEY.length;
            byte original  = buf.get(valueStart);
            buf.put(valueStart, (byte)(original ^ 0x01)); // XOR flips the lowest bit

            assertThrows(DataCorruptionException.class,
                () -> LogEntry.deserialize(buf));
        }

        @Test
        @DisplayName("flipping one bit in key payload throws DataCorruptionException")
        void singleBitFlipInKeyDetected() {
            ByteBuffer buf = allocateBuffer(KEY, VALUE);
            LogEntry.serialize(buf, KEY, VALUE, LSN, TS, WALConfig.RECORD_TYPE_PUT);
            buf.flip();

            // Flip one bit in the key region
            int keyStart  = WALConfig.HEADER_SIZE_BYTES;
            byte original = buf.get(keyStart);
            buf.put(keyStart, (byte)(original ^ 0x01));

            assertThrows(DataCorruptionException.class,
                () -> LogEntry.deserialize(buf));
        }

        @Test
        @DisplayName("flipping one bit in LSN header field throws DataCorruptionException")
        void singleBitFlipInHeaderDetected() {
            ByteBuffer buf = allocateBuffer(KEY, VALUE);
            LogEntry.serialize(buf, KEY, VALUE, LSN, TS, WALConfig.RECORD_TYPE_PUT);
            buf.flip();

            // Flip one bit in LSN field (byte 8)
            byte original = buf.get(WALConfig.OFFSET_LSN);
            buf.put(WALConfig.OFFSET_LSN, (byte)(original ^ 0x01));

            assertThrows(DataCorruptionException.class,
                () -> LogEntry.deserialize(buf));
        }

        @Test
        @DisplayName("DataCorruptionException carries correct offset and both CRC values")
        void corruptionExceptionCarriesStructuredDiagnostics() {
            ByteBuffer buf = allocateBuffer(KEY, VALUE);
            LogEntry.serialize(buf, KEY, VALUE, LSN, TS, WALConfig.RECORD_TYPE_PUT);
            buf.flip();

            // Corrupt the value
            int valueStart = WALConfig.HEADER_SIZE_BYTES + KEY.length;
            buf.put(valueStart, (byte)(buf.get(valueStart) ^ 0xFF));

            DataCorruptionException ex = assertThrows(
                DataCorruptionException.class,
                () -> LogEntry.deserialize(buf)
            );

            assertAll(
                // Offset 0 because this is the start of the buffer
                () -> assertEquals(0, ex.getOffset()),
                // The two CRC values must differ — that's the whole point
                () -> assertNotEquals(ex.getExpectedCrc(), ex.getComputedCrc())
            );
        }
    }

    @Nested
    @DisplayName("serialize()")
    class Serialize {

        @Test
        @DisplayName("serialized size matches header + key + value lengths")
        void serializedSizeIsExact() {
            int expected = WALConfig.HEADER_SIZE_BYTES + KEY.length + VALUE.length;
            assertEquals(expected, LogEntry.serializedSize(KEY.length, VALUE.length));
        }

        @Test
        @DisplayName("buffer position advances by exactly serializedSize bytes")
        void bufferPositionAdvancesCorrectly() {
            ByteBuffer buf = allocateBuffer(KEY, VALUE);
            int before = buf.position();

            LogEntry.serialize(buf, KEY, VALUE, LSN, TS, WALConfig.RECORD_TYPE_PUT);

            int after = buf.position();
            assertEquals(LogEntry.serializedSize(KEY.length, VALUE.length), after - before);
        }

        @Test
        @DisplayName("TYPE byte is written at correct header offset")
        void typeByteAtCorrectOffset() {
            ByteBuffer buf = allocateBuffer(KEY, VALUE);
            LogEntry.serialize(buf, KEY, VALUE, LSN, TS, WALConfig.RECORD_TYPE_PUT);

            // Read TYPE directly from the known offset — bypasses deserialize()
            buf.flip();
            byte type = buf.get(WALConfig.OFFSET_TYPE);
            assertEquals(WALConfig.RECORD_TYPE_PUT, type);
        }

        @Test
        @DisplayName("RESERVED padding bytes 5-7 are explicitly zero")
        void paddingBytesAreZero() {
            ByteBuffer buf = allocateBuffer(KEY, VALUE);
            LogEntry.serialize(buf, KEY, VALUE, LSN, TS, WALConfig.RECORD_TYPE_PUT);

            buf.flip();
            assertEquals(0, buf.get(WALConfig.OFFSET_TYPE + 1));
            assertEquals(0, buf.get(WALConfig.OFFSET_TYPE + 2));
            assertEquals(0, buf.get(WALConfig.OFFSET_TYPE + 3));
        }

        @Test
        @DisplayName("DELETE tombstone sets TYPE byte to RECORD_TYPE_DELETE")
        void deleteTombstoneTypeIsCorrect() {
            ByteBuffer buf = allocateBuffer(KEY, new byte[0]);
            LogEntry.serialize(buf, KEY, new byte[0], LSN, TS, WALConfig.RECORD_TYPE_DELETE);

            buf.flip();
            byte type = buf.get(WALConfig.OFFSET_TYPE);
            assertEquals(WALConfig.RECORD_TYPE_DELETE, type);
        }
    }

    @Nested
    @DisplayName("deserialize()")
    class Deserialize {

        @Test
        @DisplayName("round-trip: deserialized entry matches original data")
        void roundTripPreservesAllFields() {
            ByteBuffer buf = allocateBuffer(KEY, VALUE);
            LogEntry.serialize(buf, KEY, VALUE, LSN, TS, WALConfig.RECORD_TYPE_PUT);

            buf.flip();
            DeserializedEntry entry = LogEntry.deserialize(buf);

            assertAll(
                () -> assertEquals(WALConfig.RECORD_TYPE_PUT, entry.type()),
                () -> assertEquals(LSN, entry.lsn()),
                () -> assertEquals(TS,  entry.timestamp()),
                () -> assertArrayEquals(KEY,   entry.key()),
                () -> assertArrayEquals(VALUE, entry.value())
            );
        }

        @Test
        @DisplayName("DELETE tombstone round-trip has empty value and isTombstone() true")
        void deleteTombstoneRoundTrip() {
            ByteBuffer buf = allocateBuffer(KEY, new byte[0]);
            LogEntry.serialize(buf, KEY, new byte[0], LSN, TS, WALConfig.RECORD_TYPE_DELETE);

            buf.flip();
            DeserializedEntry entry = LogEntry.deserialize(buf);

            assertAll(
                () -> assertTrue(entry.isTombstone()),
                () -> assertEquals(0, entry.value().length)
            );
        }

        @Test
        @DisplayName("empty key and value serialize and deserialize correctly")
        void emptyKeyAndValue() {
            byte[] emptyKey   = new byte[0];
            byte[] emptyValue = new byte[0];

            ByteBuffer buf = allocateBuffer(emptyKey, emptyValue);
            LogEntry.serialize(buf, emptyKey, emptyValue, LSN, TS, WALConfig.RECORD_TYPE_PUT);

            buf.flip();
            DeserializedEntry entry = LogEntry.deserialize(buf);

            assertAll(
                () -> assertEquals(0, entry.key().length),
                () -> assertEquals(0, entry.value().length)
            );
        }

        @Test
        @DisplayName("maximum key and value sizes serialize and deserialize correctly")
        void maxKeySizeAndValueSize() {
            byte[] maxKey   = new byte[WALConfig.MAX_KEY_SIZE_BYTES];
            byte[] maxValue = new byte[WALConfig.MAX_VALUE_SIZE_BYTES];

            // Fill with non-zero pattern to catch zeroing bugs
            for (int i = 0; i < maxKey.length;   i++) maxKey[i]   = (byte)(i % 127);
            for (int i = 0; i < maxValue.length; i++) maxValue[i] = (byte)(i % 127);

            ByteBuffer buf = allocateBuffer(maxKey, maxValue);
            LogEntry.serialize(buf, maxKey, maxValue, LSN, TS, WALConfig.RECORD_TYPE_PUT);

            buf.flip();
            DeserializedEntry entry = LogEntry.deserialize(buf);

            assertArrayEquals(maxKey,   entry.key());
            assertArrayEquals(maxValue, entry.value());
        }
    }

    @Nested
    @DisplayName("deserializeForRecovery()")
    class DeserializeForRecovery {

        @Test
        @DisplayName("returns correct key and disk location without reading value")
        void recoveryEntryHasCorrectFields() {
            ByteBuffer buf = allocateBuffer(KEY, VALUE);
            LogEntry.serialize(buf, KEY, VALUE, LSN, TS, WALConfig.RECORD_TYPE_PUT);
            buf.flip();

            int fileId = 1;
            RecoveryEntry entry = LogEntry.deserializeForRecovery(buf, fileId);

            assertAll(
                () -> assertEquals(WALConfig.RECORD_TYPE_PUT, entry.type()),
                () -> assertEquals(LSN,    entry.lsn()),
                () -> assertEquals(TS,     entry.timestamp()),
                () -> assertArrayEquals(KEY, entry.key()),
                () -> assertEquals(fileId, entry.fileId()),
                () -> assertEquals(VALUE.length, entry.valueSize())
            );
        }

        @Test
        @DisplayName("valueOffset points to the byte immediately after the key")
        void valueOffsetIsCorrect() {
            ByteBuffer buf = allocateBuffer(KEY, VALUE);
            LogEntry.serialize(buf, KEY, VALUE, LSN, TS, WALConfig.RECORD_TYPE_PUT);
            buf.flip();

            RecoveryEntry entry = LogEntry.deserializeForRecovery(buf, 0);

            long expectedOffset = WALConfig.HEADER_SIZE_BYTES + KEY.length;
            assertEquals(expectedOffset, entry.valueOffset());
        }

        @Test
        @DisplayName("buffer position advances past full record including value bytes")
        void bufferPositionAdvancesPastValue() {
            ByteBuffer buf = allocateBuffer(KEY, VALUE);
            LogEntry.serialize(buf, KEY, VALUE, LSN, TS, WALConfig.RECORD_TYPE_PUT);
            buf.flip();

            LogEntry.deserializeForRecovery(buf, 0);

            // Buffer should be fully consumed — no remaining bytes
            assertEquals(0, buf.remaining());
        }

        @Test
        @DisplayName("corrupt record during recovery throws DataCorruptionException")
        void corruptRecordDuringRecoveryIsDetected() {
            ByteBuffer buf = allocateBuffer(KEY, VALUE);
            LogEntry.serialize(buf, KEY, VALUE, LSN, TS, WALConfig.RECORD_TYPE_PUT);
            buf.flip();

            // Corrupt a value byte — even though we skip reading it,
            // CRC must still cover it
            int valueStart = WALConfig.HEADER_SIZE_BYTES + KEY.length;
            buf.put(valueStart, (byte)(buf.get(valueStart) ^ 0x01));

            assertThrows(DataCorruptionException.class,
                () -> LogEntry.deserializeForRecovery(buf, 0));
        }
    }
}
