package io.laminar.core.recovery;

import io.laminar.core.config.WALConfig;
import io.laminar.core.entry.LogEntry;
import io.laminar.core.model.BytesKey;
import io.laminar.core.model.RecoveryEntry;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("WalRecovery")
class WalRecoveryTest {

    @TempDir
    Path tempDir;

    private static final byte[] KEY   = "user:101".getBytes();
    private static final byte[] VALUE = "rushikesh".getBytes();
    private static final long   LSN   = 1L;
    private static final long   TS    = System.currentTimeMillis();

    // -------------------------------------------------------------------------
    // Helpers — write real serialized records to segment files on disk
    // -------------------------------------------------------------------------

    /** Creates a segment file named {@code data-00000000N.log}. */
    private Path segmentPath(int id) {
        return tempDir.resolve(
            String.format(WALConfig.SEGMENT_FILENAME_PREFIX + "%09d" +
                          WALConfig.SEGMENT_FILE_EXTENSION, id));
    }

    /** Writes one serialized record to the given channel at its current position. */
    private void writeRecord(FileChannel ch, byte[] key, byte[] value,
                             long lsn, byte type) throws Exception {
        int size = LogEntry.serializedSize(key.length, value.length);
        ByteBuffer buf = ByteBuffer.allocateDirect(size);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        LogEntry.serialize(buf, key, value, lsn, TS, type);
        buf.flip();
        while (buf.hasRemaining()) ch.write(buf);
    }

    /** Opens a segment file for appending and returns the channel. */
    private FileChannel openForWrite(Path path) throws Exception {
        return FileChannel.open(path,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.READ);
    }

    @Nested
    @DisplayName("recover()")
    class Recover {

        @Test
        @DisplayName("returns empty index for non-existent data directory")
        void emptyIndexForMissingDirectory() throws Exception {
            Path missing = tempDir.resolve("no-such-dir");
            Map<BytesKey, RecoveryEntry> index = WalRecovery.recover(missing);
            assertTrue(index.isEmpty());
        }

        @Test
        @DisplayName("returns empty index for directory with no segment files")
        void emptyIndexForEmptyDirectory() throws Exception {
            Map<BytesKey, RecoveryEntry> index = WalRecovery.recover(tempDir);
            assertTrue(index.isEmpty());
        }

        @Test
        @DisplayName("single PUT record is indexed correctly")
        void singlePutIsIndexed() throws Exception {
            Path seg = segmentPath(1);
            try (FileChannel ch = openForWrite(seg)) {
                writeRecord(ch, KEY, VALUE, LSN, WALConfig.RECORD_TYPE_PUT);
            }

            Map<BytesKey, RecoveryEntry> index = WalRecovery.recover(tempDir);

            BytesKey key = new BytesKey(KEY);
            assertAll(
                () -> assertEquals(1, index.size()),
                () -> assertTrue(index.containsKey(key)),
                () -> assertArrayEquals(KEY,   index.get(key).key()),
                () -> assertEquals(VALUE.length, index.get(key).valueSize())
            );
        }

        @Test
        @DisplayName("PUT followed by DELETE removes the key from the index")
        void putThenDeleteRemovesKey() throws Exception {
            Path seg = segmentPath(1);
            try (FileChannel ch = openForWrite(seg)) {
                writeRecord(ch, KEY, VALUE,    LSN,     WALConfig.RECORD_TYPE_PUT);
                writeRecord(ch, KEY, new byte[0], LSN + 1, WALConfig.RECORD_TYPE_DELETE);
            }

            Map<BytesKey, RecoveryEntry> index = WalRecovery.recover(tempDir);

            assertTrue(index.isEmpty());
        }

        @Test
        @DisplayName("DELETE followed by re-PUT restores the key — last write wins")
        void deleteThenRePutRestoresKey() throws Exception {
            Path seg = segmentPath(1);
            try (FileChannel ch = openForWrite(seg)) {
                writeRecord(ch, KEY, VALUE,    LSN,     WALConfig.RECORD_TYPE_PUT);
                writeRecord(ch, KEY, new byte[0], LSN + 1, WALConfig.RECORD_TYPE_DELETE);
                writeRecord(ch, KEY, VALUE,    LSN + 2, WALConfig.RECORD_TYPE_PUT);
            }

            Map<BytesKey, RecoveryEntry> index = WalRecovery.recover(tempDir);

            assertTrue(index.containsKey(new BytesKey(KEY)));
        }

        @Test
        @DisplayName("DELETE of a key that was never PUT is a no-op")
        void deleteOfAbsentKeyIsNoOp() throws Exception {
            Path seg = segmentPath(1);
            try (FileChannel ch = openForWrite(seg)) {
                writeRecord(ch, KEY, new byte[0], LSN, WALConfig.RECORD_TYPE_DELETE);
            }

            Map<BytesKey, RecoveryEntry> index = WalRecovery.recover(tempDir);

            assertTrue(index.isEmpty());
        }

        @Test
        @DisplayName("multiple keys across two segments are all indexed")
        void multipleKeysAcrossSegments() throws Exception {
            byte[] key1 = "user:1".getBytes();
            byte[] key2 = "user:2".getBytes();
            byte[] key3 = "user:3".getBytes();

            try (FileChannel ch = openForWrite(segmentPath(1))) {
                writeRecord(ch, key1, VALUE, 1L, WALConfig.RECORD_TYPE_PUT);
                writeRecord(ch, key2, VALUE, 2L, WALConfig.RECORD_TYPE_PUT);
            }
            try (FileChannel ch = openForWrite(segmentPath(2))) {
                writeRecord(ch, key3, VALUE, 3L, WALConfig.RECORD_TYPE_PUT);
            }

            Map<BytesKey, RecoveryEntry> index = WalRecovery.recover(tempDir);

            assertAll(
                () -> assertEquals(3, index.size()),
                () -> assertTrue(index.containsKey(new BytesKey(key1))),
                () -> assertTrue(index.containsKey(new BytesKey(key2))),
                () -> assertTrue(index.containsKey(new BytesKey(key3)))
            );
        }

        @Test
        @DisplayName("later segment overwrites earlier entry for same key")
        void laterSegmentOverwritesEarlierEntry() throws Exception {
            byte[] valueV1 = "v1".getBytes();
            byte[] valueV2 = "v2".getBytes();

            try (FileChannel ch = openForWrite(segmentPath(1))) {
                writeRecord(ch, KEY, valueV1, 1L, WALConfig.RECORD_TYPE_PUT);
            }
            try (FileChannel ch = openForWrite(segmentPath(2))) {
                writeRecord(ch, KEY, valueV2, 2L, WALConfig.RECORD_TYPE_PUT);
            }

            Map<BytesKey, RecoveryEntry> index = WalRecovery.recover(tempDir);

            // Entry must point to segment 2 — the later write wins
            assertEquals(2, index.get(new BytesKey(KEY)).fileId());
        }
    }

    @Nested
    @DisplayName("corrupt tail handling")
    class CorruptTail {

        @Test
        @DisplayName("CRC corruption truncates file at corrupt record offset")
        void crcCorruptionTruncatesFile() throws Exception {
            Path seg = segmentPath(1);
            int goodRecordSize = LogEntry.serializedSize(KEY.length, VALUE.length);

            try (FileChannel ch = openForWrite(seg)) {
                writeRecord(ch, KEY, VALUE, LSN, WALConfig.RECORD_TYPE_PUT);

                // Write a second record then corrupt one byte in its value
                ByteBuffer buf = ByteBuffer.allocateDirect(goodRecordSize);
                buf.order(ByteOrder.LITTLE_ENDIAN);
                LogEntry.serialize(buf, KEY, VALUE, LSN + 1, TS, WALConfig.RECORD_TYPE_PUT);
                buf.flip();

                // Flip one bit in the value region of the second record
                int corruptOffset = WALConfig.HEADER_SIZE_BYTES + KEY.length;
                buf.put(corruptOffset, (byte)(buf.get(corruptOffset) ^ 0x01));

                while (buf.hasRemaining()) ch.write(buf);
            }

            WalRecovery.recover(tempDir);

            // File must be truncated to exactly one good record
            assertEquals(goodRecordSize, seg.toFile().length());
        }

        @Test
        @DisplayName("corrupt record is not added to the index")
        void corruptRecordNotIndexed() throws Exception {
            byte[] key2 = "user:102".getBytes();
            Path seg = segmentPath(1);
            int size = LogEntry.serializedSize(KEY.length, VALUE.length);

            try (FileChannel ch = openForWrite(seg)) {
                writeRecord(ch, KEY, VALUE, LSN, WALConfig.RECORD_TYPE_PUT);

                ByteBuffer buf = ByteBuffer.allocateDirect(size);
                buf.order(ByteOrder.LITTLE_ENDIAN);
                LogEntry.serialize(buf, key2, VALUE, LSN + 1, TS, WALConfig.RECORD_TYPE_PUT);
                buf.flip();
                buf.put(WALConfig.HEADER_SIZE_BYTES, (byte)(buf.get(WALConfig.HEADER_SIZE_BYTES) ^ 0x01));
                while (buf.hasRemaining()) ch.write(buf);
            }

            Map<BytesKey, RecoveryEntry> index = WalRecovery.recover(tempDir);

            assertAll(
                () -> assertTrue(index.containsKey(new BytesKey(KEY))),
                () -> assertFalse(index.containsKey(new BytesKey(key2)))
            );
        }

        @Test
        @DisplayName("corruption in non-last segment stops scanning — subsequent segment not indexed")
        void corruptionInEarlySegmentStopsScanning() throws Exception {
            byte[] key2 = "user:102".getBytes();
            int size = LogEntry.serializedSize(KEY.length, VALUE.length);

            // Segment 1 — corrupt second record
            try (FileChannel ch = openForWrite(segmentPath(1))) {
                writeRecord(ch, KEY, VALUE, 1L, WALConfig.RECORD_TYPE_PUT);

                ByteBuffer buf = ByteBuffer.allocateDirect(size);
                buf.order(ByteOrder.LITTLE_ENDIAN);
                LogEntry.serialize(buf, KEY, VALUE, 2L, TS, WALConfig.RECORD_TYPE_PUT);
                buf.flip();
                buf.put(WALConfig.HEADER_SIZE_BYTES, (byte)(buf.get(WALConfig.HEADER_SIZE_BYTES) ^ 0x01));
                while (buf.hasRemaining()) ch.write(buf);
            }

            // Segment 2 — valid record
            try (FileChannel ch = openForWrite(segmentPath(2))) {
                writeRecord(ch, key2, VALUE, 3L, WALConfig.RECORD_TYPE_PUT);
            }

            Map<BytesKey, RecoveryEntry> index = WalRecovery.recover(tempDir);

            // key2 from segment 2 must NOT be indexed — scanning stopped
            assertFalse(index.containsKey(new BytesKey(key2)));
        }

        @Test
        @DisplayName("corrupt KEY_SIZE field truncates at that record offset")
        void corruptKeySizeFieldTruncates() throws Exception {
            Path seg = segmentPath(1);
            int goodSize = LogEntry.serializedSize(KEY.length, VALUE.length);

            try (FileChannel ch = openForWrite(seg)) {
                writeRecord(ch, KEY, VALUE, LSN, WALConfig.RECORD_TYPE_PUT);

                // Write a second record with a corrupted KEY_SIZE field
                ByteBuffer buf = ByteBuffer.allocateDirect(goodSize);
                buf.order(ByteOrder.LITTLE_ENDIAN);
                LogEntry.serialize(buf, KEY, VALUE, LSN + 1, TS, WALConfig.RECORD_TYPE_PUT);
                buf.flip();

                // Overwrite KEY_SIZE with MAX+1 to trigger the bounds guard
                buf.putInt(WALConfig.OFFSET_KEY_SIZE, WALConfig.MAX_KEY_SIZE_BYTES + 1);
                while (buf.hasRemaining()) ch.write(buf);
            }

            WalRecovery.recover(tempDir);

            assertEquals(goodSize, seg.toFile().length());
        }

        @Test
        @DisplayName("partial record at end of file is truncated")
        void partialRecordAtEndOfFileTruncated() throws Exception {
            Path seg = segmentPath(1);
            int goodSize = LogEntry.serializedSize(KEY.length, VALUE.length);

            try (FileChannel ch = openForWrite(seg)) {
                writeRecord(ch, KEY, VALUE, LSN, WALConfig.RECORD_TYPE_PUT);

                // Write only a partial header — simulates crash mid-header-write
                ByteBuffer partial = ByteBuffer.allocate(WALConfig.HEADER_SIZE_BYTES / 2);
                partial.order(ByteOrder.LITTLE_ENDIAN);
                for (int i = 0; i < partial.capacity(); i++) partial.put((byte) i);
                partial.flip();
                ch.write(partial);
            }

            WalRecovery.recover(tempDir);

            assertEquals(goodSize, seg.toFile().length());
        }
    }
}
