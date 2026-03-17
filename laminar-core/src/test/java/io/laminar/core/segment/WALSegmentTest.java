package io.laminar.core.segment;

import io.laminar.core.config.WALConfig;
import io.laminar.core.exception.DiskFullException;
import io.laminar.core.exception.SegmentFullException;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("WALSegment")
class WALSegmentTest {

    @TempDir
    Path tempDir;

    /**
     * Creates a segment path inside the temp directory.
     * Each call produces a unique filename to avoid cross-test interference.
     */
    private Path segmentPath(String name) {
        return tempDir.resolve(name + WALSegment.EXTENSION);
    }

    /**
     * Allocates a direct ByteBuffer pre-filled with a repeating non-zero
     * pattern and flipped to read mode — ready to pass to append().
     *
     * Non-zero fill (i % 127) catches silent zeroing bugs where bytes
     * are never actually written but the buffer reads back as zeros.
     */
    private ByteBuffer filledBuffer(int size) {
        ByteBuffer buf = ByteBuffer.allocateDirect(size);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < size; i++) {
            buf.put((byte)(i % 127));
        }
        buf.flip();
        return buf;
    }

    @Nested
    @DisplayName("construction")
    class Construction {

        @Test
        @DisplayName("creates a new .log file at the given path")
        void createsNewFileAtPath() throws IOException {
            Path path = segmentPath("data-001");

            try (WALSegment segment = new WALSegment(path, 1)) {
                assertTrue(path.toFile().exists());
            }
        }

        @Test
        @DisplayName("new segment starts with zero bytes written")
        void newSegmentHasZeroSize() throws IOException {
            Path path = segmentPath("data-001");

            try (WALSegment segment = new WALSegment(path, 1)) {
                assertEquals(0L, segment.getCurrentSizeBytes());
            }
        }

        @Test
        @DisplayName("reopening existing file does not overwrite existing bytes")
        void reopenDoesNotOverwriteExistingBytes() throws IOException {
            Path path = segmentPath("data-001");
            ByteBuffer buf = filledBuffer(64);

            // First open — write 64 bytes
            try (WALSegment first = new WALSegment(path, 1)) {
                first.append(buf);
                first.flush();
            }

            // Second open — simulates restart into existing segment
            try (WALSegment second = new WALSegment(path, 1)) {
                // Must resume from byte 64, not overwrite from byte 0
                assertEquals(64L, second.getCurrentSizeBytes());
            }

            // File must still contain exactly 64 bytes
            assertEquals(64L, path.toFile().length());
        }

        @Test
        @DisplayName("segmentId is stored and retrievable")
        void segmentIdIsCorrect() throws IOException {
            Path path = segmentPath("data-042");

            try (WALSegment segment = new WALSegment(path, 42)) {
                assertEquals(42, segment.getSegmentId());
            }
        }
    }

    @Nested
    @DisplayName("append()")
    class Append {

        @Test
        @DisplayName("returns zero as writeOffset for first append on empty segment")
        void firstAppendReturnsOffsetZero() throws IOException {
            Path path = segmentPath("data-001");
            ByteBuffer buf = filledBuffer(128);

            try (WALSegment segment = new WALSegment(path, 1)) {
                long offset = segment.append(buf);
                assertEquals(0L, offset);
            }
        }

        @Test
        @DisplayName("second append returns offset equal to first record size")
        void secondAppendReturnsCorrectOffset() throws IOException {
            Path path = segmentPath("data-001");

            try (WALSegment segment = new WALSegment(path, 1)) {
                ByteBuffer first = filledBuffer(128);
                segment.append(first);

                ByteBuffer second = filledBuffer(64);
                long offset = segment.append(second);

                // Second record starts immediately after the first
                assertEquals(128L, offset);
            }
        }

        @Test
        @DisplayName("currentSizeBytes advances by exactly the bytes written")
        void currentSizeBytesAdvancesCorrectly() throws IOException {
            Path path = segmentPath("data-001");
            ByteBuffer buf = filledBuffer(256);

            try (WALSegment segment = new WALSegment(path, 1)) {
                segment.append(buf);
                assertEquals(256L, segment.getCurrentSizeBytes());
            }
        }

        @Test
        @DisplayName("bytes written to disk match bytes in buffer")
        void bytesOnDiskMatchBuffer() throws IOException {
            Path path = segmentPath("data-001");
            ByteBuffer written = filledBuffer(64);

            try (WALSegment segment = new WALSegment(path, 1)) {
                segment.append(written);
                segment.flush();
            }

            // Read back raw bytes from disk and compare
            ByteBuffer readBack = ByteBuffer.allocate(64);
            try (FileChannel reader = FileChannel.open(
                    path,
                    StandardOpenOption.READ)) {
                reader.read(readBack, 0);
            }

            readBack.flip();
            written.rewind();

            while (written.hasRemaining()) {
                assertEquals(written.get(), readBack.get());
            }
        }

        @Test
        @DisplayName("zero-length buffer appends without error and returns correct offset")
        void zeroLengthBufferAppends() throws IOException {
            Path path = segmentPath("data-001");
            ByteBuffer empty = ByteBuffer.allocateDirect(0);
            empty.flip();

            try (WALSegment segment = new WALSegment(path, 1)) {
                long offset = segment.append(empty);
                assertEquals(0L, offset);
                assertEquals(0L, segment.getCurrentSizeBytes());
            }
        }

        @Test
        @DisplayName("throws SegmentFullException when segment is at capacity")
        void throwsSegmentFullExceptionWhenFull() 
                throws IOException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
            Path path = segmentPath("data-001");

            try (WALSegment segment = new WALSegment(path, 1)) {
                // Manually set size to max via reflection to avoid
                // writing 64MB of actual data in a unit test
                java.lang.reflect.Field field =
                        WALSegment.class.getDeclaredField("currentSizeBytes");
                field.setAccessible(true);
                field.set(segment, WALConfig.MAX_SEGMENT_SIZE_BYTES);

                ByteBuffer buf = filledBuffer(64);
                assertThrows(SegmentFullException.class,
                    () -> segment.append(buf));
            }
        }

        @Test
        @DisplayName("throws IllegalStateException when appending to closed segment")
        void throwsIllegalStateExceptionWhenClosed() throws IOException {
            Path path = segmentPath("data-001");
            WALSegment segment = new WALSegment(path, 1);
            segment.close();

            ByteBuffer buf = filledBuffer(64);
            assertThrows(IllegalStateException.class,
                () -> segment.append(buf));
        }
    }

    @Nested
    @DisplayName("isFull()")
    class IsFull {

        @Test
        @DisplayName("returns false when segment is below capacity")
        void returnsFalseWhenBelowCapacity() throws IOException {
            Path path = segmentPath("data-001");

            try (WALSegment segment = new WALSegment(path, 1)) {
                assertFalse(segment.isFull());
            }
        }

        @Test
        @DisplayName("returns true when segment reaches exactly MAX_SEGMENT_SIZE_BYTES")
        void returnsTrueAtExactCapacity()
                throws IOException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
            Path path = segmentPath("data-001");

            try (WALSegment segment = new WALSegment(path, 1)) {
                java.lang.reflect.Field field =
                        WALSegment.class.getDeclaredField("currentSizeBytes");
                field.setAccessible(true);
                field.set(segment, WALConfig.MAX_SEGMENT_SIZE_BYTES);

                assertTrue(segment.isFull());
            }
        }
    }

    @Nested
    @DisplayName("close()")
    class Close {

        @Test
        @DisplayName("sets isClosed() to true")
        void setsIsClosedTrue() throws IOException {
            Path path = segmentPath("data-001");
            WALSegment segment = new WALSegment(path, 1);

            assertFalse(segment.isClosed());
            segment.close();
            assertTrue(segment.isClosed());
        }

        @Test
        @DisplayName("is idempotent — second call does not throw")
        void closeIsIdempotent() throws IOException {
            Path path = segmentPath("data-001");
            WALSegment segment = new WALSegment(path, 1);

            segment.close();

            // Second close must be silent — no exception
            assertDoesNotThrow(segment::close);
        }

        @Test
        @DisplayName("flushes pending bytes before closing")
        void flushesBytesBeforeClose() throws IOException {
            Path path = segmentPath("data-001");
            ByteBuffer buf = filledBuffer(128);

            try (WALSegment segment = new WALSegment(path, 1)) {
                segment.append(buf);
                // close() internally calls flush() — bytes must reach disk
            }

            // File must contain exactly 128 bytes after close
            assertEquals(128L, path.toFile().length());
        }

        @Test
        @DisplayName("flush() on closed segment is a no-op")
        void flushOnClosedSegmentIsNoOp() throws IOException {
            Path path = segmentPath("data-001");
            WALSegment segment = new WALSegment(path, 1);
            segment.close();

            // Must not throw — flush() checks closed flag before calling force()
            assertDoesNotThrow(segment::flush);
        }
    }
}
