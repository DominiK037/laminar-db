package io.laminar.core.segment;

import io.laminar.core.config.WALConfig;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("SegmentManager")
class SegmentManagerTest {

    @TempDir
    Path tempDir;

    // -------------------------------------------------------------------------
    // Reflection helpers
    // -------------------------------------------------------------------------

    private void forceActiveSegmentFull(SegmentManager manager)
            throws Exception {
        Field activeSegmentField = SegmentManager.class
                .getDeclaredField("activeSegment");
        activeSegmentField.setAccessible(true);
        WALSegment activeSegment = (WALSegment) activeSegmentField
                .get(manager);

        Field currentSizeBytesField = WALSegment.class
                .getDeclaredField("currentSizeBytes");
        currentSizeBytesField.setAccessible(true);
        currentSizeBytesField.set(activeSegment,
                WALConfig.MAX_SEGMENT_SIZE_BYTES);
    }

    private WALSegment getActiveSegment(SegmentManager manager)
            throws Exception {
        Field field = SegmentManager.class
                .getDeclaredField("activeSegment");
        field.setAccessible(true);
        return (WALSegment) field.get(manager);
    }

    private List<Path> listSegmentFiles() throws IOException {
        try (var stream = Files.list(tempDir)) {
            return stream
                .filter(p -> p.toString().endsWith(WALSegment.EXTENSION))
                .sorted()
                .collect(Collectors.toList());
        }
    }

    @Nested
    @DisplayName("construction")
    class Construction {

        @Test
        @DisplayName("creates data-000000001.log on empty directory")
        void createsFirstSegmentOnEmptyDirectory() throws Exception {
            try (SegmentManager manager = new SegmentManager(tempDir)) {
                List<Path> files = listSegmentFiles();
                assertEquals(1, files.size());
                assertEquals("data-000000001.log",
                    files.get(0).getFileName().toString());
            }
        }

        @Test
        @DisplayName("first segment has ID 1")
        void firstSegmentHasIdOne() throws Exception {
            try (SegmentManager manager = new SegmentManager(tempDir)) {
                WALSegment active = getActiveSegment(manager);
                assertEquals(1, active.getSegmentId());
            }
        }

        @Test
        @DisplayName("creates data directory if it does not exist")
        void createsDataDirectoryIfMissing() throws Exception {
            Path nested = tempDir.resolve("data/wal");
            try (SegmentManager manager = new SegmentManager(nested)) {
                assertTrue(Files.exists(nested));
            }
        }
    }

    @Nested
    @DisplayName("restart recovery")
    class RestartRecovery {

        @Test
        @DisplayName("reopens existing segment on restart without overwriting")
        void reopensExistingSegmentOnRestart() throws Exception {
            // First open — creates segment, close it
            try (SegmentManager first = new SegmentManager(tempDir)) {
                // segment exists now
            }

            // Second open — simulates restart
            try (SegmentManager second = new SegmentManager(tempDir)) {
                List<Path> files = listSegmentFiles();
                // Must not create a second segment — resumes existing one
                assertEquals(1, files.size());
            }
        }

        @Test
        @DisplayName("rolls to new segment on restart if last segment is full")
        void rollsToNewSegmentIfLastIsFull() throws Exception {
            // Pre-create a segment file that is exactly at capacity on disk
            // Using a FileChannel to set the file size without writing real data
            Path fullSegment = tempDir.resolve("data-000000001.log");
            try (FileChannel ch = FileChannel.open(
                    fullSegment,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE)) {
                // position to MAX size and write one byte — creates a sparse
                // file of MAX_SEGMENT_SIZE_BYTES on disk
                ch.position(WALConfig.MAX_SEGMENT_SIZE_BYTES - 1);
                ch.write(java.nio.ByteBuffer.wrap(new byte[]{1}));
            }

            // SegmentManager opens it, channel.size() returns MAX, isFull() = true
            // rotation must happen immediately
            try (SegmentManager manager = new SegmentManager(tempDir)) {
                List<Path> files = listSegmentFiles();
                assertEquals(2, files.size());
                assertEquals("data-000000002.log",
                    files.get(1).getFileName().toString());
            }
        }

        @Test
        @DisplayName("active segment ID is correct after restart into existing segment")
        void segmentIdCorrectAfterRestart() throws Exception {
            try (SegmentManager first = new SegmentManager(tempDir)) {
                // segment 1 created
            }

            try (SegmentManager second = new SegmentManager(tempDir)) {
                WALSegment active = getActiveSegment(second);
                assertEquals(1, active.getSegmentId());
            }
        }
    }

    @Nested
    @DisplayName("rotateIfFull()")
    class RotateIfFull {

        @Test
        @DisplayName("no-op when active segment is below capacity")
        void noOpWhenBelowCapacity() throws Exception {
            try (SegmentManager manager = new SegmentManager(tempDir)) {
                WALSegment before = getActiveSegment(manager);
                manager.rotateIfFull();
                WALSegment after = getActiveSegment(manager);

                // Same instance — no rotation occurred
                assertSame(before, after);
            }
        }

        @Test
        @DisplayName("creates new segment when active is full")
        void createsNewSegmentWhenFull() throws Exception {
            try (SegmentManager manager = new SegmentManager(tempDir)) {
                forceActiveSegmentFull(manager);
                manager.rotateIfFull();

                List<Path> files = listSegmentFiles();
                assertEquals(2, files.size());
            }
        }

        @Test
        @DisplayName("new segment after rotation has incremented ID")
        void newSegmentHasIncrementedId() throws Exception {
            try (SegmentManager manager = new SegmentManager(tempDir)) {
                forceActiveSegmentFull(manager);
                manager.rotateIfFull();

                WALSegment active = getActiveSegment(manager);
                assertEquals(2, active.getSegmentId());
            }
        }

        @Test
        @DisplayName("new segment filename follows zero-padded format")
        void newSegmentFilenameFollowsFormat() throws Exception {
            try (SegmentManager manager = new SegmentManager(tempDir)) {
                forceActiveSegmentFull(manager);
                manager.rotateIfFull();

                List<Path> files = listSegmentFiles();
                assertEquals(
                    "data-000000002.log",
                    files.get(1).getFileName().toString()
                );
            }
        }

        @Test
        @DisplayName("old segment is closed after rotation")
        void oldSegmentIsClosedAfterRotation() throws Exception {
            try (SegmentManager manager = new SegmentManager(tempDir)) {
                WALSegment old = getActiveSegment(manager);
                forceActiveSegmentFull(manager);
                manager.rotateIfFull();

                assertTrue(old.isClosed());
            }
        }

        @Test
        @DisplayName("multiple rotations produce monotonically increasing IDs")
        void multipleRotationsProduceMonotonicIds() throws Exception {
            try (SegmentManager manager = new SegmentManager(tempDir)) {
                for (int expectedId = 2; expectedId <= 5; expectedId++) {
                    forceActiveSegmentFull(manager);
                    manager.rotateIfFull();
                    assertEquals(
                        expectedId,
                        getActiveSegment(manager).getSegmentId()
                    );
                }

                assertEquals(5, listSegmentFiles().size());
            }
        }
    }

    @Nested
    @DisplayName("close()")
    class Close {

        @Test
        @DisplayName("closes the active segment on shutdown")
        void closesActiveSegment() throws Exception {
            SegmentManager manager = new SegmentManager(tempDir);
            WALSegment active = getActiveSegment(manager);

            manager.close();

            assertTrue(active.isClosed());
        }

        @Test
        @DisplayName("is idempotent — second call does not throw")
        void closeIsIdempotent() throws Exception {
            SegmentManager manager = new SegmentManager(tempDir);
            manager.close();

            assertDoesNotThrow(manager::close);
        }
    }

    @Nested
    @DisplayName("filename parsing")
    class FilenameParsing {

        @Test
        @DisplayName("throws IllegalArgumentException on malformed filename")
        void throwsOnMalformedFilename() throws Exception {
            // Create a .log file with a non-standard name in the directory
            Files.createFile(tempDir.resolve("corrupt-name.log"));

            assertThrows(IllegalArgumentException.class,
                () -> new SegmentManager(tempDir));
        }
    }
}
