package io.laminar.core.append;

import io.laminar.core.config.WALConfig;
import io.laminar.core.entry.LogEntry;
import io.laminar.core.model.DeserializedEntry;
import io.laminar.core.segment.SegmentManager;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("LogAppender")
class LogAppenderTest {

    @TempDir
    Path tempDir;

    private static final byte[] KEY   = "user:101".getBytes();
    private static final byte[] VALUE = "{'name':'rushikesh'}".getBytes();

    // -------------------------------------------------------------------------
    // Helper — reads all records from the first segment file on disk
    // -------------------------------------------------------------------------

    private List<DeserializedEntry> readRecordsFromDisk(Path dir) throws Exception {
        Path segmentFile = dir.resolve("data-000000001.log");
        List<DeserializedEntry> records = new ArrayList<>();

        try (FileChannel ch = FileChannel.open(segmentFile, StandardOpenOption.READ)) {
            long fileSize = ch.size();
            if (fileSize == 0) return records;

            ByteBuffer buf = ByteBuffer.allocate((int) fileSize);
            buf.order(ByteOrder.LITTLE_ENDIAN);
            ch.read(buf, 0);
            buf.flip();

            while (buf.hasRemaining()) {
                records.add(LogEntry.deserialize(buf));
            }
        }

        return records;
    }

    @Nested
    @DisplayName("construction()")
    class Construction {

        @Test
        @DisplayName("appender thread is alive after construction")
        void appenderThreadIsAliveAfterConstruction() throws Exception {
            try (SegmentManager sm = new SegmentManager(tempDir);
                 LogAppender appender = new LogAppender(sm)) {

                // Give the thread a moment to start
                Thread.sleep(10);

                boolean found = Thread.getAllStackTraces().keySet().stream()
                        .anyMatch(t -> "log-appender".equals(t.getName()) && t.isAlive());

                assertTrue(found);
            }
        }
    }

    @Nested
    @DisplayName("submit()")
    class Submit {

        @Test
        @DisplayName("PUT task future completes after successful append")
        void putTaskFutureCompletesAfterAppend() throws Exception {
            try (SegmentManager sm = new SegmentManager(tempDir);
                 LogAppender appender = new LogAppender(sm)) {

                LogTask task = new LogTask(KEY, VALUE, WALConfig.RECORD_TYPE_PUT);
                appender.submit(task);

                // Blocks until LogAppender calls task.complete()
                assertNull(task.getFuture().get());
            }
        }

        @Test
        @DisplayName("DELETE task future completes after successful append")
        void deleteTaskFutureCompletesAfterAppend() throws Exception {
            try (SegmentManager sm = new SegmentManager(tempDir);
                 LogAppender appender = new LogAppender(sm)) {

                LogTask task = new LogTask(KEY, new byte[0], WALConfig.RECORD_TYPE_DELETE);
                appender.submit(task);

                assertNull(task.getFuture().get());
            }
        }

        @Test
        @DisplayName("bytes written to disk match serialized size")
        void bytesOnDiskMatchSerializedSize() throws Exception {
            try (SegmentManager sm = new SegmentManager(tempDir);
                 LogAppender appender = new LogAppender(sm)) {

                LogTask task = new LogTask(KEY, VALUE, WALConfig.RECORD_TYPE_PUT);
                appender.submit(task);
                task.getFuture().get();
            }
            // Appender is closed — final flush completed

            int expectedSize = LogEntry.serializedSize(KEY.length, VALUE.length);
            long actualSize  = tempDir.resolve("data-000000001.log").toFile().length();

            assertEquals(expectedSize, actualSize);
        }

        @Test
        @DisplayName("LSN increments monotonically across submitted tasks")
        void lsnIncrementsMonotonically() throws Exception {
            int taskCount = 3;

            try (SegmentManager sm = new SegmentManager(tempDir);
                 LogAppender appender = new LogAppender(sm)) {

                List<CompletableFuture<Void>> futures = new ArrayList<>();
                for (int i = 0; i < taskCount; i++) {
                    LogTask task = new LogTask(KEY, VALUE, WALConfig.RECORD_TYPE_PUT);
                    appender.submit(task);
                    futures.add(task.getFuture());
                }

                // Wait for all writes to complete
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
            }
            // Close triggers final flush — safe to read from disk now

            List<DeserializedEntry> records = readRecordsFromDisk(tempDir);

            assertEquals(taskCount, records.size());
            for (int i = 0; i < taskCount; i++) {
                assertEquals(i + 1L, records.get(i).lsn());
            }
        }

        @Test
        @DisplayName("record type is preserved on disk for PUT and DELETE")
        void recordTypePreservedOnDisk() throws Exception {
            try (SegmentManager sm = new SegmentManager(tempDir);
                 LogAppender appender = new LogAppender(sm)) {

                LogTask put    = new LogTask(KEY, VALUE,    WALConfig.RECORD_TYPE_PUT);
                LogTask delete = new LogTask(KEY, new byte[0], WALConfig.RECORD_TYPE_DELETE);
                appender.submit(put);
                appender.submit(delete);

                CompletableFuture.allOf(put.getFuture(), delete.getFuture()).get();
            }

            List<DeserializedEntry> records = readRecordsFromDisk(tempDir);

            assertAll(
                () -> assertEquals(WALConfig.RECORD_TYPE_PUT,    records.get(0).type()),
                () -> assertEquals(WALConfig.RECORD_TYPE_DELETE, records.get(1).type())
            );
        }
    }

    @Nested
    @DisplayName("close()")
    class Close {

        @Test
        @DisplayName("appender thread is not alive after close()")
        void appenderThreadNotAliveAfterClose() throws Exception {
            SegmentManager sm = new SegmentManager(tempDir);
            LogAppender appender = new LogAppender(sm);

            appender.close();
            sm.close();

            boolean found = Thread.getAllStackTraces().keySet().stream()
                    .anyMatch(t -> "log-appender".equals(t.getName()) && t.isAlive());

            assertFalse(found);
        }

        @Test
        @DisplayName("is idempotent — second close() does not throw")
        void closeIsIdempotent() throws Exception {
            SegmentManager sm = new SegmentManager(tempDir);
            LogAppender appender = new LogAppender(sm);
            appender.close();

            assertDoesNotThrow(appender::close);
            sm.close();
        }

        @Test
        @DisplayName("tasks submitted before close() are not abandoned")
        void shutdownDrainsRemainingTasks() throws Exception {
            int taskCount = 20;
            List<LogTask> tasks = new ArrayList<>();

            try (SegmentManager sm = new SegmentManager(tempDir);
                 LogAppender appender = new LogAppender(sm)) {

                for (int i = 0; i < taskCount; i++) {
                    LogTask task = new LogTask(KEY, VALUE, WALConfig.RECORD_TYPE_PUT);
                    tasks.add(task);
                    appender.submit(task);
                }
                // close() blocks until the drain loop exits — all tasks must be done
            }

            long completedCount = tasks.stream()
                    .filter(t -> t.getFuture().isDone() && !t.getFuture().isCompletedExceptionally())
                    .count();

            assertEquals(taskCount, completedCount);
        }
    }
}
