package io.laminar.core.append;

import io.laminar.core.config.WALConfig;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("LogTask")
class LogTaskTest {

    private static final byte[] KEY   = "user:101".getBytes();
    private static final byte[] VALUE = "{'name':'rushikesh'}".getBytes();

    @Nested
    @DisplayName("construction()")
    class Construction {

        @Test
        @DisplayName("stores key, value, and recordType exactly as supplied")
        void storesFieldsExactlyAsSupplied() {
            LogTask task = new LogTask(KEY, VALUE, WALConfig.RECORD_TYPE_PUT);

            assertAll(
                () -> assertArrayEquals(KEY,                     task.getKey()),
                () -> assertArrayEquals(VALUE,                   task.getValue()),
                () -> assertEquals(WALConfig.RECORD_TYPE_PUT,    task.getRecordType())
            );
        }

        @Test
        @DisplayName("future is not done immediately after construction")
        void futureIsNotDoneOnConstruction() {
            LogTask task = new LogTask(KEY, VALUE, WALConfig.RECORD_TYPE_PUT);

            assertFalse(task.getFuture().isDone());
        }

        @Test
        @DisplayName("DELETE task stores RECORD_TYPE_DELETE")
        void deleteTaskStoresCorrectRecordType() {
            LogTask task = new LogTask(KEY, new byte[0], WALConfig.RECORD_TYPE_DELETE);

            assertEquals(WALConfig.RECORD_TYPE_DELETE, task.getRecordType());
        }

        @Test
        @DisplayName("zero-length value with PUT type is a valid task — type is not inferred from payload")
        void zeroLengthValueWithPutTypeIsValid() {
            LogTask task = new LogTask(KEY, new byte[0], WALConfig.RECORD_TYPE_PUT);

            assertAll(
                () -> assertEquals(WALConfig.RECORD_TYPE_PUT, task.getRecordType()),
                () -> assertEquals(0, task.getValue().length)
            );
        }
    }

    @Nested
    @DisplayName("complete()")
    class Complete {

        @Test
        @DisplayName("future is done after complete()")
        void futureIsDoneAfterComplete() {
            LogTask task = new LogTask(KEY, VALUE, WALConfig.RECORD_TYPE_PUT);
            task.complete();

            assertTrue(task.getFuture().isDone());
        }

        @Test
        @DisplayName("future.get() returns null after complete()")
        void futureGetReturnsNullAfterComplete() throws Exception {
            LogTask task = new LogTask(KEY, VALUE, WALConfig.RECORD_TYPE_PUT);
            task.complete();

            assertNull(task.getFuture().get());
        }

        @Test
        @DisplayName("complete() is idempotent — second call does not throw")
        void completeIsIdempotent() {
            LogTask task = new LogTask(KEY, VALUE, WALConfig.RECORD_TYPE_PUT);
            task.complete();

            assertDoesNotThrow(task::complete);
        }
    }

    @Nested
    @DisplayName("completeExceptionally()")
    class CompleteExceptionally {

        @Test
        @DisplayName("future is done after completeExceptionally()")
        void futureIsDoneAfterCompleteExceptionally() {
            LogTask task = new LogTask(KEY, VALUE, WALConfig.RECORD_TYPE_PUT);
            task.completeExceptionally(new RuntimeException("disk full"));

            assertTrue(task.getFuture().isDone());
        }

        @Test
        @DisplayName("future.get() throws ExecutionException wrapping the original cause")
        void futureGetThrowsExecutionExceptionWithCause() {
            LogTask task   = new LogTask(KEY, VALUE, WALConfig.RECORD_TYPE_PUT);
            Exception cause = new RuntimeException("disk full");
            task.completeExceptionally(cause);

            ExecutionException ex = assertThrows(
                ExecutionException.class,
                () -> task.getFuture().get()
            );
            assertSame(cause, ex.getCause());
        }

        @Test
        @DisplayName("future reports isCompletedExceptionally() after completeExceptionally()")
        void futureIsCompletedExceptionally() {
            LogTask task = new LogTask(KEY, VALUE, WALConfig.RECORD_TYPE_PUT);
            task.completeExceptionally(new RuntimeException("disk full"));

            assertTrue(task.getFuture().isCompletedExceptionally());
        }
    }

    @Nested
    @DisplayName("getFuture()")
    class GetFuture {

        @Test
        @DisplayName("returns same CompletableFuture instance on every call")
        void returnsSameFutureInstance() {
            LogTask task = new LogTask(KEY, VALUE, WALConfig.RECORD_TYPE_PUT);

            CompletableFuture<Void> first  = task.getFuture();
            CompletableFuture<Void> second = task.getFuture();

            assertSame(first, second);
        }
    }
}
