package io.laminar.core;

import io.laminar.core.config.WALConfig;
import io.laminar.core.model.BytesKey;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("WriteAheadLog")
class WriteAheadLogTest {

    @TempDir
    Path tempDir;

    private static final byte[] KEY   = "user:101".getBytes();
    private static final byte[] VALUE = "rushikesh".getBytes();

    @Nested
    @DisplayName("put()")
    class Put {

        @Test
        @DisplayName("put() completes without error for valid key and value")
        void putSucceedsForValidKeyAndValue() throws Exception {
            try (WriteAheadLog wal = new WriteAheadLog(tempDir)) {
                assertDoesNotThrow(() -> wal.put(KEY, VALUE));
            }
        }

        @Test
        @DisplayName("put() with empty value succeeds — zero-length value is a valid PUT")
        void putWithEmptyValueSucceeds() throws Exception {
            try (WriteAheadLog wal = new WriteAheadLog(tempDir)) {
                assertDoesNotThrow(() -> wal.put(KEY, new byte[0]));
            }
        }

        @Test
        @DisplayName("throws IllegalArgumentException for null key")
        void throwsForNullKey() throws Exception {
            try (WriteAheadLog wal = new WriteAheadLog(tempDir)) {
                assertThrows(IllegalArgumentException.class,
                    () -> wal.put(null, VALUE));
            }
        }

        @Test
        @DisplayName("throws IllegalArgumentException for empty key")
        void throwsForEmptyKey() throws Exception {
            try (WriteAheadLog wal = new WriteAheadLog(tempDir)) {
                assertThrows(IllegalArgumentException.class,
                    () -> wal.put(new byte[0], VALUE));
            }
        }

        @Test
        @DisplayName("throws IllegalArgumentException for null value")
        void throwsForNullValue() throws Exception {
            try (WriteAheadLog wal = new WriteAheadLog(tempDir)) {
                assertThrows(IllegalArgumentException.class,
                    () -> wal.put(KEY, null));
            }
        }

        @Test
        @DisplayName("throws IllegalArgumentException when key exceeds MAX_KEY_SIZE_BYTES")
        void throwsForOversizedKey() throws Exception {
            try (WriteAheadLog wal = new WriteAheadLog(tempDir)) {
                byte[] oversized = new byte[WALConfig.MAX_KEY_SIZE_BYTES + 1];
                assertThrows(IllegalArgumentException.class,
                    () -> wal.put(oversized, VALUE));
            }
        }

        @Test
        @DisplayName("throws IllegalArgumentException when value exceeds MAX_VALUE_SIZE_BYTES")
        void throwsForOversizedValue() throws Exception {
            try (WriteAheadLog wal = new WriteAheadLog(tempDir)) {
                byte[] oversized = new byte[WALConfig.MAX_VALUE_SIZE_BYTES + 1];
                assertThrows(IllegalArgumentException.class,
                    () -> wal.put(KEY, oversized));
            }
        }

        @Test
        @DisplayName("accepts key of exactly MAX_KEY_SIZE_BYTES")
        void acceptsKeyAtExactMaxSize() throws Exception {
            try (WriteAheadLog wal = new WriteAheadLog(tempDir)) {
                byte[] maxKey = new byte[WALConfig.MAX_KEY_SIZE_BYTES];
                assertDoesNotThrow(() -> wal.put(maxKey, VALUE));
            }
        }

        @Test
        @DisplayName("accepts value of exactly MAX_VALUE_SIZE_BYTES")
        void acceptsValueAtExactMaxSize() throws Exception {
            try (WriteAheadLog wal = new WriteAheadLog(tempDir)) {
                byte[] maxValue = new byte[WALConfig.MAX_VALUE_SIZE_BYTES];
                assertDoesNotThrow(() -> wal.put(KEY, maxValue));
            }
        }
    }

    @Nested
    @DisplayName("delete()")
    class Delete {

        @Test
        @DisplayName("delete() completes without error for valid key")
        void deleteSucceedsForValidKey() throws Exception {
            try (WriteAheadLog wal = new WriteAheadLog(tempDir)) {
                assertDoesNotThrow(() -> wal.delete(KEY));
            }
        }

        @Test
        @DisplayName("throws IllegalArgumentException for null key")
        void throwsForNullKey() throws Exception {
            try (WriteAheadLog wal = new WriteAheadLog(tempDir)) {
                assertThrows(IllegalArgumentException.class,
                    () -> wal.delete(null));
            }
        }

        @Test
        @DisplayName("throws IllegalArgumentException for empty key")
        void throwsForEmptyKey() throws Exception {
            try (WriteAheadLog wal = new WriteAheadLog(tempDir)) {
                assertThrows(IllegalArgumentException.class,
                    () -> wal.delete(new byte[0]));
            }
        }

        @Test
        @DisplayName("throws IllegalArgumentException when key exceeds MAX_KEY_SIZE_BYTES")
        void throwsForOversizedKey() throws Exception {
            try (WriteAheadLog wal = new WriteAheadLog(tempDir)) {
                byte[] oversized = new byte[WALConfig.MAX_KEY_SIZE_BYTES + 1];
                assertThrows(IllegalArgumentException.class,
                    () -> wal.delete(oversized));
            }
        }
    }

    @Nested
    @DisplayName("recovery integration")
    class RecoveryIntegration {

        @Test
        @DisplayName("index contains key after put() and restart")
        void indexContainsKeyAfterRestart() throws Exception {
            try (WriteAheadLog wal = new WriteAheadLog(tempDir)) {
                wal.put(KEY, VALUE);
            }

            try (WriteAheadLog wal = new WriteAheadLog(tempDir)) {
                assertTrue(wal.getIndex().containsKey(new BytesKey(KEY)));
            }
        }

        @Test
        @DisplayName("index does not contain key after delete() and restart")
        void indexDoesNotContainKeyAfterDeleteAndRestart() throws Exception {
            try (WriteAheadLog wal = new WriteAheadLog(tempDir)) {
                wal.put(KEY, VALUE);
                wal.delete(KEY);
            }

            try (WriteAheadLog wal = new WriteAheadLog(tempDir)) {
                assertFalse(wal.getIndex().containsKey(new BytesKey(KEY)));
            }
        }

        @Test
        @DisplayName("index is empty on first open of a new directory")
        void indexIsEmptyOnFirstOpen() throws Exception {
            try (WriteAheadLog wal = new WriteAheadLog(tempDir)) {
                assertTrue(wal.getIndex().isEmpty());
            }
        }
    }

    @Nested
    @DisplayName("close()")
    class Close {

        @Test
        @DisplayName("is idempotent — second close() does not throw")
        void closeIsIdempotent() throws Exception {
            WriteAheadLog wal = new WriteAheadLog(tempDir);
            wal.close();
            assertDoesNotThrow(wal::close);
        }
    }
}
