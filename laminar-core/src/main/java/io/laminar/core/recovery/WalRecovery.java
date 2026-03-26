package io.laminar.core.recovery;

import io.laminar.core.config.WALConfig;
import io.laminar.core.entry.LogEntry;
import io.laminar.core.exception.DataCorruptionException;
import io.laminar.core.model.BytesKey;
import io.laminar.core.model.RecoveryEntry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Startup WAL replay — scans all segment files, rebuilds the key index,
 * and truncates any corrupt tail left by a crash.
 *
 * <p>Run once before {@code SegmentManager} opens, so the active segment
 * is clean before new writes begin.
 *
 * <h3>Recovery algorithm</h3>
 * <ol>
 *   <li>List all {@code .log} files in the data directory, sorted ascending
 *       by filename (alphabetical = numerical order due to zero-padded names).</li>
 *   <li>For each file, read records sequentially using
 *       {@link LogEntry#deserializeForRecovery}.</li>
 *   <li>Apply each record to the index: PUT updates, DELETE removes.</li>
 *   <li>On {@link DataCorruptionException}: truncate the file at the corrupt
 *       offset and stop scanning. Subsequent segments are not scanned.</li>
 * </ol>
 *
 * <h3>Why truncate instead of skip</h3>
 * <p>Skipping a corrupt record requires knowing its exact byte length, which
 * requires reading {@code KEY_SIZE} and {@code VAL_SIZE} from the header.
 * If the header itself is corrupt, those fields are garbage — there is no
 * reliable way to find the next record boundary. Truncating at the corrupt
 * offset removes the unreadable tail, leaving a clean append boundary for
 * {@code LogAppender}.
 *
 * <h3>Why stop after the first corrupt segment</h3>
 * <p>Corruption can only appear in the active segment at crash time. If a
 * non-last segment has corruption, something beyond a normal crash occurred
 * (hardware failure, partial overwrite). Scanning subsequent segments after
 * truncation would index records written after a logically broken boundary,
 * which cannot be trusted.
 *
 * <h3>Last-write-wins on DELETE</h3>
 * <p>Records are processed in LSN order (segment files are sorted by sequence
 * number). For each key, the last record seen wins: PUT inserts or updates,
 * DELETE removes. A DELETE followed by a re-PUT correctly re-adds the key.
 * The returned index contains only live keys — no tombstones.
 *
 * <p><b>Thread safety:</b> stateless. {@link #recover} may be called from any
 * thread but must not be called concurrently with {@code LogAppender} writes
 * to the same data directory.
 */
public final class WalRecovery {

    /**
     * Maximum size of a single serialized record.
     * Used to size the reuse buffer — no per-record allocation needed.
     */
    private static final int MAX_RECORD_SIZE =
            WALConfig.HEADER_SIZE_BYTES  +
            WALConfig.MAX_KEY_SIZE_BYTES +
            WALConfig.MAX_VALUE_SIZE_BYTES;

    private WalRecovery() {}

    /**
     * Scans all WAL segment files in {@code dataDirectory} and rebuilds the
     * key index.
     *
     * <p>If the data directory does not exist or contains no segment files,
     * an empty index is returned — this is the normal state on first startup.
     *
     * @param dataDirectory directory containing the {@code .log} segment files
     * @return live key index — keys with the most recent DELETE are absent;
     *         values are {@link RecoveryEntry} instances pointing to disk locations
     * @throws IOException if a segment file cannot be opened or truncated
     */
    public static Map<BytesKey, RecoveryEntry> recover(Path dataDirectory) throws IOException {
        Map<BytesKey, RecoveryEntry> index = new HashMap<>();

        List<Path> segments = findSegments(dataDirectory);

        for (Path segmentPath : segments) {
            int     segmentId  = parseSegmentId(segmentPath);
            boolean isCorrupted  = recoverSegment(segmentPath, segmentId, index);
            if (isCorrupted) {
                // Stop — do not index records from subsequent segments.
                break;
            }
        }

        return index;
    }

    /**
     * Reads all records from one segment file and applies them to {@code index}.
     *
     * <p>Two reuse buffers are allocated once per segment:
     * <ul>
     *   <li>{@code headerBuf} — fixed 32 bytes, used to peek {@code KEY_SIZE}
     *       and {@code VAL_SIZE} before reading the full record.</li>
     *   <li>{@code recordBuf} — sized to {@value #MAX_RECORD_SIZE} bytes,
     *       limit adjusted per record to avoid allocating per call.</li>
     * </ul>
     *
     * <p>The file is opened with both {@code READ} and {@code WRITE} so
     * {@link FileChannel#truncate(long)} is available without reopening.
     *
     * @return {@code true} if corruption was found and the file was truncated;
     *         {@code false} if the segment was fully and cleanly scanned
     */
    private static boolean recoverSegment(
            Path path,
            int segmentId,
            Map<BytesKey, RecoveryEntry> index) throws IOException {

        try (FileChannel channel = FileChannel.open(
                path,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE)) {

            long     fileSize  = channel.size();
            long     position  = 0L;
            ByteBuffer headerBuf = ByteBuffer.allocate(WALConfig.HEADER_SIZE_BYTES);
            headerBuf.order(ByteOrder.LITTLE_ENDIAN);
            ByteBuffer recordBuf = ByteBuffer.allocate(MAX_RECORD_SIZE);
            recordBuf.order(ByteOrder.LITTLE_ENDIAN);

            while (position < fileSize) {

                // ── Step 1: read the fixed-size header ──────────────────────
                headerBuf.clear();
                int headerRead = channel.read(headerBuf, position);

                if (headerRead < WALConfig.HEADER_SIZE_BYTES) {
                    // Truncated header — crash mid-write on the header bytes.
                    channel.truncate(position);
                    return true;
                }

                headerBuf.flip();

                // Peek KEY_SIZE and VAL_SIZE using absolute gets — no position change.
                int keySize = headerBuf.getInt(WALConfig.OFFSET_KEY_SIZE);
                int valSize = headerBuf.getInt(WALConfig.OFFSET_VAL_SIZE);

                // ── Step 2: guard against corrupt size fields ────────────────
                // If KEY_SIZE or VAL_SIZE are garbage (e.g. 2^31 - 1), reading
                // that many bytes would far exceed the segment size. Treat as
                // corruption rather than attempt an oversized allocation.
                if (keySize < 0 || keySize > WALConfig.MAX_KEY_SIZE_BYTES ||
                    valSize < 0 || valSize > WALConfig.MAX_VALUE_SIZE_BYTES) {
                    channel.truncate(position);
                    return true;
                }

                int recordSize = WALConfig.HEADER_SIZE_BYTES + keySize + valSize;

                // ── Step 3: read the full record into the reuse buffer ───────
                recordBuf.clear();
                recordBuf.limit(recordSize);
                int recordRead = channel.read(recordBuf, position);

                if (recordRead < recordSize) {
                    // Partial record — crash mid-write on the payload bytes.
                    channel.truncate(position);
                    return true;
                }

                recordBuf.flip();

                // ── Step 4: deserialize and apply to index ───────────────────
                try {
                    RecoveryEntry entry = LogEntry.deserializeForRecovery(recordBuf, segmentId);
                    BytesKey key = new BytesKey(entry.key());

                    if (entry.type() == WALConfig.RECORD_TYPE_DELETE) {
                        index.remove(key);
                    } else {
                        index.put(key, entry);
                    }

                    position += recordSize;

                } catch (DataCorruptionException e) {
                    channel.truncate(position);
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Lists all {@code .log} files in {@code dataDirectory}, sorted ascending
     * by filename. Returns an empty list if the directory does not exist.
     */
    private static List<Path> findSegments(Path dataDirectory) throws IOException {
        if (!Files.exists(dataDirectory)) {
            return List.of();
        }
        try (var stream = Files.list(dataDirectory)) {
            return stream
                    .filter(p -> p.toString().endsWith(WALConfig.SEGMENT_FILE_EXTENSION))
                    .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                    .collect(Collectors.toList());
        }
    }

    /**
     * Parses the numeric segment ID from a segment filename.
     *
     * <p>Example: {@code data-000000007.log} → {@code 7}.
     *
     * @param path path to the segment file
     * @return the numeric segment ID
     * @throws IllegalArgumentException if the filename does not match the
     *                                  expected format
     */
    private static int parseSegmentId(Path path) {
        String filename   = path.getFileName().toString();
        String numberPart = filename
                .replace(WALConfig.SEGMENT_FILENAME_PREFIX, "")
                .replace(WALConfig.SEGMENT_FILE_EXTENSION, "");
        try {
            return Integer.parseInt(numberPart);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Segment filename does not match expected format '" +
                WALConfig.SEGMENT_FILENAME_PREFIX + "NNNNNNNNN" +
                WALConfig.SEGMENT_FILE_EXTENSION  + "': " + filename
            );
        }
    }
}
