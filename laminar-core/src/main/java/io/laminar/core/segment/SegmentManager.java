package io.laminar.core.segment;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Manages the lifecycle of WAL segment files on disk.
 *
 * <p> {@code SegmentManager} Owns three responsibilities:
 * <ul>
 *   <li> Naming and creating new {@code .log} segment files. </li>
 *   <li> Rotating the active segment when it reaches capacity. </li>
 *   <li> Recovering the correct active segment on engine restart. </li>
 * </ul>
 *
 * <p> {@code WALSegment} knows how to write to one file.
 * {@code SegmentManager} knows which file to write to.
 *
 * <p><b> Thread safety: </b> not thread-safe. Only {@code LogAppender}'s
 * single writer thread calls this class — synchronization would be
 * pure overhead with no benefit.
 *
 * <p><b> Filename format: </b> {@code data-000000001.log}.
 * Nine zero-padded digits guarantee correct alphabetical sort order
 * up to ~1 billion segments (~40 years at sustained write rate for 500 bytes/write on average).
 * Decimal over hexadecimal — human readable without mental conversion.
 */
public final class SegmentManager implements Closeable {

    /**
     * Filename format for segment files.
     * Nine zero-padded digits — alphabetical sort = numerical sort.
     * Example: {@code data-000000001.log}, {@code data-000000042.log}.
     */
    static final String SEGMENT_NAME_FORMAT = "data-%09d";

    /** Directory where all segment files are stored. */
    private final Path dataDirectory;

    /** The segment currently accepting writes. Never null after construction. */
    private WALSegment activeSegment;

    /** ID assigned to the next segment created by {@link #roll()}. */
    private int nextSegmentId;

    /**
     * Opens the segment directory and recovers or creates the active segment.
     *
     * <p> Recovery logic on startup:
     * <ol>
     *   <li> Scan {@code dataDirectory} for all {@code *.log} files. </li>
     *   <li> Sort ascending by name — alphabetical order equals numerical
     *        order due to zero-padded filenames. </li>
     *   <li> The last file is the candidate for the active segment. </li>
     *   <li> If it is full, roll a new segment immediately. </li>
     *   <li> If no files exist, create {@code data-000000001.log}. </li>
     * </ol>
     *
     * @param dataDirectory directory where segment files are stored
     * @throws IOException if the directory cannot be read or a segment
     *                     file cannot be opened
     */
    public SegmentManager(Path dataDirectory) throws IOException {
        this.dataDirectory = dataDirectory;
        Files.createDirectories(dataDirectory);
        recover();
    }

    /**
     * Scans the data directory and restores the active segment.
     *
     * <p> Called once at construction. After this method returns,
     * {@link #activeSegment} is non-null and ready to accept writes.
     *
     * @throws IOException if segment files cannot be read
     */
    private void recover() throws IOException {
        List<Path> segments = findExistingSegments();

        if (segments.isEmpty()) {
            // Brand new data directory — start from segment 1
            nextSegmentId  = 1;
            activeSegment  = createSegment(nextSegmentId++);
            return;
        }

        // Last file in sorted order = highest sequence number = active candidate
        Path lastPath    = segments.get(segments.size() - 1);
        int  lastId      = parseSegmentId(lastPath);
        nextSegmentId    = lastId + 1;
        activeSegment    = new WALSegment(lastPath, lastId);

        // If the last segment is already full, roll immediately
        if (activeSegment.isFull()) {
            activeSegment.close();
            activeSegment = createSegment(nextSegmentId++);
        }
    }

    /**
     * Returns the segment currently accepting writes.
     *
     * <p> Callers should check {@link #rotateIfFull()} before each write
     * to ensure the active segment has capacity.
     */
    WALSegment getActiveSegment() {
        return activeSegment;
    }

    /**
     * Rotates the active segment if it has reached capacity.
     *
     * <p> Rotation sequence:
     * <ol>
     *   <li> Close the current active segment — triggers a final flush. </li>
     *   <li> Create a new segment with the next sequence ID. </li>
     *   <li> Promote it to active. </li>
     * </ol>
     *
     * <p> Called by {@code LogAppender} before every write batch.
     * If the active segment is not full this is a fast no-op —
     * just one {@code boolean} check.
     *
     * @throws IOException if the new segment file cannot be created
     */
    void rotateIfFull() throws IOException {
        if (activeSegment.isFull()) {
            activeSegment.close();
            activeSegment = createSegment(nextSegmentId++);
        }
    }

    /**
     * Creates a new segment file with the given ID.
     *
     * @param segmentId the monotonically increasing segment identifier
     * @return an open {@link WALSegment} ready to accept writes
     * @throws IOException if the file cannot be created
     */
    private WALSegment createSegment(int segmentId) throws IOException {
        String filename = String.format(SEGMENT_NAME_FORMAT, segmentId)
                        + WALSegment.EXTENSION;
        Path path = dataDirectory.resolve(filename);
        return new WALSegment(path, segmentId);
    }

    /**
     * Lists all {@code .log} files in the data directory, sorted
     * ascending by filename.
     *
     * <p> Alphabetical sort equals numerical sort because filenames
     * use zero-padded sequence numbers.
     *
     * @return sorted list of segment paths — empty if none exist
     * @throws IOException if the directory cannot be read
     */
    private List<Path> findExistingSegments() throws IOException {
        try (var stream = Files.list(dataDirectory)) {
            return stream
                .filter(p -> p.toString().endsWith(WALSegment.EXTENSION))
                .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                .collect(Collectors.toList());
        }
    }

    /**
     * Parses the segment ID from a segment filename.
     *
     * <p> Example: {@code data-000000007.log} → {@code 7}.
     *
     * @param path path to a segment file
     * @return the numeric segment ID embedded in the filename
     * @throws IllegalArgumentException if the filename does not match
     *                                  the expected format
     */
    private int parseSegmentId(Path path) {
        String filename = path.getFileName().toString();
        // Strip "data-" prefix and ".log" suffix, parse the padded number
        String numberPart = filename
                .replace("data-", "")
                .replace(WALSegment.EXTENSION, "");
        try {
            return Integer.parseInt(numberPart);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Segment filename does not match expected format " +
                "'data-NNNNNNNNN.log': " + filename
            );
        }
    }

    /**
     * Rotates if full, then appends {@code buffer} to the active segment.
     *
     * <p>Combines rotation and append into one call so {@link io.laminar.core.append.LogAppender}
     * does not need direct access to {@link WALSegment}, which is package-private.
     *
     * @param buffer serialized record in read mode — must be flipped before calling
     * @return byte offset where this record starts in the active segment file
     * @throws IOException if rotation or the append fails
     */
    public long append(ByteBuffer buffer) throws IOException {
        rotateIfFull();
        return activeSegment.append(buffer);
    }

    /**
     * Flushes the active segment to durable storage ({@code fdatasync}).
     *
     * <p>Called by {@link io.laminar.core.append.LogAppender} on the group commit
     * schedule — every {@link io.laminar.core.config.WALConfig#FLUSH_INTERVAL_MS} ms
     * or every {@link io.laminar.core.config.WALConfig#FLUSH_BUFFER_SIZE_BYTES} bytes,
     * whichever comes first.
     *
     * @throws IOException if the flush fails
     */
    public void flush() throws IOException {
        activeSegment.flush();
    }

    /**
     * Closes the active segment and releases all resources.
     *
     * <p> Called when the engine shuts down. Delegates to
     * {@link WALSegment#close()} which triggers a final flush
     * before releasing the file descriptor.
     *
     * @throws IOException if the active segment cannot be closed
     */
    @Override
    public void close() throws IOException {
        if (activeSegment != null) {
            activeSegment.close();
        }
    }
}
