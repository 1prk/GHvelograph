package org.radsim.ghwrapper.store;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Reader for segment store files in RSEG format v1.
 *
 * Supports:
 * - Sequential iteration over all records (loads all into memory)
 * - Optional random access by ghEdgeId via in-memory index
 *
 * Usage (sequential):
 * <pre>
 * try (SegmentStoreReader reader = new SegmentStoreReader(path)) {
 *     for (SegmentRecord record : reader) {
 *         // Process record
 *     }
 * }
 * </pre>
 *
 * Usage (random access):
 * <pre>
 * try (SegmentStoreReader reader = new SegmentStoreReader(path, true)) {
 *     SegmentRecord record = reader.getByGhEdgeId(123);
 * }
 * </pre>
 */
public class SegmentStoreReader implements Closeable, Iterable<SegmentRecord> {
    private static final byte[] MAGIC = {'R', 'S', 'E', 'G'};
    private static final byte VERSION = 1;

    private final Path path;
    private final int recordCount;
    private final boolean randomAccessEnabled;
    private Map<Integer, Long> ghEdgeIdToOffset;
    private RandomAccessFile randomAccessFile;
    private List<SegmentRecord> cachedRecords;

    /**
     * Creates a reader for sequential access only.
     */
    public SegmentStoreReader(Path path) throws IOException {
        this(path, false);
    }

    /**
     * Creates a reader with optional random access support.
     *
     * @param path the path to the segment store file
     * @param enableRandomAccess if true, builds an in-memory index for random access
     */
    public SegmentStoreReader(Path path, boolean enableRandomAccess) throws IOException {
        this.path = path;
        this.randomAccessEnabled = enableRandomAccess;

        // Read and validate header
        try (DataInputStream in = new DataInputStream(new FileInputStream(path.toFile()))) {
            byte[] magic = new byte[4];
            in.readFully(magic);
            if (!java.util.Arrays.equals(magic, MAGIC)) {
                throw new IOException("Invalid magic bytes. Expected RSEG format.");
            }

            byte version = in.readByte();
            if (version != VERSION) {
                throw new IOException("Unsupported version: " + version + ". Expected: " + VERSION);
            }

            this.recordCount = in.readInt();
        }

        if (enableRandomAccess) {
            buildIndex();
        }
    }

    /**
     * Builds an in-memory index mapping ghEdgeId to file offset.
     */
    private void buildIndex() throws IOException {
        ghEdgeIdToOffset = new HashMap<>();
        randomAccessFile = new RandomAccessFile(path.toFile(), "r");

        long offset = 9; // Skip header: 4 (magic) + 1 (version) + 4 (recordCount)
        randomAccessFile.seek(offset);

        for (int i = 0; i < recordCount; i++) {
            int ghEdgeId = randomAccessFile.readInt();
            ghEdgeIdToOffset.put(ghEdgeId, offset);

            // Skip rest of record
            randomAccessFile.readLong();  // baseWayId
            randomAccessFile.readInt();   // segIndex
            randomAccessFile.readByte();  // flags
            int nodeCount = randomAccessFile.readInt();
            randomAccessFile.skipBytes(nodeCount * 8); // Skip nodeRefs

            offset = randomAccessFile.getFilePointer();
        }
    }

    /**
     * Retrieves a record by its GraphHopper edge ID.
     *
     * @param ghEdgeId the edge ID to look up
     * @return the segment record, or null if not found
     * @throws IOException if an I/O error occurs
     * @throws UnsupportedOperationException if random access is not enabled
     */
    public SegmentRecord getByGhEdgeId(int ghEdgeId) throws IOException {
        if (!randomAccessEnabled) {
            throw new UnsupportedOperationException("Random access not enabled. Create reader with enableRandomAccess=true");
        }

        Long offset = ghEdgeIdToOffset.get(ghEdgeId);
        if (offset == null) {
            return null;
        }

        synchronized (randomAccessFile) {
            randomAccessFile.seek(offset);
            return readRecordFromRandomAccessFile(randomAccessFile);
        }
    }

    private SegmentRecord readRecordFromRandomAccessFile(RandomAccessFile raf) throws IOException {
        int ghEdgeId = raf.readInt();
        long baseWayId = raf.readLong();
        int segIndex = raf.readInt();
        byte flags = raf.readByte();
        int nodeCount = raf.readInt();

        long[] nodeRefs = new long[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            nodeRefs[i] = raf.readLong();
        }

        return new SegmentRecord(ghEdgeId, baseWayId, segIndex, flags, nodeRefs);
    }

    public int getRecordCount() {
        return recordCount;
    }

    /**
     * Loads all records into memory for iteration.
     * WARNING: Only use this for small datasets. For large datasets, use iterator() for streaming.
     */
    private List<SegmentRecord> loadAllRecords() throws IOException {
        if (cachedRecords != null) {
            return cachedRecords;
        }

        cachedRecords = new ArrayList<>(recordCount);

        try (DataInputStream in = new DataInputStream(new FileInputStream(path.toFile()))) {
            // Skip header
            in.skipBytes(9); // 4 (magic) + 1 (version) + 4 (recordCount)

            for (int i = 0; i < recordCount; i++) {
                int ghEdgeId = in.readInt();
                long baseWayId = in.readLong();
                int segIndex = in.readInt();
                byte flags = in.readByte();
                int nodeCount = in.readInt();

                long[] nodeRefs = new long[nodeCount];
                for (int j = 0; j < nodeCount; j++) {
                    nodeRefs[j] = in.readLong();
                }

                cachedRecords.add(new SegmentRecord(ghEdgeId, baseWayId, segIndex, flags, nodeRefs));
            }
        }

        return cachedRecords;
    }

    @Override
    public Iterator<SegmentRecord> iterator() {
        try {
            return new StreamingIterator();
        } catch (IOException e) {
            throw new RuntimeException("Failed to create streaming iterator", e);
        }
    }

    /**
     * Streaming iterator that reads records on-demand without loading all into memory.
     */
    private class StreamingIterator implements Iterator<SegmentRecord> {
        private final DataInputStream inputStream;
        private int recordsRead;

        StreamingIterator() throws IOException {
            this.inputStream = new DataInputStream(new FileInputStream(path.toFile()));
            // Skip header
            inputStream.skipBytes(9); // 4 (magic) + 1 (version) + 4 (recordCount)
            this.recordsRead = 0;
        }

        @Override
        public boolean hasNext() {
            boolean hasMore = recordsRead < recordCount;
            if (!hasMore) {
                // Close stream when done
                try {
                    inputStream.close();
                } catch (IOException e) {
                    // Log but don't throw - we're done anyway
                }
            }
            return hasMore;
        }

        @Override
        public SegmentRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            try {
                int ghEdgeId = inputStream.readInt();
                long baseWayId = inputStream.readLong();
                int segIndex = inputStream.readInt();
                byte flags = inputStream.readByte();
                int nodeCount = inputStream.readInt();

                long[] nodeRefs = new long[nodeCount];
                for (int j = 0; j < nodeCount; j++) {
                    nodeRefs[j] = inputStream.readLong();
                }

                recordsRead++;
                return new SegmentRecord(ghEdgeId, baseWayId, segIndex, flags, nodeRefs);
            } catch (IOException e) {
                try {
                    inputStream.close();
                } catch (IOException e2) {
                    // Suppress
                }
                throw new RuntimeException("Failed to read segment record at index " + recordsRead, e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (randomAccessFile != null) {
            randomAccessFile.close();
        }
    }
}
