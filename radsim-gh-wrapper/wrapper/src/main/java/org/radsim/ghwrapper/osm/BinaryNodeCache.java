package org.radsim.ghwrapper.osm;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Binary memory-mapped node cache for efficient storage and retrieval.
 *
 * Format:
 * - Header: "RNOD" (4 bytes) + version (1 byte) + nodeCount (int32)
 * - Index section: sorted array of nodeId (int64) + offset (int32) pairs
 * - Data section: fixed-size records (lat: double, lon: double, ele: double)
 *
 * Fixed record size: 24 bytes (3 * 8-byte doubles)
 * Index entry size: 12 bytes (8-byte ID + 4-byte offset)
 *
 * Memory usage for 100M nodes:
 * - Index: ~1.2 GB
 * - Data: ~2.4 GB
 * - Total: ~3.6 GB (memory-mapped, not all in heap)
 *
 * Compared to text format + HashMap:
 * - Text: ~20-30 GB in heap (strings, object overhead, HashMap)
 * - Binary: ~3.6 GB memory-mapped (minimal heap usage)
 */
public class BinaryNodeCache implements Closeable, INodeCache {
    private static final byte[] MAGIC = {'R', 'N', 'O', 'D'};
    private static final byte VERSION = 1;
    private static final int RECORD_SIZE = 24; // 3 * 8 bytes (lat, lon, ele)

    private final Path cacheFile;
    private FileChannel writeChannel;
    private DataOutputStream indexStream;
    private DataOutputStream dataStream;
    private Path tempIndexFile;
    private Path tempDataFile;
    private int nodeCount = 0;

    // For reading
    private MappedByteBuffer indexBuffer;
    private MappedByteBuffer dataBuffer;
    private Long2IntOpenHashMap indexMap;

    public BinaryNodeCache(Path cacheFile) {
        this.cacheFile = cacheFile;
    }

    /**
     * Opens cache for writing. Uses temporary files that are merged on close.
     */
    public void openForWrite() throws IOException {
        Files.createDirectories(cacheFile.getParent());

        // Create temp files for index and data
        tempIndexFile = cacheFile.getParent().resolve(cacheFile.getFileName() + ".idx.tmp");
        tempDataFile = cacheFile.getParent().resolve(cacheFile.getFileName() + ".dat.tmp");

        indexStream = new DataOutputStream(new BufferedOutputStream(
            new FileOutputStream(tempIndexFile.toFile()), 1024 * 1024));
        dataStream = new DataOutputStream(new BufferedOutputStream(
            new FileOutputStream(tempDataFile.toFile()), 1024 * 1024));

        nodeCount = 0;
    }

    /**
     * Writes a node. Nodes should be written in sorted order by ID for optimal index building.
     */
    public void put(OsmNode node) throws IOException {
        if (indexStream == null) {
            throw new IllegalStateException("Cache not opened for writing");
        }

        // Write index entry: nodeId + data offset
        indexStream.writeLong(node.getId());
        indexStream.writeInt(nodeCount * RECORD_SIZE);

        // Write data record: lat, lon, ele
        dataStream.writeDouble(node.getLat());
        dataStream.writeDouble(node.getLon());
        dataStream.writeDouble(node.hasElevation() ? node.getEle() : Double.NaN);

        nodeCount++;

        if (nodeCount % 1_000_000 == 0) {
            System.out.println("  Binary node cache: written " + nodeCount + " nodes");
        }
    }

    /**
     * Finalizes writing and creates the final binary file.
     */
    public void finishWrite() throws IOException {
        if (indexStream == null) {
            return;
        }

        indexStream.close();
        dataStream.close();

        System.out.println("Merging index and data into final cache file...");

        // Merge into final file
        try (FileChannel outChannel = FileChannel.open(cacheFile,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {

            // Write header
            ByteBuffer header = ByteBuffer.allocate(9);
            header.put(MAGIC);
            header.put(VERSION);
            header.putInt(nodeCount);
            header.flip();
            outChannel.write(header);

            // Copy index
            try (FileChannel indexChannel = FileChannel.open(tempIndexFile, StandardOpenOption.READ)) {
                indexChannel.transferTo(0, indexChannel.size(), outChannel);
            }

            // Copy data
            try (FileChannel dataChannel = FileChannel.open(tempDataFile, StandardOpenOption.READ)) {
                dataChannel.transferTo(0, dataChannel.size(), outChannel);
            }
        }

        // Clean up temp files
        Files.deleteIfExists(tempIndexFile);
        Files.deleteIfExists(tempDataFile);

        indexStream = null;
        dataStream = null;

        System.out.println("Binary node cache complete: " + nodeCount + " nodes");
    }

    /**
     * Loads the cache for reading. Builds an in-memory index for fast lookups.
     */
    public void load() throws IOException {
        if (!Files.exists(cacheFile)) {
            indexMap = new Long2IntOpenHashMap();
            indexMap.defaultReturnValue(-1);
            return;
        }

        try (FileChannel channel = FileChannel.open(cacheFile, StandardOpenOption.READ)) {
            // Read and validate header
            ByteBuffer header = ByteBuffer.allocate(9);
            channel.read(header);
            header.flip();

            byte[] magic = new byte[4];
            header.get(magic);
            if (!java.util.Arrays.equals(magic, MAGIC)) {
                throw new IOException("Invalid node cache format");
            }

            byte version = header.get();
            if (version != VERSION) {
                throw new IOException("Unsupported node cache version: " + version);
            }

            int count = header.getInt();

            // Map index section (12 bytes per entry: 8-byte ID + 4-byte offset)
            long indexSize = (long) count * 12;
            indexBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 9, indexSize);

            // Map data section
            long dataOffset = 9 + indexSize;
            long dataSize = (long) count * RECORD_SIZE;
            dataBuffer = channel.map(FileChannel.MapMode.READ_ONLY, dataOffset, dataSize);

            // Build in-memory index map for fast binary search alternative
            // For large datasets, this uses ~1.2GB for 100M nodes (much less than text format)
            indexMap = new Long2IntOpenHashMap(count);
            indexMap.defaultReturnValue(-1);

            for (int i = 0; i < count; i++) {
                long nodeId = indexBuffer.getLong(i * 12);
                int offset = indexBuffer.getInt(i * 12 + 8);
                indexMap.put(nodeId, offset);
            }

            System.out.println("Loaded binary node cache: " + count + " nodes, index size: " +
                             (indexMap.size() * 12 / 1024 / 1024) + " MB");
        }
    }

    /**
     * Retrieves a node by ID.
     */
    public OsmNode get(long osmNodeId) {
        if (indexMap == null) {
            return null;
        }

        int offset = indexMap.get(osmNodeId);
        if (offset < 0) {
            return null;
        }

        double lat = dataBuffer.getDouble(offset);
        double lon = dataBuffer.getDouble(offset + 8);
        double ele = dataBuffer.getDouble(offset + 16);

        return new OsmNode(osmNodeId, lat, lon, ele);
    }

    /**
     * Returns the number of nodes in the cache.
     */
    public int size() {
        return indexMap != null ? indexMap.size() : 0;
    }

    @Override
    public void close() throws IOException {
        if (indexStream != null) {
            finishWrite();
        }

        // Memory-mapped buffers are automatically released when garbage collected
        indexBuffer = null;
        dataBuffer = null;
        indexMap = null;
    }
}
