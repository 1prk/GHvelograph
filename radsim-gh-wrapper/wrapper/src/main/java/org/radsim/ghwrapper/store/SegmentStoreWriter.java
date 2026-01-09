package org.radsim.ghwrapper.store;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Writer for segment store files in RSEG format v1.
 *
 * File format:
 * <pre>
 * Header:
 *   - magic: 4 bytes "RSEG"
 *   - version: 1 byte (0x01)
 *   - recordCount: int32 (placeholder, updated on close)
 *
 * Records (sequential):
 *   - ghEdgeId: int32
 *   - baseWayId: int64
 *   - segIndex: int32
 *   - flags: byte
 *   - nodeCount: int32
 *   - nodeRefs: nodeCount * int64
 * </pre>
 *
 * Usage:
 * <pre>
 * try (SegmentStoreWriter writer = new SegmentStoreWriter(path)) {
 *     writer.write(record1);
 *     writer.write(record2);
 * }
 * </pre>
 */
public class SegmentStoreWriter implements Closeable {
    private static final byte[] MAGIC = {'R', 'S', 'E', 'G'};
    private static final byte VERSION = 1;
    private static final int HEADER_SIZE = 4 + 1 + 4; // magic + version + recordCount

    private final Path path;
    private final DataOutputStream out;
    private int recordCount;
    private boolean closed;

    public SegmentStoreWriter(Path path) throws IOException {
        this.path = path;
        this.out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path.toFile())));
        this.recordCount = 0;
        this.closed = false;
        writeHeader();
    }

    private void writeHeader() throws IOException {
        out.write(MAGIC);
        out.writeByte(VERSION);
        out.writeInt(0); // Placeholder for recordCount, will be updated on close
    }

    /**
     * Writes a segment record to the store.
     *
     * @param record the record to write
     * @throws IOException if an I/O error occurs
     * @throws IllegalStateException if the writer is closed
     */
    public void write(SegmentRecord record) throws IOException {
        if (closed) {
            throw new IllegalStateException("Writer is closed");
        }

        out.writeInt(record.getGhEdgeId());
        out.writeLong(record.getBaseWayId());
        out.writeInt(record.getSegIndex());
        out.writeByte(record.getFlags());
        out.writeInt(record.getNodeCount());

        for (long nodeRef : record.getNodeRefs()) {
            out.writeLong(nodeRef);
        }

        recordCount++;
    }

    /**
     * Returns the number of records written so far.
     */
    public int getRecordCount() {
        return recordCount;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        closed = true;
        out.close();

        // Update the record count in the header
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(path.toFile(), "rw")) {
            raf.seek(5); // Skip magic (4 bytes) and version (1 byte)
            raf.writeInt(recordCount);
        }
    }
}
