package org.radsim.ghwrapper.store;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class SegmentStoreTest {

    private Path tempFile;

    @Before
    public void setUp() throws IOException {
        tempFile = Files.createTempFile("segment-store-test", ".rseg");
    }

    @After
    public void tearDown() throws IOException {
        if (tempFile != null && Files.exists(tempFile)) {
            Files.delete(tempFile);
        }
    }

    @Test
    public void testWriteAndReadSingleRecord() throws IOException {
        // Create a test record
        long[] nodeRefs = {1001L, 1002L, 1003L};
        SegmentRecord original = new SegmentRecord(42, 999L, 0, false, nodeRefs);

        // Write the record
        try (SegmentStoreWriter writer = new SegmentStoreWriter(tempFile)) {
            writer.write(original);
            assertEquals(1, writer.getRecordCount());
        }

        // Read it back
        try (SegmentStoreReader reader = new SegmentStoreReader(tempFile)) {
            assertEquals(1, reader.getRecordCount());

            List<SegmentRecord> records = new ArrayList<>();
            for (SegmentRecord record : reader) {
                records.add(record);
            }

            assertEquals(1, records.size());
            SegmentRecord read = records.get(0);

            assertEquals(original, read);
            assertEquals(42, read.getGhEdgeId());
            assertEquals(999L, read.getBaseWayId());
            assertEquals(0, read.getSegIndex());
            assertFalse(read.isBarrierEdge());
            assertArrayEquals(nodeRefs, read.getNodeRefs());
        }
    }

    @Test
    public void testWriteAndReadMultipleRecords() throws IOException {
        List<SegmentRecord> originalRecords = new ArrayList<>();

        // Create test records
        originalRecords.add(new SegmentRecord(1, 100L, 0, false, new long[]{1, 2, 3}));
        originalRecords.add(new SegmentRecord(2, 100L, 1, false, new long[]{3, 4, 5, 6}));
        originalRecords.add(new SegmentRecord(3, 200L, 0, true, new long[]{10, 11}));
        originalRecords.add(new SegmentRecord(4, 300L, 0, false, new long[]{20, 21, 22, 23, 24}));

        // Write records
        try (SegmentStoreWriter writer = new SegmentStoreWriter(tempFile)) {
            for (SegmentRecord record : originalRecords) {
                writer.write(record);
            }
            assertEquals(4, writer.getRecordCount());
        }

        // Read records back
        try (SegmentStoreReader reader = new SegmentStoreReader(tempFile)) {
            assertEquals(4, reader.getRecordCount());

            List<SegmentRecord> readRecords = new ArrayList<>();
            for (SegmentRecord record : reader) {
                readRecords.add(record);
            }

            assertEquals(originalRecords.size(), readRecords.size());
            for (int i = 0; i < originalRecords.size(); i++) {
                assertEquals("Record " + i + " mismatch", originalRecords.get(i), readRecords.get(i));
            }
        }
    }

    @Test
    public void testBarrierEdgeFlag() throws IOException {
        SegmentRecord barrierRecord = new SegmentRecord(1, 100L, 0, true, new long[]{1, 2});
        SegmentRecord normalRecord = new SegmentRecord(2, 100L, 1, false, new long[]{2, 3});

        try (SegmentStoreWriter writer = new SegmentStoreWriter(tempFile)) {
            writer.write(barrierRecord);
            writer.write(normalRecord);
        }

        try (SegmentStoreReader reader = new SegmentStoreReader(tempFile)) {
            List<SegmentRecord> records = new ArrayList<>();
            for (SegmentRecord record : reader) {
                records.add(record);
            }

            assertTrue("First record should be barrier edge", records.get(0).isBarrierEdge());
            assertFalse("Second record should not be barrier edge", records.get(1).isBarrierEdge());
        }
    }

    @Test
    public void testRandomAccessLookup() throws IOException {
        // Write multiple records with different ghEdgeIds
        try (SegmentStoreWriter writer = new SegmentStoreWriter(tempFile)) {
            writer.write(new SegmentRecord(10, 100L, 0, false, new long[]{1, 2}));
            writer.write(new SegmentRecord(20, 200L, 0, false, new long[]{3, 4}));
            writer.write(new SegmentRecord(30, 300L, 0, false, new long[]{5, 6}));
            writer.write(new SegmentRecord(40, 400L, 0, true, new long[]{7, 8, 9}));
        }

        // Read with random access enabled
        try (SegmentStoreReader reader = new SegmentStoreReader(tempFile, true)) {
            // Lookup existing records
            SegmentRecord record20 = reader.getByGhEdgeId(20);
            assertNotNull(record20);
            assertEquals(20, record20.getGhEdgeId());
            assertEquals(200L, record20.getBaseWayId());
            assertArrayEquals(new long[]{3, 4}, record20.getNodeRefs());

            SegmentRecord record40 = reader.getByGhEdgeId(40);
            assertNotNull(record40);
            assertEquals(40, record40.getGhEdgeId());
            assertTrue(record40.isBarrierEdge());
            assertArrayEquals(new long[]{7, 8, 9}, record40.getNodeRefs());

            // Lookup non-existing record
            SegmentRecord notFound = reader.getByGhEdgeId(99);
            assertNull(notFound);
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRandomAccessNotEnabled() throws IOException {
        // Write a record
        try (SegmentStoreWriter writer = new SegmentStoreWriter(tempFile)) {
            writer.write(new SegmentRecord(1, 100L, 0, false, new long[]{1, 2}));
        }

        // Try to use random access without enabling it
        try (SegmentStoreReader reader = new SegmentStoreReader(tempFile, false)) {
            reader.getByGhEdgeId(1); // Should throw
        }
    }

    @Test
    public void testEmptyStore() throws IOException {
        // Write no records
        try (SegmentStoreWriter writer = new SegmentStoreWriter(tempFile)) {
            assertEquals(0, writer.getRecordCount());
        }

        // Read empty store
        try (SegmentStoreReader reader = new SegmentStoreReader(tempFile)) {
            assertEquals(0, reader.getRecordCount());

            List<SegmentRecord> records = new ArrayList<>();
            for (SegmentRecord record : reader) {
                records.add(record);
            }
            assertTrue(records.isEmpty());
        }
    }

    @Test
    public void testLargeNodeRefsList() throws IOException {
        // Create a record with many node references
        long[] manyNodes = new long[1000];
        for (int i = 0; i < manyNodes.length; i++) {
            manyNodes[i] = 10000L + i;
        }

        SegmentRecord record = new SegmentRecord(1, 999L, 0, false, manyNodes);

        try (SegmentStoreWriter writer = new SegmentStoreWriter(tempFile)) {
            writer.write(record);
        }

        try (SegmentStoreReader reader = new SegmentStoreReader(tempFile)) {
            List<SegmentRecord> records = new ArrayList<>();
            for (SegmentRecord r : reader) {
                records.add(r);
            }

            assertEquals(1, records.size());
            SegmentRecord read = records.get(0);
            assertEquals(1000, read.getNodeCount());
            assertArrayEquals(manyNodes, read.getNodeRefs());
        }
    }

    @Test(expected = IOException.class)
    public void testInvalidMagicBytes() throws IOException {
        // Write invalid file
        Files.write(tempFile, new byte[]{0, 1, 2, 3, 1, 0, 0, 0, 0});

        // Try to read it
        new SegmentStoreReader(tempFile);
    }

    @Test(expected = IllegalStateException.class)
    public void testWriteAfterClose() throws IOException {
        SegmentStoreWriter writer = new SegmentStoreWriter(tempFile);
        writer.close();

        // Try to write after close
        writer.write(new SegmentRecord(1, 100L, 0, false, new long[]{1, 2}));
    }

    @Test
    public void testSegmentRecordEquality() {
        SegmentRecord record1 = new SegmentRecord(1, 100L, 0, false, new long[]{1, 2, 3});
        SegmentRecord record2 = new SegmentRecord(1, 100L, 0, false, new long[]{1, 2, 3});
        SegmentRecord record3 = new SegmentRecord(2, 100L, 0, false, new long[]{1, 2, 3});

        assertEquals(record1, record2);
        assertNotEquals(record1, record3);
        assertEquals(record1.hashCode(), record2.hashCode());
    }

    @Test
    public void testSegmentRecordToString() {
        SegmentRecord record = new SegmentRecord(42, 999L, 2, true, new long[]{1, 2, 3});
        String str = record.toString();

        assertTrue(str.contains("ghEdgeId=42"));
        assertTrue(str.contains("baseWayId=999"));
        assertTrue(str.contains("segIndex=2"));
        assertTrue(str.contains("isBarrierEdge=true"));
        assertTrue(str.contains("nodeCount=3"));
    }
}
