package org.radsim.ghwrapper.importhook;

import org.junit.Test;
import org.radsim.ghwrapper.store.SegmentRecord;
import org.radsim.ghwrapper.store.SegmentStoreReader;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.*;

public class SegmentCaptureTest {

    @Test
    public void testReadCapturedSegments() throws IOException {
        // This test reads the captured segment file if it exists
        Path segmentFile = Paths.get("test-output.rseg");

        if (!segmentFile.toFile().exists()) {
            System.out.println("Skipping test - test-output.rseg not found");
            return;
        }

        try (SegmentStoreReader reader = new SegmentStoreReader(segmentFile)) {
            System.out.println("Reading segment store with " + reader.getRecordCount() + " records");

            int count = 0;
            long lastBaseWayId = -1;
            int segmentsInLastWay = 0;

            for (SegmentRecord record : reader) {
                count++;

                // Verify basic integrity
                assertTrue("ghEdgeId should be >= 0", record.getGhEdgeId() >= 0);
                assertTrue("baseWayId should be > 0", record.getBaseWayId() > 0);
                assertTrue("segIndex should be >= 0", record.getSegIndex() >= 0);
                assertTrue("nodeRefs should have at least 2 nodes", record.getNodeCount() >= 2);

                // Track segments per way
                if (record.getBaseWayId() != lastBaseWayId) {
                    lastBaseWayId = record.getBaseWayId();
                    segmentsInLastWay = 1;
                } else {
                    segmentsInLastWay++;
                }

                // Sample output for first few records
                if (count <= 5) {
                    System.out.println("Record " + count + ": " + record);
                }
            }

            assertEquals("Record count mismatch", reader.getRecordCount(), count);
            System.out.println("Successfully verified all " + count + " records");
        }
    }
}
