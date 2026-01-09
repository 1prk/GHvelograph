package org.radsim.ghwrapper.importhook;

import com.graphhopper.reader.ReaderWay;
import com.graphhopper.reader.osm.WaySegmentParserWithCallback;
import com.graphhopper.util.PointList;
import org.radsim.ghwrapper.store.SegmentRecord;
import org.radsim.ghwrapper.store.SegmentStoreWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * Edge handler that captures segment metadata via SegmentCallback and matches it
 * with GraphHopper edge IDs.
 *
 * This class implements both SegmentCallback (called BEFORE edge creation) and
 * EdgeHandler (called DURING edge creation). It maintains a queue of pending
 * segment metadata and matches them with sequentially-assigned edge IDs.
 *
 * Usage:
 * <pre>
 * SegmentCapturingEdgeHandler handler = new SegmentCapturingEdgeHandler(segmentStoreWriter);
 * WaySegmentParserWithCallback parser = new WaySegmentParserWithCallback.Builder(...)
 *     .setSegmentCallback(handler)
 *     .setEdgeHandler(handler)
 *     .build();
 * </pre>
 */
public class SegmentCapturingEdgeHandler implements SegmentCallback, WaySegmentParserWithCallback.EdgeHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentCapturingEdgeHandler.class);

    private final SegmentStoreWriter segmentStoreWriter;
    private final Queue<PendingSegment> pendingSegments = new ArrayDeque<>();
    private int segmentsCaptured = 0;
    private int nextGhEdgeId = 0;  // Track edge IDs since they're assigned sequentially

    public SegmentCapturingEdgeHandler(SegmentStoreWriter segmentStoreWriter) {
        this.segmentStoreWriter = segmentStoreWriter;
    }

    @Override
    public void onSegment(long baseWayId, List<Long> osmNodeIds, int segIndex, boolean isBarrierEdge) {
        // Queue the pending segment metadata before the edge is created
        pendingSegments.add(new PendingSegment(baseWayId, osmNodeIds, segIndex, isBarrierEdge));
    }

    @Override
    public void handleEdge(int from, int to, PointList pointList, ReaderWay way, List<Map<String, Object>> nodeTags) {
        // Pop the pending segment metadata
        PendingSegment pending = pendingSegments.poll();
        if (pending == null) {
            throw new IllegalStateException("No pending segment for edge " + from + "->" + to +
                    ". onSegment() and handleEdge() calls are out of sync.");
        }

        // Assign the next sequential edge ID
        int ghEdgeId = nextGhEdgeId++;

        // Convert List<Long> to long[] for SegmentRecord
        long[] nodeRefs = new long[pending.osmNodeIds.size()];
        for (int i = 0; i < pending.osmNodeIds.size(); i++) {
            nodeRefs[i] = pending.osmNodeIds.get(i);
        }

        // Create and write the SegmentRecord
        SegmentRecord record = new SegmentRecord(ghEdgeId, pending.baseWayId, pending.segIndex, pending.isBarrierEdge, nodeRefs);

        try {
            segmentStoreWriter.write(record);
            segmentsCaptured++;

            if (segmentsCaptured % 100_000 == 0) {
                LOGGER.info("Captured {} segments", segmentsCaptured);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write segment record: " + record, e);
        }
    }

    public int getSegmentsCaptured() {
        return segmentsCaptured;
    }

    /**
     * Verifies that all pending segments have been processed.
     * Should be called after import completes.
     */
    public void verifyComplete() {
        if (!pendingSegments.isEmpty()) {
            throw new IllegalStateException("Import completed but " + pendingSegments.size() +
                    " pending segments were not matched with edges. This indicates a bug in the integration.");
        }
        LOGGER.info("Verification passed: all {} segments were successfully captured", segmentsCaptured);
    }

    /**
     * Temporary holder for segment metadata before the edge is created.
     */
    private static class PendingSegment {
        final long baseWayId;
        final List<Long> osmNodeIds;
        final int segIndex;
        final boolean isBarrierEdge;

        PendingSegment(long baseWayId, List<Long> osmNodeIds, int segIndex, boolean isBarrierEdge) {
            this.baseWayId = baseWayId;
            this.osmNodeIds = osmNodeIds;
            this.segIndex = segIndex;
            this.isBarrierEdge = isBarrierEdge;
        }
    }
}
