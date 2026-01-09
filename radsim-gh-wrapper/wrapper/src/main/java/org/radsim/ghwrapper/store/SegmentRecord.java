package org.radsim.ghwrapper.store;

import java.util.Arrays;
import java.util.Objects;

/**
 * Represents a segment record containing metadata about a GraphHopper edge
 * and its corresponding OSM way segment.
 */
public class SegmentRecord {
    private final int ghEdgeId;
    private final long baseWayId;
    private final int segIndex;
    private final byte flags;
    private final long[] nodeRefs;

    // Flag bit masks
    public static final byte FLAG_BARRIER_EDGE = 0x01;

    public SegmentRecord(int ghEdgeId, long baseWayId, int segIndex, boolean isBarrierEdge, long[] nodeRefs) {
        this.ghEdgeId = ghEdgeId;
        this.baseWayId = baseWayId;
        this.segIndex = segIndex;
        this.flags = isBarrierEdge ? FLAG_BARRIER_EDGE : 0;
        this.nodeRefs = Objects.requireNonNull(nodeRefs, "nodeRefs cannot be null");
        if (nodeRefs.length < 2) {
            throw new IllegalArgumentException("nodeRefs must contain at least 2 nodes");
        }
    }

    public SegmentRecord(int ghEdgeId, long baseWayId, int segIndex, byte flags, long[] nodeRefs) {
        this.ghEdgeId = ghEdgeId;
        this.baseWayId = baseWayId;
        this.segIndex = segIndex;
        this.flags = flags;
        this.nodeRefs = Objects.requireNonNull(nodeRefs, "nodeRefs cannot be null");
    }

    public int getGhEdgeId() {
        return ghEdgeId;
    }

    public long getBaseWayId() {
        return baseWayId;
    }

    public int getSegIndex() {
        return segIndex;
    }

    public byte getFlags() {
        return flags;
    }

    public boolean isBarrierEdge() {
        return (flags & FLAG_BARRIER_EDGE) != 0;
    }

    public long[] getNodeRefs() {
        return nodeRefs;
    }

    public int getNodeCount() {
        return nodeRefs.length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SegmentRecord that = (SegmentRecord) o;
        return ghEdgeId == that.ghEdgeId &&
               baseWayId == that.baseWayId &&
               segIndex == that.segIndex &&
               flags == that.flags &&
               Arrays.equals(nodeRefs, that.nodeRefs);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(ghEdgeId, baseWayId, segIndex, flags);
        result = 31 * result + Arrays.hashCode(nodeRefs);
        return result;
    }

    @Override
    public String toString() {
        return "SegmentRecord{" +
               "ghEdgeId=" + ghEdgeId +
               ", baseWayId=" + baseWayId +
               ", segIndex=" + segIndex +
               ", isBarrierEdge=" + isBarrierEdge() +
               ", nodeCount=" + nodeRefs.length +
               '}';
    }
}
