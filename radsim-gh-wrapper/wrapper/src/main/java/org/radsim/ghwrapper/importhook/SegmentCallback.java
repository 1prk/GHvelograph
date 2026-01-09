package org.radsim.ghwrapper.importhook;

import java.util.List;

/**
 * Callback interface for capturing segment metadata during OSM import.
 * This is invoked BEFORE the edge is actually created in GraphHopper's BaseGraph,
 * allowing us to capture the original OSM node IDs for each segment.
 */
@FunctionalInterface
public interface SegmentCallback {
    /**
     * Called for each segment before it becomes an edge in the routing graph.
     *
     * @param baseWayId    the original OSM way ID that this segment belongs to
     * @param osmNodeIds   the list of original OSM node IDs for this segment (tower + pillar nodes)
     * @param segIndex     the ordinal index of this segment within the base way (0-based, in base-way order)
     * @param isBarrierEdge whether this segment is a barrier edge (artificial segment created at barriers)
     */
    void onSegment(long baseWayId, List<Long> osmNodeIds, int segIndex, boolean isBarrierEdge);
}
