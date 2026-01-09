package org.radsim.ghwrapper.osm;

import org.radsim.ghwrapper.store.SegmentRecord;
import org.radsim.ghwrapper.store.SegmentStoreReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.*;

/**
 * Rewrites route relations by expanding way members into segment members.
 *
 * For each route relation:
 * - Iterate members in original order
 * - If member is WAY with ref=baseWayId:
 *   - Replace with list of segment ways for that baseWayId
 *   - Segments ordered by segIndex
 *   - Default filter: exclude isBarrierEdge=true
 *   - Apply same role to all inserted segment members
 * - Keep NODE/RELATION members unchanged
 */
public class RouteRelationRewriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(RouteRelationRewriter.class);

    private final Map<Long, List<SegmentRecord>> wayIdToSegments;
    private final boolean excludeBarrierEdges;

    /**
     * Creates a rewriter with segment store data.
     *
     * @param segmentStorePath path to segment store file
     * @param excludeBarrierEdges whether to exclude barrier edges (default: true)
     */
    public RouteRelationRewriter(Path segmentStorePath, boolean excludeBarrierEdges) throws Exception {
        this.excludeBarrierEdges = excludeBarrierEdges;
        this.wayIdToSegments = new HashMap<>();

        LOGGER.info("Loading segment store for relation rewriting...");
        try (SegmentStoreReader reader = new SegmentStoreReader(segmentStorePath)) {
            for (SegmentRecord record : reader) {
                // Skip barrier edges if filtering enabled
                if (excludeBarrierEdges && record.isBarrierEdge()) {
                    continue;
                }

                wayIdToSegments.computeIfAbsent(record.getBaseWayId(), k -> new ArrayList<>())
                    .add(record);
            }
        }

        // Sort segments by segIndex for each way
        for (List<SegmentRecord> segments : wayIdToSegments.values()) {
            segments.sort(Comparator.comparingInt(SegmentRecord::getSegIndex));
        }

        LOGGER.info("Loaded {} base ways with segments", wayIdToSegments.size());
    }

    /**
     * Rewrites a single route relation.
     *
     * @param relation the original relation
     * @return the rewritten relation with segment members
     */
    public OsmRelation rewrite(OsmRelation relation) {
        List<OsmRelation.Member> rewrittenMembers = new ArrayList<>();
        int waysExpanded = 0;
        int segmentsAdded = 0;

        for (OsmRelation.Member member : relation.getMembers()) {
            if (member.getType() == OsmRelation.MemberType.WAY) {
                long baseWayId = member.getRef();
                List<SegmentRecord> segments = wayIdToSegments.get(baseWayId);

                if (segments != null && !segments.isEmpty()) {
                    // Replace with segment members
                    for (SegmentRecord segment : segments) {
                        rewrittenMembers.add(new OsmRelation.Member(
                            OsmRelation.MemberType.WAY,
                            segment.getGhEdgeId(),
                            member.getRole()  // Preserve original role
                        ));
                    }
                    waysExpanded++;
                    segmentsAdded += segments.size();
                } else {
                    // No segments found for this way - keep original member
                    // This can happen if the way was not processed by GraphHopper
                    rewrittenMembers.add(member);
                }
            } else {
                // Keep NODE and RELATION members unchanged
                rewrittenMembers.add(member);
            }
        }

        if (waysExpanded > 0) {
            LOGGER.debug("Relation {}: expanded {} ways into {} segments",
                relation.getId(), waysExpanded, segmentsAdded);
        }

        return new OsmRelation(relation.getId(), relation.getTags(), rewrittenMembers);
    }

    /**
     * Rewrites a list of route relations.
     *
     * @param relations the original relations
     * @return the rewritten relations
     */
    public List<OsmRelation> rewriteAll(List<OsmRelation> relations) {
        LOGGER.info("Rewriting {} route relations...", relations.size());
        List<OsmRelation> rewritten = new ArrayList<>();

        for (OsmRelation relation : relations) {
            rewritten.add(rewrite(relation));
        }

        LOGGER.info("Rewriting complete");
        return rewritten;
    }
}
