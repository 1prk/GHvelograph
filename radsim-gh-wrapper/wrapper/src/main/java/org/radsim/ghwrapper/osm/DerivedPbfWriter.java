package org.radsim.ghwrapper.osm;

import crosby.binary.osmosis.OsmosisSerializer;
import org.openstreetmap.osmosis.core.container.v0_6.*;
import org.openstreetmap.osmosis.core.domain.v0_6.*;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.openstreetmap.osmosis.osmbinary.file.BlockOutputStream;
import org.radsim.ghwrapper.store.SegmentRecord;
import org.radsim.ghwrapper.store.SegmentStoreReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.util.*;

/**
 * Writes derived PBF files where each segment becomes a way.
 *
 * Output structure:
 * - Nodes: All nodes from node cache
 * - Ways: Each segment becomes a way with:
 *   - id = ghEdgeId
 *   - nodeRefs = SegmentRecord.nodeRefs
 *   - tags: base_id=<baseWayId> plus selected tags from base way
 * - Relations: Rewritten route relations with segment members
 */
public class DerivedPbfWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(DerivedPbfWriter.class);

    private final Path segmentStorePath;
    private final INodeCache nodeCache;
    private final IWayTagCache wayTagCache;
    private final List<OsmRelation> rewrittenRelations;
    private final boolean excludeBarrierEdges;

    /**
     * Creates a derived PBF writer.
     *
     * @param segmentStorePath path to segment store
     * @param nodeCache node cache (must be loaded)
     * @param wayTagCache way tag cache (must be loaded)
     * @param rewrittenRelations rewritten route relations
     * @param excludeBarrierEdges whether to exclude barrier edges
     */
    public DerivedPbfWriter(Path segmentStorePath,
                           INodeCache nodeCache,
                           IWayTagCache wayTagCache,
                           List<OsmRelation> rewrittenRelations,
                           boolean excludeBarrierEdges) {
        this.segmentStorePath = segmentStorePath;
        this.nodeCache = nodeCache;
        this.wayTagCache = wayTagCache;
        this.rewrittenRelations = rewrittenRelations;
        this.excludeBarrierEdges = excludeBarrierEdges;
    }

    /**
     * Writes the derived PBF file.
     *
     * @param outputFile path to output PBF file
     */
    public void write(Path outputFile) throws Exception {
        LOGGER.info("Writing derived PBF file: {}", outputFile);

        File file = outputFile.toFile();
        file.getParentFile().mkdirs();

        FileOutputStream fos = new FileOutputStream(file);
        BlockOutputStream pbfStream = new BlockOutputStream(fos);
        OsmosisSerializer serializer = new OsmosisSerializer(pbfStream);

        try {
            serializer.initialize(Collections.emptyMap());

            // Write nodes
            LOGGER.info("Writing nodes...");
            int nodesWritten = writeNodes(serializer);

            // Write ways (segments)
            LOGGER.info("Writing ways (segments)...");
            int waysWritten = writeWays(serializer);

            // Write relations
            LOGGER.info("Writing relations...");
            int relationsWritten = writeRelations(serializer);

            serializer.complete();

            LOGGER.info("Derived PBF writing complete:");
            LOGGER.info("  Nodes: {}", nodesWritten);
            LOGGER.info("  Ways: {}", waysWritten);
            LOGGER.info("  Relations: {}", relationsWritten);
        } finally {
            pbfStream.close();
            fos.close();
        }
    }

    private int writeNodes(Sink sink) {
        int count = 0;
        Set<Long> writtenNodeIds = new HashSet<>();

        // Get all node IDs we need from segment store (only from highway ways)
        Set<Long> neededNodeIds = new HashSet<>();
        try (SegmentStoreReader reader = new SegmentStoreReader(segmentStorePath)) {
            for (SegmentRecord record : reader) {
                if (excludeBarrierEdges && record.isBarrierEdge()) {
                    continue;
                }

                // Only include nodes from ways with highway tag
                Map<String, String> baseTags = wayTagCache.get(record.getBaseWayId());
                if (baseTags == null || !baseTags.containsKey("highway")) {
                    continue;
                }

                for (long nodeRef : record.getNodeRefs()) {
                    neededNodeIds.add(nodeRef);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error reading segment store", e);
            return 0;
        }

        // Write nodes in sorted order (OSM convention)
        List<Long> sortedNodeIds = new ArrayList<>(neededNodeIds);
        Collections.sort(sortedNodeIds);

        for (Long nodeId : sortedNodeIds) {
            OsmNode osmNode = nodeCache.get(nodeId);
            if (osmNode == null) {
                LOGGER.warn("Node {} not found in cache, skipping", nodeId);
                continue;
            }

            if (writtenNodeIds.contains(nodeId)) {
                continue;  // Skip duplicates
            }

            Node node = new Node(
                new CommonEntityData(
                    nodeId,
                    1,  // version
                    new Date(),
                    OsmUser.NONE,
                    1  // changeset
                ),
                osmNode.getLat(),
                osmNode.getLon()
            );

            sink.process(new NodeContainer(node));
            writtenNodeIds.add(nodeId);
            count++;

            if (count % 100_000 == 0) {
                LOGGER.info("  Written {} nodes...", count);
            }
        }

        return count;
    }

    private int writeWays(Sink sink) throws Exception {
        int count = 0;
        int skipped = 0;

        try (SegmentStoreReader reader = new SegmentStoreReader(segmentStorePath)) {
            for (SegmentRecord record : reader) {
                if (excludeBarrierEdges && record.isBarrierEdge()) {
                    continue;
                }

                // Only include ways with highway tag
                Map<String, String> baseTags = wayTagCache.get(record.getBaseWayId());
                if (baseTags == null || !baseTags.containsKey("highway")) {
                    skipped++;
                    continue;
                }

                // Create way from segment
                List<WayNode> wayNodes = new ArrayList<>();
                for (long nodeRef : record.getNodeRefs()) {
                    wayNodes.add(new WayNode(nodeRef));
                }

                // Build tags
                Collection<Tag> tags = new ArrayList<>();
                tags.add(new Tag("base_id", String.valueOf(record.getBaseWayId())));

                // Copy relevant tags (highway, name, surface, etc.)
                for (String key : Arrays.asList("highway", "name", "ref", "surface",
                                                "maxspeed", "oneway", "bicycle", "foot")) {
                    String value = baseTags.get(key);
                    if (value != null) {
                        tags.add(new Tag(key, value));
                    }
                }

                Way way = new Way(
                    new CommonEntityData(
                        record.getGhEdgeId(),
                        1,  // version
                        new Date(),
                        OsmUser.NONE,
                        1,  // changeset
                        tags
                    ),
                    wayNodes
                );

                sink.process(new WayContainer(way));
                count++;

                if (count % 10_000 == 0) {
                    LOGGER.info("  Written {} ways...", count);
                }
            }
        }

        if (skipped > 0) {
            LOGGER.info("  Skipped {} non-highway ways", skipped);
        }

        return count;
    }

    private int writeRelations(Sink sink) {
        int count = 0;

        for (OsmRelation osmRelation : rewrittenRelations) {
            List<RelationMember> members = new ArrayList<>();

            for (OsmRelation.Member member : osmRelation.getMembers()) {
                EntityType entityType;
                switch (member.getType()) {
                    case NODE:
                        entityType = EntityType.Node;
                        break;
                    case WAY:
                        entityType = EntityType.Way;
                        break;
                    case RELATION:
                        entityType = EntityType.Relation;
                        break;
                    default:
                        continue;
                }

                members.add(new RelationMember(
                    member.getRef(),
                    entityType,
                    member.getRole()
                ));
            }

            Collection<Tag> tags = new ArrayList<>();
            for (Map.Entry<String, String> tag : osmRelation.getTags().entrySet()) {
                tags.add(new Tag(tag.getKey(), tag.getValue()));
            }

            Relation relation = new Relation(
                new CommonEntityData(
                    osmRelation.getId(),
                    1,  // version
                    new Date(),
                    OsmUser.NONE,
                    1,  // changeset
                    tags
                ),
                members
            );

            sink.process(new RelationContainer(relation));
            count++;
        }

        return count;
    }
}
