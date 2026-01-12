package org.radsim.ghwrapper.osm;

import com.graphhopper.reader.ReaderElement;
import com.graphhopper.reader.ReaderNode;
import com.graphhopper.reader.ReaderRelation;
import com.graphhopper.reader.ReaderWay;
import com.graphhopper.reader.osm.OSMFileHeader;
import com.graphhopper.reader.osm.OSMInput;
import com.graphhopper.reader.osm.OSMInputFile;
import org.radsim.ghwrapper.store.SegmentRecord;
import org.radsim.ghwrapper.store.SegmentStoreReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.*;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

/**
 * Extracts needed OSM data from PBF file based on segment store.
 *
 * Two-pass extraction:
 * 1. Read segment store to collect needed node IDs and way IDs
 * 2. Parse PBF and extract only the needed elements
 */
public class OsmDataExtractor {
    private static final Logger LOGGER = LoggerFactory.getLogger(OsmDataExtractor.class);

    /**
     * Extracts OSM data and writes to cache directory.
     *
     * @param osmFile path to input OSM PBF file
     * @param segmentStorePath path to segment store file
     * @param cacheDir output directory for cache files
     * @return extraction statistics
     */
    public static ExtractionStats extract(Path osmFile, Path segmentStorePath, Path cacheDir) throws Exception {
        LOGGER.info("Starting OSM data extraction");
        LOGGER.info("  OSM file: {}", osmFile);
        LOGGER.info("  Segment store: {}", segmentStorePath);
        LOGGER.info("  Cache directory: {}", cacheDir);

        // Pass 1: Read segment store to collect needed IDs
        LOGGER.info("Pass 1: Reading segment store to identify needed IDs...");
        LongOpenHashSet neededNodeIds = new LongOpenHashSet();
        LongOpenHashSet neededWayIds = new LongOpenHashSet();

        try (SegmentStoreReader reader = new SegmentStoreReader(segmentStorePath)) {
            for (SegmentRecord record : reader) {
                neededWayIds.add(record.getBaseWayId());
                for (long nodeRef : record.getNodeRefs()) {
                    neededNodeIds.add(nodeRef);
                }
            }
        }

        LOGGER.info("  Needed node IDs: {}", neededNodeIds.size());
        LOGGER.info("  Needed way IDs: {}", neededWayIds.size());

        // Pass 2: Parse OSM file and extract needed data
        LOGGER.info("Pass 2: Extracting data from OSM file...");

        Path nodeCachePath = cacheDir.resolve("nodes.txt");
        Path wayTagCachePath = cacheDir.resolve("way_tags.txt");
        Path relationCachePath = cacheDir.resolve("relations.txt");

        try (NodeCache nodeCache = new NodeCache(nodeCachePath);
             WayTagCache wayTagCache = new WayTagCache(wayTagCachePath);
             RelationCache relationCache = new RelationCache(relationCachePath)) {

            nodeCache.openForWrite();
            wayTagCache.openForWrite();
            relationCache.openForWrite();

            ExtractionHandler handler = new ExtractionHandler(
                neededNodeIds, neededWayIds, nodeCache, wayTagCache, relationCache
            );

            // Parse OSM file
            OSMInput osmInput = new OSMInputFile(osmFile.toFile()).setWorkerThreads(2).open();
            ReaderElement element;
            while ((element = osmInput.getNext()) != null) {
                handler.handleElement(element);
            }
            osmInput.close();

            LOGGER.info("Extraction complete");
            LOGGER.info("  Nodes extracted: {}", handler.nodesExtracted);
            LOGGER.info("  Ways extracted: {}", handler.waysExtracted);
            LOGGER.info("  Relations extracted: {}", handler.relationsExtracted);

            return new ExtractionStats(
                handler.nodesExtracted,
                handler.waysExtracted,
                handler.relationsExtracted
            );
        }
    }

    /**
     * Handler for processing OSM elements during extraction.
     */
    private static class ExtractionHandler {
        private final Set<Long> neededNodeIds;
        private final Set<Long> neededWayIds;
        private final NodeCache nodeCache;
        private final WayTagCache wayTagCache;
        private final RelationCache relationCache;

        int nodesExtracted = 0;
        int waysExtracted = 0;
        int relationsExtracted = 0;

        ExtractionHandler(Set<Long> neededNodeIds, Set<Long> neededWayIds,
                         NodeCache nodeCache, WayTagCache wayTagCache,
                         RelationCache relationCache) {
            this.neededNodeIds = neededNodeIds;
            this.neededWayIds = neededWayIds;
            this.nodeCache = nodeCache;
            this.wayTagCache = wayTagCache;
            this.relationCache = relationCache;
        }

        void handleElement(ReaderElement element) throws Exception {
            switch (element.getType()) {
                case NODE:
                    handleNode((ReaderNode) element);
                    break;
                case WAY:
                    handleWay((ReaderWay) element);
                    break;
                case RELATION:
                    handleRelation((ReaderRelation) element);
                    break;
                case FILEHEADER:
                    // Ignore file header
                    break;
            }
        }

        void handleNode(ReaderNode node) throws IOException {
            if (neededNodeIds.contains(node.getId())) {
                // Elevation might be in tags (ele tag) or not available - default to NaN
                double ele = Double.NaN;
                Object eleTag = node.getTag("ele");
                if (eleTag != null) {
                    try {
                        ele = Double.parseDouble(eleTag.toString());
                    } catch (NumberFormatException e) {
                        // Ignore invalid elevation values
                    }
                }

                OsmNode osmNode = new OsmNode(node.getId(), node.getLat(), node.getLon(), ele);
                nodeCache.put(osmNode);
                nodesExtracted++;

                if (nodesExtracted % 100_000 == 0) {
                    LOGGER.info("  Extracted {} nodes so far...", nodesExtracted);
                }
            }
        }

        private static final Set<String> WHITELIST = Set.of(
            "highway", "name", "ref", "surface", "maxspeed", "oneway", "bicycle", "foot"
        );

        void handleWay(ReaderWay way) throws IOException {
            if (neededWayIds.contains(way.getId())) {
                Map<String, String> tags = new HashMap<>();
                way.getTags().forEach((k, v) -> {
                    String key = k.toString();
                    if (WHITELIST.contains(key)) {
                        tags.put(key, v.toString());
                    }
                });
                wayTagCache.put(way.getId(), tags);
                waysExtracted++;

                if (waysExtracted % 10_000 == 0) {
                    LOGGER.info("  Extracted {} ways so far...", waysExtracted);
                }
            }
        }

        void handleRelation(ReaderRelation relation) throws IOException {
            // Extract route relations (type=route or type=route_master)
            Object typeTag = relation.getTag("type");
            if (typeTag != null) {
                String type = typeTag.toString();
                if ("route".equals(type) || "route_master".equals(type)) {
                    // Convert to OsmRelation
                    Map<String, String> tags = new HashMap<>();
                    relation.getTags().forEach((k, v) -> tags.put(k.toString(), v.toString()));

                    List<OsmRelation.Member> members = new ArrayList<>();
                    for (ReaderRelation.Member member : relation.getMembers()) {
                        OsmRelation.MemberType memberType;
                        switch (member.getType()) {
                            case NODE:
                                memberType = OsmRelation.MemberType.NODE;
                                break;
                            case WAY:
                                memberType = OsmRelation.MemberType.WAY;
                                break;
                            case RELATION:
                                memberType = OsmRelation.MemberType.RELATION;
                                break;
                            default:
                                continue;  // Skip unknown types
                        }
                        members.add(new OsmRelation.Member(memberType, member.getRef(), member.getRole()));
                    }

                    OsmRelation osmRelation = new OsmRelation(relation.getId(), tags, members);
                    relationCache.put(osmRelation);
                    relationsExtracted++;
                }
            }
        }
    }

    /**
     * Statistics from extraction.
     */
    public static class ExtractionStats {
        public final int nodesExtracted;
        public final int waysExtracted;
        public final int relationsExtracted;

        public ExtractionStats(int nodesExtracted, int waysExtracted, int relationsExtracted) {
            this.nodesExtracted = nodesExtracted;
            this.waysExtracted = waysExtracted;
            this.relationsExtracted = relationsExtracted;
        }

        @Override
        public String toString() {
            return "ExtractionStats{nodes=" + nodesExtracted +
                   ", ways=" + waysExtracted +
                   ", relations=" + relationsExtracted + '}';
        }
    }
}
