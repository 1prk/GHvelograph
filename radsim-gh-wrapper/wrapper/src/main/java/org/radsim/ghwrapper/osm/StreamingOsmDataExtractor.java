package org.radsim.ghwrapper.osm;

import com.graphhopper.reader.ReaderElement;
import com.graphhopper.reader.ReaderNode;
import com.graphhopper.reader.ReaderRelation;
import com.graphhopper.reader.ReaderWay;
import com.graphhopper.reader.osm.OSMInput;
import com.graphhopper.reader.osm.OSMInputFile;
import org.radsim.ghwrapper.store.SegmentRecord;
import org.radsim.ghwrapper.store.SegmentStoreReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Streaming OSM data extractor that avoids loading all IDs into memory.
 *
 * Strategy:
 * 1. Extract needed IDs from segment store and write to sorted files on disk
 * 2. Load sorted ID arrays (much more compact than HashSets)
 * 3. Stream through OSM file using binary search for membership testing
 *
 * Memory usage:
 * - Sorted long[]: 8 bytes per ID
 * - 100M nodes: ~800 MB (vs ~3-4 GB for LongOpenHashSet)
 * - 10M ways: ~80 MB (vs ~300-400 MB for LongOpenHashSet)
 *
 * For extremely large datasets, can use disk-based sorted files with binary search.
 */
public class StreamingOsmDataExtractor {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamingOsmDataExtractor.class);

    /**
     * Extracts OSM data using optimized binary caches and streaming.
     *
     * @param osmFile path to input OSM PBF file
     * @param segmentStorePath path to segment store file
     * @param cacheDir output directory for cache files
     * @param buildDictionary whether to build tag dictionary (slower but better compression)
     * @return extraction statistics
     */
    public static OsmDataExtractor.ExtractionStats extract(
            Path osmFile,
            Path segmentStorePath,
            Path cacheDir,
            boolean buildDictionary) throws Exception {

        LOGGER.info("Starting streaming OSM data extraction");
        LOGGER.info("  OSM file: {}", osmFile);
        LOGGER.info("  Segment store: {}", segmentStorePath);
        LOGGER.info("  Cache directory: {}", cacheDir);
        LOGGER.info("  Build dictionary: {}", buildDictionary);

        // Step 1: Extract needed IDs from segment store
        LOGGER.info("Step 1: Extracting needed IDs from segment store...");
        IdSets idSets = extractNeededIds(segmentStorePath);

        LOGGER.info("  Needed nodes: {}", idSets.nodeIds.length);
        LOGGER.info("  Needed ways: {}", idSets.wayIds.length);
        LOGGER.info("  Memory usage: ~{} MB",
            (idSets.nodeIds.length * 8 + idSets.wayIds.length * 8) / 1024 / 1024);

        Files.createDirectories(cacheDir);

        Path nodeCachePath = cacheDir.resolve("nodes.bin");
        Path wayTagCachePath = cacheDir.resolve("way_tags.bin");
        Path relationCachePath = cacheDir.resolve("relations.txt");

        // Step 2: Optional dictionary building pass
        CompressedWayTagCache wayTagCache = new CompressedWayTagCache(wayTagCachePath);

        if (buildDictionary) {
            LOGGER.info("Step 2: Building tag dictionary (first pass through OSM file)...");
            buildTagDictionary(osmFile, idSets.wayIds, wayTagCache);
        }

        // Step 3: Main extraction pass
        LOGGER.info("Step 3: Extracting data from OSM file...");

        BinaryNodeCache nodeCache = new BinaryNodeCache(nodeCachePath);
        RelationCache relationCache = new RelationCache(relationCachePath);

        nodeCache.openForWrite();
        wayTagCache.openForWrite();
        relationCache.openForWrite();

        ExtractionHandler handler = new ExtractionHandler(
            idSets.nodeIds, idSets.wayIds, nodeCache, wayTagCache, relationCache
        );

        // Parse OSM file
        OSMInput osmInput = new OSMInputFile(osmFile.toFile()).setWorkerThreads(2).open();
        ReaderElement element;
        while ((element = osmInput.getNext()) != null) {
            handler.handleElement(element);
        }
        osmInput.close();

        nodeCache.finishWrite();
        wayTagCache.finishWrite();
        relationCache.close();

        LOGGER.info("Extraction complete");
        LOGGER.info("  Nodes extracted: {}", handler.nodesExtracted);
        LOGGER.info("  Ways extracted: {}", handler.waysExtracted);
        LOGGER.info("  Relations extracted: {}", handler.relationsExtracted);

        return new OsmDataExtractor.ExtractionStats(
            handler.nodesExtracted,
            handler.waysExtracted,
            handler.relationsExtracted
        );
    }

    /**
     * Extracts needed IDs from segment store into sorted arrays.
     * Uses disk-based approach to avoid memory issues with large datasets.
     */
    private static IdSets extractNeededIds(Path segmentStorePath) throws IOException {
        Path tempDir = Files.createTempDirectory("rseg-ids");
        Path nodeIdsFile = tempDir.resolve("node_ids.bin");
        Path wayIdsFile = tempDir.resolve("way_ids.bin");

        try {
            // Step 1: Stream through segment store and write IDs to temporary files
            LOGGER.info("  Writing IDs to temporary files...");
            try (DataOutputStream nodeOut = new DataOutputStream(
                    new BufferedOutputStream(new FileOutputStream(nodeIdsFile.toFile())));
                 DataOutputStream wayOut = new DataOutputStream(
                    new BufferedOutputStream(new FileOutputStream(wayIdsFile.toFile())));
                 SegmentStoreReader reader = new SegmentStoreReader(segmentStorePath)) {

                int recordCount = 0;
                for (SegmentRecord record : reader) {
                    wayOut.writeLong(record.getBaseWayId());
                    for (long nodeRef : record.getNodeRefs()) {
                        nodeOut.writeLong(nodeRef);
                    }

                    if (++recordCount % 1_000_000 == 0) {
                        LOGGER.info("    Processed {} segment records...", recordCount);
                    }
                }
            }

            // Step 2: Sort and deduplicate IDs
            LOGGER.info("  Sorting and deduplicating node IDs...");
            long[] nodeIds = sortAndDeduplicateIds(nodeIdsFile);
            LOGGER.info("  Sorting and deduplicating way IDs...");
            long[] wayIds = sortAndDeduplicateIds(wayIdsFile);

            return new IdSets(nodeIds, wayIds);
        } finally {
            // Clean up temporary files
            try {
                Files.deleteIfExists(nodeIdsFile);
                Files.deleteIfExists(wayIdsFile);
                Files.deleteIfExists(tempDir);
            } catch (IOException e) {
                LOGGER.warn("Failed to clean up temporary files", e);
            }
        }
    }

    /**
     * Reads IDs from a binary file, sorts them, and removes duplicates using external sort.
     * Processes data in chunks to avoid loading everything into memory.
     */
    private static long[] sortAndDeduplicateIds(Path idsFile) throws IOException {
        long fileSize = Files.size(idsFile);
        long totalIdCount = fileSize / 8; // 8 bytes per long

        LOGGER.info("    Total IDs in file: {}", totalIdCount);

        // Chunk size: 10M IDs = 80 MB per chunk (safe for most heap sizes)
        int CHUNK_SIZE = 10_000_000;

        // Step 1: Sort chunks and write to temporary files
        List<Path> sortedChunks = new ArrayList<>();
        Path tempDir = idsFile.getParent();

        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(new FileInputStream(idsFile.toFile()), 8 * 1024 * 1024))) {

            int chunkIndex = 0;
            long idsProcessed = 0;

            while (idsProcessed < totalIdCount) {
                // Read chunk
                int chunkSize = (int) Math.min(CHUNK_SIZE, totalIdCount - idsProcessed);
                long[] chunk = new long[chunkSize];

                for (int i = 0; i < chunkSize; i++) {
                    chunk[i] = in.readLong();
                }

                // Sort chunk
                Arrays.sort(chunk);

                // Write sorted chunk to temp file
                Path chunkFile = tempDir.resolve("chunk_" + chunkIndex + ".bin");
                try (DataOutputStream out = new DataOutputStream(
                        new BufferedOutputStream(new FileOutputStream(chunkFile.toFile())))) {
                    for (long id : chunk) {
                        out.writeLong(id);
                    }
                }

                sortedChunks.add(chunkFile);
                idsProcessed += chunkSize;
                chunkIndex++;

                LOGGER.info("    Sorted chunk {}/{} ({} IDs)", chunkIndex,
                        (totalIdCount + CHUNK_SIZE - 1) / CHUNK_SIZE, chunkSize);
            }
        }

        // Step 2: Merge sorted chunks and deduplicate
        LOGGER.info("    Merging {} sorted chunks...", sortedChunks.size());

        List<Long> uniqueIds = new ArrayList<>();
        PriorityQueue<ChunkReader> pq = new PriorityQueue<>(
                Comparator.comparingLong(r -> r.currentValue));

        // Open all chunk files
        try {
            for (Path chunkFile : sortedChunks) {
                ChunkReader reader = new ChunkReader(chunkFile);
                if (reader.hasNext()) {
                    reader.next();
                    pq.add(reader);
                }
            }

            // Merge with deduplication
            long lastId = Long.MIN_VALUE;
            while (!pq.isEmpty()) {
                ChunkReader reader = pq.poll();
                long currentId = reader.currentValue;

                // Only add if different from last (deduplication)
                if (currentId != lastId) {
                    uniqueIds.add(currentId);
                    lastId = currentId;
                }

                // Advance reader and re-add to queue if more data
                if (reader.hasNext()) {
                    reader.next();
                    pq.add(reader);
                } else {
                    reader.close();
                }

                // Progress reporting
                if (uniqueIds.size() % 1_000_000 == 0) {
                    LOGGER.info("      Merged {} unique IDs...", uniqueIds.size());
                }
            }
        } finally {
            // Ensure all readers are closed
            while (!pq.isEmpty()) {
                pq.poll().close();
            }

            // Clean up chunk files
            for (Path chunkFile : sortedChunks) {
                try {
                    Files.deleteIfExists(chunkFile);
                } catch (IOException e) {
                    LOGGER.warn("Failed to delete chunk file: {}", chunkFile, e);
                }
            }
        }

        LOGGER.info("    Unique IDs after deduplication: {}", uniqueIds.size());

        // Convert to array
        long[] result = new long[uniqueIds.size()];
        for (int i = 0; i < uniqueIds.size(); i++) {
            result[i] = uniqueIds.get(i);
        }

        return result;
    }

    /**
     * Reader for a sorted chunk file.
     */
    private static class ChunkReader implements Closeable {
        private final DataInputStream in;
        private boolean hasNext;
        long currentValue;

        ChunkReader(Path file) throws IOException {
            this.in = new DataInputStream(
                    new BufferedInputStream(new FileInputStream(file.toFile())));
            this.hasNext = true;
        }

        boolean hasNext() {
            return hasNext;
        }

        void next() throws IOException {
            try {
                currentValue = in.readLong();
            } catch (EOFException e) {
                hasNext = false;
            }
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }

    /**
     * Builds tag dictionary from a sample of ways.
     */
    private static void buildTagDictionary(Path osmFile, long[] neededWayIds,
                                          CompressedWayTagCache wayTagCache) throws Exception {
        List<Map.Entry<Long, Map<String, String>>> sample = new ArrayList<>();
        int sampleSize = Math.min(100_000, neededWayIds.length);

        OSMInput osmInput = new OSMInputFile(osmFile.toFile()).setWorkerThreads(2).open();
        ReaderElement element;
        int sampled = 0;

        while ((element = osmInput.getNext()) != null && sampled < sampleSize) {
            if (element.getType() == ReaderElement.Type.WAY) {
                ReaderWay way = (ReaderWay) element;
                if (Arrays.binarySearch(neededWayIds, way.getId()) >= 0) {
                    Map<String, String> tags = new HashMap<>();
                    way.getTags().forEach((k, v) -> tags.put(k.toString(), v.toString()));
                    sample.add(new AbstractMap.SimpleEntry<>(way.getId(), tags));
                    sampled++;
                }
            }
        }
        osmInput.close();

        wayTagCache.buildDictionary(sample.iterator(), sample.size());
    }

    /**
     * Handler for processing OSM elements during extraction.
     */
    private static class ExtractionHandler {
        private final long[] neededNodeIds;
        private final long[] neededWayIds;
        private final BinaryNodeCache nodeCache;
        private final CompressedWayTagCache wayTagCache;
        private final RelationCache relationCache;

        int nodesExtracted = 0;
        int waysExtracted = 0;
        int relationsExtracted = 0;

        ExtractionHandler(long[] neededNodeIds, long[] neededWayIds,
                         BinaryNodeCache nodeCache, CompressedWayTagCache wayTagCache,
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
                    break;
            }
        }

        void handleNode(ReaderNode node) throws IOException {
            // Binary search in sorted array
            if (Arrays.binarySearch(neededNodeIds, node.getId()) >= 0) {
                double ele = Double.NaN;
                Object eleTag = node.getTag("ele");
                if (eleTag != null) {
                    try {
                        ele = Double.parseDouble(eleTag.toString());
                    } catch (NumberFormatException e) {
                        // Ignore
                    }
                }

                OsmNode osmNode = new OsmNode(node.getId(), node.getLat(), node.getLon(), ele);
                nodeCache.put(osmNode);
                nodesExtracted++;

                if (nodesExtracted % 1_000_000 == 0) {
                    LOGGER.info("  Extracted {} nodes...", nodesExtracted);
                }
            }
        }

        private static final Set<String> WHITELIST = Set.of(
            "highway", "name", "ref", "surface", "maxspeed", "oneway", "bicycle", "foot",
            "lanes", "cycleway", "sidewalk", "lit", "access"
        );

        void handleWay(ReaderWay way) throws IOException {
            if (Arrays.binarySearch(neededWayIds, way.getId()) >= 0) {
                Map<String, String> tags = new HashMap<>();
                way.getTags().forEach((k, v) -> {
                    String key = k.toString();
                    if (WHITELIST.contains(key)) {
                        tags.put(key, v.toString());
                    }
                });
                wayTagCache.put(way.getId(), tags);
                waysExtracted++;

                if (waysExtracted % 100_000 == 0) {
                    LOGGER.info("  Extracted {} ways...", waysExtracted);
                }
            }
        }

        void handleRelation(ReaderRelation relation) throws IOException {
            Object typeTag = relation.getTag("type");
            if (typeTag != null) {
                String type = typeTag.toString();
                if ("route".equals(type) || "route_master".equals(type)) {
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
                                continue;
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
     * Holds sorted ID arrays for efficient binary search.
     */
    private static class IdSets {
        final long[] nodeIds;
        final long[] wayIds;

        IdSets(long[] nodeIds, long[] wayIds) {
            this.nodeIds = nodeIds;
            this.wayIds = wayIds;
        }
    }
}
