package org.radsim.ghwrapper.importhook;

import com.graphhopper.reader.osm.WaySegmentParserWithCallback;
import com.graphhopper.storage.BaseGraph;
import com.graphhopper.storage.Directory;
import com.graphhopper.storage.MMapDirectory;
import com.graphhopper.util.PointAccess;
import org.radsim.ghwrapper.store.SegmentStoreWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Orchestrates OSM import with segment capture.
 *
 * Uses WaySegmentParserWithCallback to parse OSM files and captures segment metadata
 * (base way ID, OSM node IDs, segment index, barrier flag) via SegmentCapturingEdgeHandler.
 *
 * This does NOT create a full GraphHopper routing graph - it only parses the OSM data
 * to extract segment information needed for later processing.
 */
public class SegmentCaptureImporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentCaptureImporter.class);

    /**
     * Runs the import and captures segments to the specified output file.
     *
     * @param osmFile path to the input OSM PBF file
     * @param segmentStorePath path to the output segment store file
     * @return the number of segments captured
     * @throws IOException if an I/O error occurs
     */
    public static int runImport(Path osmFile, Path segmentStorePath) throws IOException {
        LOGGER.info("Starting segment capture import");
        LOGGER.info("  Input OSM file: {}", osmFile);
        LOGGER.info("  Output segment store: {}", segmentStorePath);

        // Create temporary directory for memory-mapped graph storage
        Path tempDir = Files.createTempDirectory("gh-segment-capture-");
        LOGGER.info("  Temp graph cache: {}", tempDir);

        try {
            // Create segment store writer
            try (SegmentStoreWriter writer = new SegmentStoreWriter(segmentStorePath)) {

                // Create the capturing edge handler
                SegmentCapturingEdgeHandler handler = new SegmentCapturingEdgeHandler(writer);

                // Use memory-mapped directory for better performance with large datasets
                // MMapDirectory uses OS memory management instead of heap
                Directory directory = new MMapDirectory(tempDir.toString());

                // Create a minimal BaseGraph to get PointAccess - GH 11.0 API
                // Pre-size to avoid resizing overhead (estimate based on typical OSM files)
                BaseGraph baseGraph = new BaseGraph.Builder(100_000)
                        .set3D(false)
                        .setDir(directory)
                        .create();
                PointAccess pointAccess = baseGraph.getNodeAccess();

                // Build the way segment parser with our custom callback
                // Use single worker thread for large datasets to minimize memory usage
                WaySegmentParserWithCallback parser = new WaySegmentParserWithCallback.Builder(pointAccess, directory)
                        .setSegmentCallback(handler)  // Called BEFORE edge creation with OSM node IDs
                        .setEdgeHandler(handler)       // Called DURING edge creation to assign IDs
                        .setWorkerThreads(1)           // Single thread reduces memory pressure
                        .build();

                // Run the OSM import
                LOGGER.info("Reading OSM file and capturing segments...");
                parser.readOSM(osmFile.toFile());

                // Verify everything was captured correctly
                handler.verifyComplete();

                int segmentCount = handler.getSegmentsCaptured();
                LOGGER.info("Segment capture complete: {} segments captured", segmentCount);

                // Clean up graph resources
                baseGraph.close();
                directory.close();

                return segmentCount;
            }
        } finally {
            // Clean up temporary directory and all files
            deleteRecursively(tempDir);
            LOGGER.info("Cleaned up temporary graph cache");
        }
    }

    /**
     * Convenience method that takes string paths.
     */
    public static int runImport(String osmFile, String segmentStorePath) throws IOException {
        return runImport(Path.of(osmFile), Path.of(segmentStorePath));
    }

    /**
     * Recursively deletes a directory and all its contents.
     */
    private static void deleteRecursively(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return;
        }

        Files.walk(directory)
                .sorted((a, b) -> -a.compareTo(b))  // Reverse order to delete files before directories
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        LOGGER.warn("Failed to delete temporary file: {}", path, e);
                    }
                });
    }
}
