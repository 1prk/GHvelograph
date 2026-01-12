package org.radsim.ghwrapper.cli;

import org.radsim.ghwrapper.osm.OsmDataExtractor;
import org.radsim.ghwrapper.osm.StreamingOsmDataExtractor;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.util.concurrent.Callable;

@Command(
    name = "extract-osm",
    description = "Extract needed OSM data (nodes, way tags, relations) from PBF file based on segment store",
    mixinStandardHelpOptions = true
)
public class ExtractOsmCommand implements Callable<Integer> {

    @Option(
        names = {"--osm"},
        required = true,
        description = "Input OSM PBF file path"
    )
    private File osmFile;

    @Option(
        names = {"--segments"},
        required = true,
        description = "Input segment store file path (*.rseg)"
    )
    private File segmentStoreFile;

    @Option(
        names = {"--out", "-o"},
        required = true,
        description = "Output cache directory"
    )
    private File cacheDir;

    @Option(
        names = {"--optimized"},
        description = "Use optimized binary formats and streaming (recommended for large datasets)"
    )
    private boolean optimized = false;

    @Option(
        names = {"--build-dictionary"},
        description = "Build tag dictionary for compression (slower but saves more space, only with --optimized)"
    )
    private boolean buildDictionary = false;

    @Option(
        names = {"--force", "-f"},
        description = "Force re-extraction even if cache already exists"
    )
    private boolean force = false;

    @Override
    public Integer call() throws Exception {
        // Validate input files exist
        if (!osmFile.exists()) {
            System.err.println("Error: OSM file does not exist: " + osmFile);
            return 1;
        }

        if (!osmFile.isFile()) {
            System.err.println("Error: OSM path is not a file: " + osmFile);
            return 1;
        }

        if (!segmentStoreFile.exists()) {
            System.err.println("Error: Segment store file does not exist: " + segmentStoreFile);
            return 1;
        }

        if (!segmentStoreFile.isFile()) {
            System.err.println("Error: Segment store path is not a file: " + segmentStoreFile);
            return 1;
        }

        // Create cache directory if needed
        if (!cacheDir.exists()) {
            if (!cacheDir.mkdirs()) {
                System.err.println("Error: Failed to create cache directory: " + cacheDir);
                return 1;
            }
        }

        // Check if cache already exists
        boolean cacheExists = checkCacheExists(cacheDir, optimized);
        if (cacheExists && !force) {
            System.out.println("Cache already exists: " + cacheDir);
            System.out.println("Skipping extraction. Use --force to re-extract.");
            System.out.println();

            // Report existing cache info
            reportCacheInfo(cacheDir, optimized);

            return 0;
        }

        // Run the extraction
        if (force && cacheExists) {
            System.out.println("Forcing re-extraction (existing cache will be overwritten)");
        }
        System.out.println("Extracting OSM data from PBF file...");
        if (optimized) {
            System.out.println("Using optimized binary formats and streaming extraction");
            if (buildDictionary) {
                System.out.println("Building tag dictionary (this will take longer but save more space)");
            }
        } else {
            System.out.println("Using legacy text format (consider --optimized for large datasets)");
        }
        System.out.println();

        OsmDataExtractor.ExtractionStats stats;

        if (optimized) {
            stats = StreamingOsmDataExtractor.extract(
                osmFile.toPath(),
                segmentStoreFile.toPath(),
                cacheDir.toPath(),
                buildDictionary
            );
        } else {
            stats = OsmDataExtractor.extract(
                osmFile.toPath(),
                segmentStoreFile.toPath(),
                cacheDir.toPath()
            );
        }

        System.out.println();
        System.out.println("Success! Extraction complete:");
        System.out.println("  Nodes extracted: " + stats.nodesExtracted);
        System.out.println("  Ways extracted: " + stats.waysExtracted);
        System.out.println("  Relations extracted: " + stats.relationsExtracted);
        System.out.println("Output written to: " + cacheDir);

        return 0;
    }

    /**
     * Checks if cache files already exist.
     */
    private boolean checkCacheExists(File cacheDir, boolean optimized) {
        if (optimized) {
            // Binary format
            File nodeCache = new File(cacheDir, "nodes.bin");
            File wayCache = new File(cacheDir, "way_tags.bin");
            File relationCache = new File(cacheDir, "relations.txt");
            return nodeCache.exists() && wayCache.exists() && relationCache.exists();
        } else {
            // Legacy text format
            File nodeCache = new File(cacheDir, "nodes.txt");
            File wayCache = new File(cacheDir, "way_tags.txt");
            File relationCache = new File(cacheDir, "relations.txt");
            return nodeCache.exists() && wayCache.exists() && relationCache.exists();
        }
    }

    /**
     * Reports information about existing cache files.
     */
    private void reportCacheInfo(File cacheDir, boolean optimized) {
        System.out.println("Existing cache info:");
        System.out.println("  Directory: " + cacheDir);

        if (optimized) {
            File nodeCache = new File(cacheDir, "nodes.bin");
            File wayCache = new File(cacheDir, "way_tags.bin");
            File relationCache = new File(cacheDir, "relations.txt");

            if (nodeCache.exists()) {
                System.out.println("  Node cache: " + (nodeCache.length() / 1024 / 1024) + " MB");
            }
            if (wayCache.exists()) {
                System.out.println("  Way tag cache: " + (wayCache.length() / 1024 / 1024) + " MB");
            }
            if (relationCache.exists()) {
                System.out.println("  Relations: " + (relationCache.length() / 1024) + " KB");
            }
        } else {
            File nodeCache = new File(cacheDir, "nodes.txt");
            File wayCache = new File(cacheDir, "way_tags.txt");
            File relationCache = new File(cacheDir, "relations.txt");

            if (nodeCache.exists()) {
                System.out.println("  Node cache: " + (nodeCache.length() / 1024 / 1024) + " MB");
            }
            if (wayCache.exists()) {
                System.out.println("  Way tag cache: " + (wayCache.length() / 1024 / 1024) + " MB");
            }
            if (relationCache.exists()) {
                System.out.println("  Relations: " + (relationCache.length() / 1024) + " KB");
            }
        }
    }
}
