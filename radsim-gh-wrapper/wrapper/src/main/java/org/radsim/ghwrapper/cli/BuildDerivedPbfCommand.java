package org.radsim.ghwrapper.cli;

import org.radsim.ghwrapper.osm.*;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.util.List;
import java.util.concurrent.Callable;

@Command(
    name = "build-derived-pbf",
    description = "Build derived PBF file with segment ways and rewritten route relations",
    mixinStandardHelpOptions = true
)
public class BuildDerivedPbfCommand implements Callable<Integer> {

    @Option(
        names = {"--segments"},
        required = true,
        description = "Input segment store file path (*.rseg)"
    )
    private File segmentStoreFile;

    @Option(
        names = {"--cache"},
        required = true,
        description = "Input cache directory (from extract-osm command)"
    )
    private File cacheDir;

    @Option(
        names = {"--out", "-o"},
        required = true,
        description = "Output derived PBF file path"
    )
    private File outputFile;

    @Option(
        names = {"--include-barrier-edges"},
        description = "Include barrier edges in output (default: false)"
    )
    private boolean includeBarrierEdges = false;

    @Option(
        names = {"--force", "-f"},
        description = "Force rebuild even if derived PBF already exists"
    )
    private boolean force = false;

    @Override
    public Integer call() throws Exception {
        // Validate inputs
        if (!segmentStoreFile.exists()) {
            System.err.println("Error: Segment store file does not exist: " + segmentStoreFile);
            return 1;
        }

        if (!segmentStoreFile.isFile()) {
            System.err.println("Error: Segment store path is not a file: " + segmentStoreFile);
            return 1;
        }

        if (!cacheDir.exists()) {
            System.err.println("Error: Cache directory does not exist: " + cacheDir);
            return 1;
        }

        if (!cacheDir.isDirectory()) {
            System.err.println("Error: Cache path is not a directory: " + cacheDir);
            return 1;
        }

        // Check if output already exists
        if (outputFile.exists() && !force) {
            System.out.println("Derived PBF already exists: " + outputFile);
            System.out.println("Skipping build. Use --force to rebuild.");
            System.out.println();

            // Report existing file info
            System.out.println("Existing derived PBF info:");
            System.out.println("  Path: " + outputFile);
            System.out.println("  Size: " + (outputFile.length() / 1024 / 1024) + " MB");

            return 0;
        }

        // Ensure output directory exists
        File outputDir = outputFile.getParentFile();
        if (outputDir != null && !outputDir.exists()) {
            if (!outputDir.mkdirs()) {
                System.err.println("Error: Failed to create output directory: " + outputDir);
                return 1;
            }
        }

        if (force && outputFile.exists()) {
            System.out.println("Forcing rebuild (existing file will be overwritten)");
        }
        System.out.println("Building derived PBF file...");
        System.out.println();
        System.out.println("Inputs:");
        System.out.println("  Segment store: " + segmentStoreFile);
        System.out.println("  Cache directory: " + cacheDir);
        System.out.println("  Exclude barrier edges: " + !includeBarrierEdges);
        System.out.println();

        // Load cache files - auto-detect binary vs text format
        System.out.println("Loading cache files...");

        // Check for binary or text node cache
        boolean useBinaryNodeCache = cacheDir.toPath().resolve("nodes.bin").toFile().exists();
        boolean useBinaryWayCache = cacheDir.toPath().resolve("way_tags.bin").toFile().exists();

        if (useBinaryNodeCache) {
            System.out.println("  Using optimized binary node cache");
        } else {
            System.out.println("  Using legacy text node cache");
        }

        if (useBinaryWayCache) {
            System.out.println("  Using optimized compressed way tag cache");
        } else {
            System.out.println("  Using legacy text way tag cache");
        }

        // Load node cache
        NodeCache nodeCache = null;
        BinaryNodeCache binaryNodeCache = null;

        if (useBinaryNodeCache) {
            binaryNodeCache = new BinaryNodeCache(cacheDir.toPath().resolve("nodes.bin"));
            binaryNodeCache.load();
            System.out.println("  Loaded " + binaryNodeCache.size() + " nodes");
        } else {
            nodeCache = new NodeCache(cacheDir.toPath().resolve("nodes.txt"));
            nodeCache.load();
            System.out.println("  Loaded " + nodeCache.size() + " nodes");
        }

        // Load way tag cache
        WayTagCache wayTagCache = null;
        CompressedWayTagCache compressedWayTagCache = null;

        if (useBinaryWayCache) {
            compressedWayTagCache = new CompressedWayTagCache(cacheDir.toPath().resolve("way_tags.bin"));
            compressedWayTagCache.load();
            System.out.println("  Loaded " + compressedWayTagCache.size() + " ways");
        } else {
            wayTagCache = new WayTagCache(cacheDir.toPath().resolve("way_tags.txt"));
            wayTagCache.load();
            System.out.println("  Loaded " + wayTagCache.size() + " ways");
        }

        // Load relation cache (always text for now)
        RelationCache relationCache = new RelationCache(cacheDir.toPath().resolve("relations.txt"));
        relationCache.load();
        System.out.println("  Loaded " + relationCache.size() + " relations");
        System.out.println();

        // Rewrite relations
        System.out.println("Rewriting route relations...");
        RouteRelationRewriter rewriter = new RouteRelationRewriter(
            segmentStoreFile.toPath(),
            !includeBarrierEdges  // excludeBarrierEdges
        );

        List<OsmRelation> originalRelations = relationCache.getAll();
        List<OsmRelation> rewrittenRelations = rewriter.rewriteAll(originalRelations);
        System.out.println();

        // Write derived PBF
        System.out.println("Writing derived PBF file...");

        // Use the appropriate cache type (both implement the same interface)
        INodeCache iNodeCache = useBinaryNodeCache ? binaryNodeCache : nodeCache;
        IWayTagCache iWayTagCache = useBinaryWayCache ? compressedWayTagCache : wayTagCache;

        DerivedPbfWriter writer = new DerivedPbfWriter(
            segmentStoreFile.toPath(),
            iNodeCache,
            iWayTagCache,
            rewrittenRelations,
            !includeBarrierEdges  // excludeBarrierEdges
        );

        writer.write(outputFile.toPath());
        System.out.println();

        System.out.println("Success! Derived PBF file written to: " + outputFile);
        System.out.println("File size: " + (outputFile.length() / 1024) + " KB");

        return 0;
    }
}
