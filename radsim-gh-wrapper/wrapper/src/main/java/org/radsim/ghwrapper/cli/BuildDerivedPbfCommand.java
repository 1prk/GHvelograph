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

        // Ensure output directory exists
        File outputDir = outputFile.getParentFile();
        if (outputDir != null && !outputDir.exists()) {
            if (!outputDir.mkdirs()) {
                System.err.println("Error: Failed to create output directory: " + outputDir);
                return 1;
            }
        }

        System.out.println("Building derived PBF file...");
        System.out.println();
        System.out.println("Inputs:");
        System.out.println("  Segment store: " + segmentStoreFile);
        System.out.println("  Cache directory: " + cacheDir);
        System.out.println("  Exclude barrier edges: " + !includeBarrierEdges);
        System.out.println();

        // Load cache files
        System.out.println("Loading cache files...");

        NodeCache nodeCache = new NodeCache(cacheDir.toPath().resolve("nodes.txt"));
        nodeCache.load();
        System.out.println("  Loaded " + nodeCache.size() + " nodes");

        WayTagCache wayTagCache = new WayTagCache(cacheDir.toPath().resolve("way_tags.txt"));
        wayTagCache.load();
        System.out.println("  Loaded " + wayTagCache.size() + " ways");

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
        DerivedPbfWriter writer = new DerivedPbfWriter(
            segmentStoreFile.toPath(),
            nodeCache,
            wayTagCache,
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
