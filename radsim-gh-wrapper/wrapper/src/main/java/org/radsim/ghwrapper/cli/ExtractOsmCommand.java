package org.radsim.ghwrapper.cli;

import org.radsim.ghwrapper.osm.OsmDataExtractor;
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

        // Run the extraction
        System.out.println("Extracting OSM data from PBF file...");
        System.out.println();

        OsmDataExtractor.ExtractionStats stats = OsmDataExtractor.extract(
            osmFile.toPath(),
            segmentStoreFile.toPath(),
            cacheDir.toPath()
        );

        System.out.println();
        System.out.println("Success! Extraction complete:");
        System.out.println("  Nodes extracted: " + stats.nodesExtracted);
        System.out.println("  Ways extracted: " + stats.waysExtracted);
        System.out.println("  Relations extracted: " + stats.relationsExtracted);
        System.out.println("Output written to: " + cacheDir);

        return 0;
    }
}
