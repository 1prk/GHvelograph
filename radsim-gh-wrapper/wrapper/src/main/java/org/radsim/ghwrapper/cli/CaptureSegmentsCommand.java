package org.radsim.ghwrapper.cli;

import org.radsim.ghwrapper.importhook.SegmentCaptureImporter;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.util.concurrent.Callable;

@Command(
    name = "capture-segments",
    description = "Capture segment metadata from OSM PBF file during GraphHopper import",
    mixinStandardHelpOptions = true
)
public class CaptureSegmentsCommand implements Callable<Integer> {

    @Option(
        names = {"--osm"},
        required = true,
        description = "Input OSM PBF file path"
    )
    private File osmFile;

    @Option(
        names = {"--segments", "-o"},
        required = true,
        description = "Output segment store file path (*.rseg)"
    )
    private File segmentStoreFile;

    @Option(
        names = {"--force", "-f"},
        description = "Force re-capture even if segment store already exists"
    )
    private boolean force = false;

    @Override
    public Integer call() throws Exception {
        // Validate input file exists
        if (!osmFile.exists()) {
            System.err.println("Error: OSM file does not exist: " + osmFile);
            return 1;
        }

        if (!osmFile.isFile()) {
            System.err.println("Error: OSM path is not a file: " + osmFile);
            return 1;
        }

        // Check if output already exists
        if (segmentStoreFile.exists() && !force) {
            System.out.println("Segment store already exists: " + segmentStoreFile);
            System.out.println("Skipping capture. Use --force to re-capture.");
            System.out.println();

            // Report existing file info
            System.out.println("Existing segment store info:");
            System.out.println("  Path: " + segmentStoreFile);
            System.out.println("  Size: " + (segmentStoreFile.length() / 1024 / 1024) + " MB");

            return 0;
        }

        // Create parent directory for output if needed
        File parentDir = segmentStoreFile.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            if (!parentDir.mkdirs()) {
                System.err.println("Error: Failed to create output directory: " + parentDir);
                return 1;
            }
        }

        // Run the import
        if (force && segmentStoreFile.exists()) {
            System.out.println("Forcing re-capture (existing file will be overwritten)");
        }
        System.out.println("Capturing segments from OSM file...");
        System.out.println();

        int segmentCount = SegmentCaptureImporter.runImport(
            osmFile.toPath(),
            segmentStoreFile.toPath()
        );

        System.out.println();
        System.out.println("Success! Captured " + segmentCount + " segments");
        System.out.println("Output written to: " + segmentStoreFile);

        return 0;
    }
}
