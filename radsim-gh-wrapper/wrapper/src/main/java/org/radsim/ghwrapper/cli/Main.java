package org.radsim.ghwrapper.cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(
    name = "radsim-gh-wrapper",
    mixinStandardHelpOptions = true,
    version = "0.1.0-SNAPSHOT",
    description = "RadSim GraphHopper Wrapper - Capture segment metadata and generate derived PBF files",
    subcommands = {
        CaptureSegmentsCommand.class,
        ExtractOsmCommand.class,
        BuildDerivedPbfCommand.class
    }
)
public class Main implements Runnable {

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public void run() {
        System.out.println("RadSim GraphHopper Wrapper v0.1.0-SNAPSHOT");
        System.out.println("Target: Java 11, GraphHopper 11.0");
        System.out.println();
        System.out.println("Available commands:");
        System.out.println("  capture-segments   - Capture segment metadata from OSM PBF file");
        System.out.println("  extract-osm        - Extract needed OSM data based on segment store");
        System.out.println("  build-derived-pbf  - Build derived PBF with segment ways and rewritten relations");
        System.out.println();
        System.out.println("Use --help with any command for more information");
    }
}
