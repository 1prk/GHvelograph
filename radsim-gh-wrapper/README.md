# RadSim GraphHopper Wrapper

A Java wrapper module around GraphHopper to capture segment metadata during OSM import and generate derived PBF files with route relation rewriting.

## Overview

This tool integrates with GraphHopper's OSM import process to:
1. Capture every segment produced during import with full OSM node references
2. Store segment metadata in a compact binary format
3. Extract relevant OSM data (nodes, way tags, route relations) from original PBF
4. Generate derived PBF files where each segment becomes a way with GraphHopper edge ID

## Prerequisites

- Java 11 or later
- Gradle (wrapper included)

## Setup

### Build the Wrapper

```bash
./gradlew build
```

This will automatically download GraphHopper 11.0 from Maven Central.

## Usage

### Complete Workflow

The tool provides a three-step workflow to generate derived PBF files:

#### Step 1: Capture Segment Metadata

Capture segment metadata during GraphHopper import:

```bash
./gradlew :wrapper:run --args="capture-segments --osm path/to/input.pbf --segments path/to/output.rseg"
```

This command:
- Imports the OSM PBF file using GraphHopper
- Captures every segment produced during import
- Stores metadata in binary format (.rseg file)
- Records: GraphHopper edge ID, base way ID, OSM node references, segment index, barrier edge flag

#### Step 2: Extract OSM Data

Extract needed OSM data based on captured segments:

```bash
./gradlew :wrapper:run --args="extract-osm --osm path/to/input.pbf --segments path/to/segments.rseg --out path/to/cache-dir"
```

This command:
- Reads the segment store to identify needed node/way IDs
- Extracts only the needed elements from the original PBF
- Caches: node coordinates (with elevation), way tags, route relations
- Output: Three text files in the cache directory

#### Step 3: Build Derived PBF

Generate the derived PBF file with segment ways and rewritten route relations:

```bash
./gradlew :wrapper:run --args="build-derived-pbf --segments path/to/segments.rseg --cache path/to/cache-dir --out path/to/derived.pbf"
```

This command:
- Loads cached OSM data
- Rewrites route relations (expands way members into segment members)
- Writes derived PBF where each segment becomes a way
- Each way has: id=ghEdgeId, nodeRefs from segment, base_id tag, copied tags from original way

Optional: Use `--include-barrier-edges` to include barrier edges in the output (default: excluded)

### Example: Complete Workflow

```bash
# Step 1: Capture segments
./gradlew :wrapper:run --args="capture-segments --osm data/Wedel.pbf --segments output/wedel.rseg"

# Step 2: Extract OSM data
./gradlew :wrapper:run --args="extract-osm --osm data/Wedel.pbf --segments output/wedel.rseg --out output/cache"

# Step 3: Build derived PBF
./gradlew :wrapper:run --args="build-derived-pbf --segments output/wedel.rseg --cache output/cache --out output/wedel-derived.pbf"
```

### CLI Help

To see all available commands:

```bash
./gradlew :wrapper:run
```

For help on a specific command:

```bash
./gradlew :wrapper:run --args="capture-segments --help"
./gradlew :wrapper:run --args="extract-osm --help"
./gradlew :wrapper:run --args="build-derived-pbf --help"
```

## Derived PBF Format

The derived PBF file has the following structure:

### Ways (Segments)
Each segment from GraphHopper becomes a way with:
- **Way ID**: GraphHopper edge ID (integer)
- **Node References**: Original OSM node IDs from the segment
- **Tags**:
  - `base_id`: Original OSM way ID that this segment came from
  - Plus copied tags from the original way: `highway`, `name`, `ref`, `surface`, `maxspeed`, `oneway`, `bicycle`, `foot`

### Relations (Rewritten Routes)
Route relations are rewritten to reference segment ways:
- Original way members are expanded into their constituent segments
- Segments are ordered by their segment index
- Original roles are preserved for all segment members
- NODE and RELATION members are kept unchanged
- Barrier edges are excluded by default (use `--include-barrier-edges` to include)

### Nodes
All nodes referenced by segments, with:
- Original OSM node IDs
- Latitude/longitude coordinates
- Elevation (if available in original OSM data)

## Project Structure

```
radsim-gh-wrapper/
├── settings.gradle
├── build.gradle
├── gradle.properties
├── wrapper/                    # Main wrapper module
│   ├── build.gradle
│   └── src/
│       ├── main/java/
│       │   └── org/radsim/ghwrapper/
│       │       ├── cli/        # Command-line interface
│       │       ├── importhook/ # GraphHopper import integration
│       │       ├── osm/        # OSM PBF extraction and writing
│       │       ├── store/      # Binary segment store format
│       │       └── util/       # Utilities
│       └── test/java/
└── README.md
```

## Development Checkpoints

This project is developed in checkpoints as specified in CLAUDE.md:

- ✓ **Checkpoint 1**: Project skeleton + Gradle wiring (COMPLETED)
- ✓ **Checkpoint 2**: Segment store format + writer/reader + tests (COMPLETED)
- ✓ **Checkpoint 3**: GraphHopper import integration (COMPLETED)
- ✓ **Checkpoint 4**: OSM extraction cache (COMPLETED)
- ✓ **Checkpoint 5**: Route relation rewrite + derived PBF writer (COMPLETED)
- ✓ **Checkpoint 6**: End-to-end demo (COMPLETED)

## License

This project integrates with GraphHopper which is licensed under Apache 2.0.
