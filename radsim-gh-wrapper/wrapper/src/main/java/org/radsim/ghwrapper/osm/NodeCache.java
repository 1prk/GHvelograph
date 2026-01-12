package org.radsim.ghwrapper.osm;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

/**
 * Simple cache for OSM nodes (id -> lat/lon/ele).
 * Uses a text-based format for simplicity and debugging.
 *
 * Format: Each line is "osmNodeId,lat,lon,ele"
 */
public class NodeCache implements Closeable, INodeCache {
    private final Path cacheFile;
    private final Long2ObjectOpenHashMap<OsmNode> cache = new Long2ObjectOpenHashMap<>();
    private BufferedWriter writer;

    public NodeCache(Path cacheFile) {
        this.cacheFile = cacheFile;
    }

    /**
     * Opens the cache for writing.
     */
    public void openForWrite() throws IOException {
        Files.createDirectories(cacheFile.getParent());
        writer = new BufferedWriter(new FileWriter(cacheFile.toFile()));
    }

    /**
     * Writes a node to the cache.
     */
    public void put(OsmNode node) throws IOException {
        if (writer == null) {
            throw new IllegalStateException("Cache not opened for writing");
        }
        writer.write(node.getId() + "," + node.getLat() + "," + node.getLon() + "," +
                     (node.hasElevation() ? node.getEle() : "") + "\n");
    }

    /**
     * Loads all nodes from the cache file into memory.
     */
    public void load() throws IOException {
        cache.clear();
        if (!Files.exists(cacheFile)) {
            return;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(cacheFile.toFile()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length < 3) continue;

                long id = Long.parseLong(parts[0]);
                double lat = Double.parseDouble(parts[1]);
                double lon = Double.parseDouble(parts[2]);
                double ele = parts.length > 3 && !parts[3].isEmpty() ? Double.parseDouble(parts[3]) : Double.NaN;

                cache.put(id, new OsmNode(id, lat, lon, ele));
            }
        }
    }

    /**
     * Gets a node from the cache.
     */
    public OsmNode get(long osmNodeId) {
        return cache.get(osmNodeId);
    }

    /**
     * Returns the number of nodes in the cache.
     */
    public int size() {
        return cache.size();
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            writer.close();
            writer = null;
        }
    }
}
