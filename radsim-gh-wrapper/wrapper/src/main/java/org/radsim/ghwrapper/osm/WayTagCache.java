package org.radsim.ghwrapper.osm;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Cache for OSM way tags (baseWayId -> tags).
 * Uses a text-based format for simplicity.
 *
 * Format: Each way is represented as:
 * wayId
 * key1=value1
 * key2=value2
 * (blank line)
 */
public class WayTagCache implements Closeable {
    private final Path cacheFile;
    private final Map<Long, Map<String, String>> cache = new HashMap<>();
    private BufferedWriter writer;

    public WayTagCache(Path cacheFile) {
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
     * Writes way tags to the cache.
     */
    public void put(long wayId, Map<String, String> tags) throws IOException {
        if (writer == null) {
            throw new IllegalStateException("Cache not opened for writing");
        }
        writer.write(Long.toString(wayId) + "\n");
        for (Map.Entry<String, String> entry : tags.entrySet()) {
            // Escape newlines and equals signs in keys/values
            String key = entry.getKey().replace("\n", "\\n").replace("=", "\\=");
            String value = entry.getValue().replace("\n", "\\n").replace("=", "\\=");
            writer.write(key + "=" + value + "\n");
        }
        writer.write("\n");  // Blank line separator
    }

    /**
     * Loads all way tags from the cache file into memory.
     */
    public void load() throws IOException {
        cache.clear();
        if (!Files.exists(cacheFile)) {
            return;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(cacheFile.toFile()))) {
            String line;
            Long currentWayId = null;
            Map<String, String> currentTags = null;

            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    // End of way entry
                    if (currentWayId != null && currentTags != null) {
                        cache.put(currentWayId, currentTags);
                    }
                    currentWayId = null;
                    currentTags = null;
                } else if (currentWayId == null) {
                    // First line: way ID
                    currentWayId = Long.parseLong(line);
                    currentTags = new HashMap<>();
                } else {
                    // Tag line: key=value
                    int equalsPos = line.indexOf('=');
                    if (equalsPos > 0) {
                        String key = line.substring(0, equalsPos).replace("\\=", "=").replace("\\n", "\n");
                        String value = line.substring(equalsPos + 1).replace("\\=", "=").replace("\\n", "\n");
                        currentTags.put(key, value);
                    }
                }
            }

            // Handle last entry if file doesn't end with blank line
            if (currentWayId != null && currentTags != null) {
                cache.put(currentWayId, currentTags);
            }
        }
    }

    /**
     * Gets way tags from the cache.
     */
    public Map<String, String> get(long wayId) {
        return cache.get(wayId);
    }

    /**
     * Returns the number of ways in the cache.
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
