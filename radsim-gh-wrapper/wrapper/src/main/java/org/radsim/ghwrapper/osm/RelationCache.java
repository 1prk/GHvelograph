package org.radsim.ghwrapper.osm;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Cache for OSM route relations.
 * Uses a text-based format for simplicity.
 *
 * Format for each relation:
 * RELATION relationId
 * TAG key=value
 * TAG key=value
 * MEMBER type,ref,role
 * MEMBER type,ref,role
 * (blank line)
 */
public class RelationCache implements Closeable {
    private final Path cacheFile;
    private final List<OsmRelation> relations = new ArrayList<>();
    private BufferedWriter writer;

    public RelationCache(Path cacheFile) {
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
     * Writes a relation to the cache.
     */
    public void put(OsmRelation relation) throws IOException {
        if (writer == null) {
            throw new IllegalStateException("Cache not opened for writing");
        }

        writer.write("RELATION " + relation.getId() + "\n");

        for (Map.Entry<String, String> tag : relation.getTags().entrySet()) {
            String key = escape(tag.getKey());
            String value = escape(tag.getValue());
            writer.write("TAG " + key + "=" + value + "\n");
        }

        for (OsmRelation.Member member : relation.getMembers()) {
            writer.write("MEMBER " + member.getType() + "," + member.getRef() + "," +
                        escape(member.getRole()) + "\n");
        }

        writer.write("\n");  // Blank line separator
    }

    /**
     * Loads all relations from the cache file into memory.
     */
    public void load() throws IOException {
        relations.clear();
        if (!Files.exists(cacheFile)) {
            return;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(cacheFile.toFile()))) {
            String line;
            Long currentRelationId = null;
            Map<String, String> currentTags = null;
            List<OsmRelation.Member> currentMembers = null;

            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    // End of relation entry
                    if (currentRelationId != null) {
                        relations.add(new OsmRelation(currentRelationId, currentTags, currentMembers));
                    }
                    currentRelationId = null;
                    currentTags = null;
                    currentMembers = null;
                } else if (line.startsWith("RELATION ")) {
                    currentRelationId = Long.parseLong(line.substring(9));
                    currentTags = new HashMap<>();
                    currentMembers = new ArrayList<>();
                } else if (line.startsWith("TAG ")) {
                    String tagLine = line.substring(4);
                    int equalsPos = tagLine.indexOf('=');
                    if (equalsPos > 0 && currentTags != null) {
                        String key = unescape(tagLine.substring(0, equalsPos));
                        String value = unescape(tagLine.substring(equalsPos + 1));
                        currentTags.put(key, value);
                    }
                } else if (line.startsWith("MEMBER ") && currentMembers != null) {
                    String memberLine = line.substring(7);
                    String[] parts = memberLine.split(",", 3);
                    if (parts.length == 3) {
                        OsmRelation.MemberType type = OsmRelation.MemberType.valueOf(parts[0]);
                        long ref = Long.parseLong(parts[1]);
                        String role = unescape(parts[2]);
                        currentMembers.add(new OsmRelation.Member(type, ref, role));
                    }
                }
            }

            // Handle last entry if file doesn't end with blank line
            if (currentRelationId != null) {
                relations.add(new OsmRelation(currentRelationId, currentTags, currentMembers));
            }
        }
    }

    /**
     * Gets all relations from the cache.
     */
    public List<OsmRelation> getAll() {
        return new ArrayList<>(relations);
    }

    /**
     * Returns the number of relations in the cache.
     */
    public int size() {
        return relations.size();
    }

    private String escape(String s) {
        return s.replace("\n", "\\n").replace(",", "\\,").replace("=", "\\=");
    }

    private String unescape(String s) {
        return s.replace("\\=", "=").replace("\\,", ",").replace("\\n", "\n");
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            writer.close();
            writer = null;
        }
    }
}
