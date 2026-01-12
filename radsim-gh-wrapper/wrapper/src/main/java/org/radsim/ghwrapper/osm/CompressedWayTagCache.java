package org.radsim.ghwrapper.osm;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ShortOpenHashMap;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

/**
 * Compressed way tag cache using frequency-based string dictionary.
 *
 * Storage strategy:
 * - Common key=value pairs (e.g., highway=residential, surface=asphalt) → 2-byte dictionary index
 * - Unique strings (e.g., name=Main Street) → string pool reference
 *
 * Format:
 * - Header: "RWAY" (4 bytes) + version (1 byte) + wayCount (int32) + dictSize (short)
 * - Dictionary section: dictSize entries of null-terminated UTF-8 strings "key=value\0"
 * - Index section: wayId (int64) + offset (int32) pairs, sorted by wayId
 * - Data section: tag records per way
 *   - tagCount (byte)
 *   - For each tag:
 *     - type (byte): 0=dictionary ref, 1=custom key-value
 *     - if type==0: dictIndex (short)
 *     - if type==1: keyLen (short) + key (UTF-8) + valueLen (short) + value (UTF-8)
 *
 * Memory savings example for 10M ways:
 * - Text format: ~15-20 GB (strings + HashMap overhead)
 * - Compressed: ~2-3 GB (dictionary + indices + unique strings)
 */
public class CompressedWayTagCache implements Closeable, IWayTagCache {
    private static final byte[] MAGIC = {'R', 'W', 'A', 'Y'};
    private static final byte VERSION = 1;
    private static final byte TYPE_DICT_REF = 0;
    private static final byte TYPE_CUSTOM = 1;
    private static final int MAX_DICT_SIZE = 32000; // Max ~32K common pairs

    private final Path cacheFile;

    // Writing state
    private Path tempIndexFile;
    private Path tempDataFile;
    private DataOutputStream indexStream;
    private DataOutputStream dataStream;
    private int wayCount = 0;
    private long currentDataOffset = 0;

    // Dictionary built during first pass
    private Object2ShortOpenHashMap<String> dictionary;
    private List<String> dictionaryList;

    // Reading state
    private MappedByteBuffer indexBuffer;
    private MappedByteBuffer dataBuffer;
    private String[] dictEntries;
    private Long2ObjectOpenHashMap<Map<String, String>> cache;

    public CompressedWayTagCache(Path cacheFile) {
        this.cacheFile = cacheFile;
    }

    /**
     * Builds frequency dictionary from a sample of ways.
     * Call this before openForWrite().
     */
    public void buildDictionary(Iterator<Map.Entry<Long, Map<String, String>>> wayIterator, int sampleSize) {
        System.out.println("Building tag dictionary from sample...");

        Map<String, Integer> frequencyMap = new HashMap<>();
        int sampled = 0;

        while (wayIterator.hasNext() && sampled < sampleSize) {
            Map.Entry<Long, Map<String, String>> entry = wayIterator.next();
            for (Map.Entry<String, String> tag : entry.getValue().entrySet()) {
                String pair = tag.getKey() + "=" + tag.getValue();
                frequencyMap.merge(pair, 1, Integer::sum);
            }
            sampled++;
        }

        // Sort by frequency and take top MAX_DICT_SIZE
        List<Map.Entry<String, Integer>> sorted = new ArrayList<>(frequencyMap.entrySet());
        sorted.sort((a, b) -> Integer.compare(b.getValue(), a.getValue()));

        dictionaryList = new ArrayList<>();
        dictionary = new Object2ShortOpenHashMap<>();
        dictionary.defaultReturnValue((short) -1);

        int limit = Math.min(sorted.size(), MAX_DICT_SIZE);
        for (int i = 0; i < limit; i++) {
            String pair = sorted.get(i).getKey();
            dictionaryList.add(pair);
            dictionary.put(pair, (short) i);
        }

        System.out.println("Dictionary built: " + dictionaryList.size() + " common tag pairs");
        if (!dictionaryList.isEmpty()) {
            System.out.println("  Top 5: " + dictionaryList.subList(0, Math.min(5, dictionaryList.size())));
        }
    }

    /**
     * Opens cache for writing.
     */
    public void openForWrite() throws IOException {
        if (dictionary == null) {
            // Use empty dictionary if not built
            dictionary = new Object2ShortOpenHashMap<>();
            dictionary.defaultReturnValue((short) -1);
            dictionaryList = new ArrayList<>();
        }

        Files.createDirectories(cacheFile.getParent());

        tempIndexFile = cacheFile.getParent().resolve(cacheFile.getFileName() + ".idx.tmp");
        tempDataFile = cacheFile.getParent().resolve(cacheFile.getFileName() + ".dat.tmp");

        indexStream = new DataOutputStream(new BufferedOutputStream(
            new FileOutputStream(tempIndexFile.toFile()), 1024 * 1024));
        dataStream = new DataOutputStream(new BufferedOutputStream(
            new FileOutputStream(tempDataFile.toFile()), 1024 * 1024));

        wayCount = 0;
        currentDataOffset = 0;
    }

    /**
     * Writes way tags.
     */
    public void put(long wayId, Map<String, String> tags) throws IOException {
        if (dataStream == null) {
            throw new IllegalStateException("Cache not opened for writing");
        }

        // Write index entry
        indexStream.writeLong(wayId);
        indexStream.writeInt((int) currentDataOffset);

        // Write data
        long startOffset = currentDataOffset;

        dataStream.writeByte(tags.size());

        for (Map.Entry<String, String> tag : tags.entrySet()) {
            String pair = tag.getKey() + "=" + tag.getValue();
            short dictIndex = dictionary.getOrDefault(pair, (short) -1);

            if (dictIndex >= 0) {
                // Dictionary reference
                dataStream.writeByte(TYPE_DICT_REF);
                dataStream.writeShort(dictIndex);
                currentDataOffset += 3; // 1 + 2
            } else {
                // Custom key-value
                byte[] keyBytes = tag.getKey().getBytes(StandardCharsets.UTF_8);
                byte[] valueBytes = tag.getValue().getBytes(StandardCharsets.UTF_8);

                dataStream.writeByte(TYPE_CUSTOM);
                dataStream.writeShort(keyBytes.length);
                dataStream.write(keyBytes);
                dataStream.writeShort(valueBytes.length);
                dataStream.write(valueBytes);

                currentDataOffset += 1 + 2 + keyBytes.length + 2 + valueBytes.length;
            }
        }

        wayCount++;

        if (wayCount % 100_000 == 0) {
            System.out.println("  Compressed way cache: written " + wayCount + " ways");
        }
    }

    /**
     * Finalizes writing.
     */
    public void finishWrite() throws IOException {
        if (indexStream == null) {
            return;
        }

        indexStream.close();
        dataStream.close();

        System.out.println("Merging way tag cache...");

        try (FileChannel outChannel = FileChannel.open(cacheFile,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {

            // Write header
            ByteBuffer header = ByteBuffer.allocate(11);
            header.put(MAGIC);
            header.put(VERSION);
            header.putInt(wayCount);
            header.putShort((short) dictionaryList.size());
            header.flip();
            outChannel.write(header);

            // Write dictionary
            for (String pair : dictionaryList) {
                byte[] bytes = (pair + "\0").getBytes(StandardCharsets.UTF_8);
                outChannel.write(ByteBuffer.wrap(bytes));
            }

            // Copy index
            try (FileChannel indexChannel = FileChannel.open(tempIndexFile, StandardOpenOption.READ)) {
                indexChannel.transferTo(0, indexChannel.size(), outChannel);
            }

            // Copy data
            try (FileChannel dataChannel = FileChannel.open(tempDataFile, StandardOpenOption.READ)) {
                dataChannel.transferTo(0, dataChannel.size(), outChannel);
            }
        }

        Files.deleteIfExists(tempIndexFile);
        Files.deleteIfExists(tempDataFile);

        indexStream = null;
        dataStream = null;

        System.out.println("Compressed way tag cache complete: " + wayCount + " ways");
    }

    /**
     * Loads cache for reading.
     */
    public void load() throws IOException {
        cache = new Long2ObjectOpenHashMap<>();

        if (!Files.exists(cacheFile)) {
            return;
        }

        try (FileChannel channel = FileChannel.open(cacheFile, StandardOpenOption.READ)) {
            // Read header
            ByteBuffer header = ByteBuffer.allocate(11);
            channel.read(header);
            header.flip();

            byte[] magic = new byte[4];
            header.get(magic);
            if (!java.util.Arrays.equals(magic, MAGIC)) {
                throw new IOException("Invalid way tag cache format");
            }

            byte version = header.get();
            if (version != VERSION) {
                throw new IOException("Unsupported version: " + version);
            }

            int count = header.getInt();
            short dictSize = header.getShort();

            // Read dictionary
            dictEntries = new String[dictSize];
            ByteArrayOutputStream dictBuffer = new ByteArrayOutputStream();
            int dictIndex = 0;

            for (int i = 0; i < dictSize; i++) {
                dictBuffer.reset();
                while (true) {
                    ByteBuffer b = ByteBuffer.allocate(1);
                    channel.read(b);
                    byte c = b.get(0);
                    if (c == 0) break;
                    dictBuffer.write(c);
                }
                dictEntries[i] = dictBuffer.toString(StandardCharsets.UTF_8);
            }

            long indexStart = channel.position();
            long indexSize = (long) count * 12; // 8 + 4

            // Map index
            indexBuffer = channel.map(FileChannel.MapMode.READ_ONLY, indexStart, indexSize);

            // Map data
            long dataStart = indexStart + indexSize;
            long dataSize = channel.size() - dataStart;
            dataBuffer = channel.map(FileChannel.MapMode.READ_ONLY, dataStart, dataSize);

            // Build cache
            for (int i = 0; i < count; i++) {
                long wayId = indexBuffer.getLong(i * 12);
                int offset = indexBuffer.getInt(i * 12 + 8);

                Map<String, String> tags = readTags(offset);
                cache.put(wayId, tags);
            }

            System.out.println("Loaded compressed way tag cache: " + count + " ways, " +
                             dictSize + " dictionary entries");
        }
    }

    private Map<String, String> readTags(int offset) {
        Map<String, String> tags = new HashMap<>();

        int tagCount = dataBuffer.get(offset) & 0xFF;
        int pos = offset + 1;

        for (int i = 0; i < tagCount; i++) {
            byte type = dataBuffer.get(pos++);

            if (type == TYPE_DICT_REF) {
                short dictIndex = dataBuffer.getShort(pos);
                pos += 2;

                String pair = dictEntries[dictIndex];
                int eqPos = pair.indexOf('=');
                if (eqPos > 0) {
                    tags.put(pair.substring(0, eqPos), pair.substring(eqPos + 1));
                }
            } else if (type == TYPE_CUSTOM) {
                short keyLen = dataBuffer.getShort(pos);
                pos += 2;

                byte[] keyBytes = new byte[keyLen];
                for (int j = 0; j < keyLen; j++) {
                    keyBytes[j] = dataBuffer.get(pos++);
                }

                short valueLen = dataBuffer.getShort(pos);
                pos += 2;

                byte[] valueBytes = new byte[valueLen];
                for (int j = 0; j < valueLen; j++) {
                    valueBytes[j] = dataBuffer.get(pos++);
                }

                String key = new String(keyBytes, StandardCharsets.UTF_8);
                String value = new String(valueBytes, StandardCharsets.UTF_8);
                tags.put(key, value);
            }
        }

        return tags;
    }

    public Map<String, String> get(long wayId) {
        return cache != null ? cache.get(wayId) : null;
    }

    public int size() {
        return cache != null ? cache.size() : 0;
    }

    @Override
    public void close() throws IOException {
        if (indexStream != null) {
            finishWrite();
        }

        indexBuffer = null;
        dataBuffer = null;
        cache = null;
    }
}
