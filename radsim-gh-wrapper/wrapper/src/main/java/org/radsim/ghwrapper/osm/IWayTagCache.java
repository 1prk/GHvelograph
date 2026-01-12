package org.radsim.ghwrapper.osm;

import java.util.Map;

/**
 * Interface for way tag cache implementations (text or binary).
 */
public interface IWayTagCache {
    /**
     * Gets way tags by OSM way ID.
     * @param wayId the OSM way ID
     * @return the tags, or null if not found
     */
    Map<String, String> get(long wayId);

    /**
     * Returns the number of ways in the cache.
     */
    int size();
}
