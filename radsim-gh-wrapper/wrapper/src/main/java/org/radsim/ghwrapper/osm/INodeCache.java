package org.radsim.ghwrapper.osm;

/**
 * Interface for node cache implementations (text or binary).
 */
public interface INodeCache {
    /**
     * Gets a node by OSM node ID.
     * @param osmNodeId the OSM node ID
     * @return the node, or null if not found
     */
    OsmNode get(long osmNodeId);

    /**
     * Returns the number of nodes in the cache.
     */
    int size();
}
