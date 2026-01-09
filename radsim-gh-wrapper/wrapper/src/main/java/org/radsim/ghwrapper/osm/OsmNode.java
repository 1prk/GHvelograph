package org.radsim.ghwrapper.osm;

/**
 * Simple OSM node with ID and coordinates.
 */
public class OsmNode {
    private final long id;
    private final double lat;
    private final double lon;
    private final double ele;  // Optional elevation, use Double.NaN if not present

    public OsmNode(long id, double lat, double lon, double ele) {
        this.id = id;
        this.lat = lat;
        this.lon = lon;
        this.ele = ele;
    }

    public OsmNode(long id, double lat, double lon) {
        this(id, lat, lon, Double.NaN);
    }

    public long getId() {
        return id;
    }

    public double getLat() {
        return lat;
    }

    public double getLon() {
        return lon;
    }

    public double getEle() {
        return ele;
    }

    public boolean hasElevation() {
        return !Double.isNaN(ele);
    }

    @Override
    public String toString() {
        return "OsmNode{id=" + id + ", lat=" + lat + ", lon=" + lon +
               (hasElevation() ? ", ele=" + ele : "") + '}';
    }
}
