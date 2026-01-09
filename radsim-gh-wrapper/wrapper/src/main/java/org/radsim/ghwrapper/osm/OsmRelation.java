package org.radsim.ghwrapper.osm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * OSM relation with tags and members.
 */
public class OsmRelation {
    private final long id;
    private final Map<String, String> tags;
    private final List<Member> members;

    public OsmRelation(long id, Map<String, String> tags, List<Member> members) {
        this.id = id;
        this.tags = new HashMap<>(tags);
        this.members = new ArrayList<>(members);
    }

    public long getId() {
        return id;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public List<Member> getMembers() {
        return members;
    }

    public String getTag(String key) {
        return tags.get(key);
    }

    @Override
    public String toString() {
        return "OsmRelation{id=" + id + ", tags=" + tags.size() + ", members=" + members.size() + '}';
    }

    /**
     * Relation member with type, ref, and role.
     */
    public static class Member {
        private final MemberType type;
        private final long ref;
        private final String role;

        public Member(MemberType type, long ref, String role) {
            this.type = type;
            this.ref = ref;
            this.role = role != null ? role : "";
        }

        public MemberType getType() {
            return type;
        }

        public long getRef() {
            return ref;
        }

        public String getRole() {
            return role;
        }

        @Override
        public String toString() {
            return type + ":" + ref + (role.isEmpty() ? "" : " (" + role + ")");
        }
    }

    public enum MemberType {
        NODE,
        WAY,
        RELATION
    }
}
