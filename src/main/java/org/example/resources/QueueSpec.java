package org.example.resources;

import java.util.Set;

// TODO can use records for these?
public class QueueSpec {
    private Set<String> groups;

    public Set<String> getGroups() {
        return groups;
    }

    public void setGroups(Set<String> groups) {
        this.groups = groups;
    }

    @Override
    public String toString() {
        return "QueueSpec{" +
                "groups=" + groups +
                '}';
    }
}
