package net.staticstudios.data.parse;

import net.staticstudios.data.util.OnDelete;
import net.staticstudios.data.util.OnUpdate;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

public class ForeignKey {
    private final Set<Link> links = new LinkedHashSet<>();
    private final String schema;
    private final String table;
    private final OnDelete onDelete;
    private final OnUpdate onUpdate;

    public ForeignKey(String schema, String table, OnDelete onDelete, OnUpdate onUpdate) {
        this.schema = schema;
        this.table = table;
        this.onDelete = onDelete;
        this.onUpdate = onUpdate;
    }

    public void addLink(Link link) {
        links.add(link);
    }

    public Set<Link> getLinkingColumns() {
        return Collections.unmodifiableSet(links);
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public OnDelete getOnDelete() {
        return onDelete;
    }

    public OnUpdate getOnUpdate() {
        return onUpdate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ForeignKey that)) return false;
        return Objects.equals(onDelete, that.onDelete) &&
                Objects.equals(onUpdate, that.onUpdate) &&
                Objects.equals(links, that.links) &&
                Objects.equals(schema, that.schema) &&
                Objects.equals(table, that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(links, schema, table, onDelete, onUpdate);
    }

    public record Link(String columnInReferencedTable, String columnInReferringTable) {
    }
}
