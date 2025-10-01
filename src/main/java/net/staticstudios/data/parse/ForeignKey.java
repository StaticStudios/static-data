package net.staticstudios.data.parse;

import net.staticstudios.data.util.OnDelete;
import net.staticstudios.data.util.OnUpdate;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

public class ForeignKey {
    private final Set<Link> links = new LinkedHashSet<>();
    private final String referencedSchema;
    private final String referencedTable;
    private final String referringSchema;
    private final String referringTable;
    private final OnDelete onDelete;
    private final OnUpdate onUpdate;

    public ForeignKey(String referringSchema, String referringTable, String referencedSchema, String referencedTable, OnDelete onDelete, OnUpdate onUpdate) {
        this.referringSchema = referringSchema;
        this.referringTable = referringTable;
        this.referencedSchema = referencedSchema;
        this.referencedTable = referencedTable;
        this.onDelete = onDelete;
        this.onUpdate = onUpdate;
    }

    public void addLink(Link link) {
        links.add(link);
    }

    public Set<Link> getLinkingColumns() {
        return Collections.unmodifiableSet(links);
    }

    public String getReferencedSchema() {
        return referencedSchema;
    }

    public String getReferencedTable() {
        return referencedTable;
    }

    public String getReferringSchema() {
        return referringSchema;
    }

    public String getReferringTable() {
        return referringTable;
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
                Objects.equals(referencedSchema, that.referencedSchema) &&
                Objects.equals(referencedTable, that.referencedTable) &&
                Objects.equals(referringSchema, that.referringSchema) &&
                Objects.equals(referringTable, that.referringTable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(links, referencedSchema, referencedTable, onDelete, onUpdate, referringSchema, referringTable);
    }

    public record Link(String columnInReferencedTable, String columnInReferringTable) {
    }
}
