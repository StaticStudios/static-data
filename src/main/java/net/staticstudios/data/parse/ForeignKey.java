package net.staticstudios.data.parse;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

public class ForeignKey {
    private final Set<Link> links = new LinkedHashSet<>();
    private final String schema;
    private final String table;

    public ForeignKey(String schema, String table) {
        this.schema = schema;
        this.table = table;
    }

    public void addColumnMapping(Link link) {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ForeignKey that)) return false;
        return Objects.equals(links, that.links) && Objects.equals(schema, that.schema) && Objects.equals(table, that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(links, schema, table);
    }

    public record Link(String columnInReferencedTable, String columnInReferringTable) {
    }
}
