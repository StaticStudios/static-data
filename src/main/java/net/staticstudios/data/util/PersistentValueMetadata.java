package net.staticstudios.data.util;

import net.staticstudios.data.UniqueData;

import java.util.Objects;

public class PersistentValueMetadata {
    private final Class<? extends UniqueData> holderClass;
    private final ColumnMetadata columnMetadata;

    public PersistentValueMetadata(Class<? extends UniqueData> holderClass, ColumnMetadata columnMetadata) {
        this.holderClass = holderClass;
        this.columnMetadata = columnMetadata;
    }

    public String getSchema() {
        return columnMetadata.schema();
    }

    public String getTable() {
        return columnMetadata.table();
    }

    public String getColumn() {
        return columnMetadata.name();
    }

    public ColumnMetadata getColumnMetadata() {
        return columnMetadata;
    }

    @Override
    public int hashCode() {
        return Objects.hash(holderClass, columnMetadata);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        PersistentValueMetadata that = (PersistentValueMetadata) obj;
        return Objects.equals(holderClass, that.holderClass) && Objects.equals(columnMetadata, that.columnMetadata);
    }
}
