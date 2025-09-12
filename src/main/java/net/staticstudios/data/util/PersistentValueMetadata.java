package net.staticstudios.data.util;

import net.staticstudios.data.UniqueData;

import java.util.Objects;

public class PersistentValueMetadata {
    private final Class<? extends UniqueData> holderClass;
    private final String schema;
    private final String table;
    private final String column;
    private final Class<?> dataType;

    public PersistentValueMetadata(Class<? extends UniqueData> holderClass, String schema, String table, String column, Class<?> dataType) {
        this.holderClass = holderClass;
        this.schema = schema;
        this.table = table;
        this.column = column;
        this.dataType = dataType;
    }

    public Class<? extends UniqueData> getHolderClass() {
        return holderClass;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public String getColumn() {
        return column;
    }

    public Class<?> getDataType() {
        return dataType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(holderClass, schema, table, column, dataType);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        PersistentValueMetadata that = (PersistentValueMetadata) obj;
        return holderClass.equals(that.holderClass) &&
                schema.equals(that.schema) &&
                table.equals(that.table) &&
                column.equals(that.column) &&
                dataType.equals(that.dataType);
    }
}
