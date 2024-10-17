package net.staticstudios.data.meta;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.value.PersistentValue;

import java.lang.reflect.Field;
import java.util.Objects;

public final class PersistentValueMetadata implements SharedValueMetadata<PersistentValue<?>> {
    private final DataManager dataManager;
    private final String table;
    private final String column;
    private final Class<?> type;
    private final Field field;

    public PersistentValueMetadata(DataManager dataManager, String table, String column, Class<?> type, Field field) {
        this.dataManager = dataManager;
        this.table = table;
        this.column = column;
        this.type = type;
        this.field = field;
    }

    /**
     * Extract the metadata from a {@link PersistentValue}.
     *
     * @param dataManager The data manager to use
     * @param parentClass The parent class that this collection is a member of
     * @param table       The table that the parent object uses
     * @param value       A dummy instance of the value
     * @return The metadata for the value
     */
    @SuppressWarnings("unused") //Used via reflection
    public static <T extends UniqueData> PersistentValueMetadata extract(DataManager dataManager, Class<T> parentClass, String table, PersistentValue<?> value, Field field) {
        return new PersistentValueMetadata(dataManager, table, value.getColumn(), value.getType(), field);
    }

    @Override
    public String getTable() {
        return table;
    }

    public String getColumn() {
        return column;
    }

    @Override
    public Class<?> getType() {
        return type;
    }

    @Override
    public Field getField() {
        return field;
    }

    @Override
    public DataManager getDataManager() {
        return dataManager;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (PersistentValueMetadata) obj;
        return Objects.equals(this.table, that.table) &&
                Objects.equals(this.column, that.column) &&
                Objects.equals(this.type, that.type) &&
                Objects.equals(this.field, that.field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(table, column, type, field);
    }

    @Override
    public String toString() {
        return "PersistentValueMetadata[" +
                "col=" + column + ", " +
                "type=" + type + ']';
    }

    @Override
    public String getAddress() {
        return "persistent_value-" + getTable() + "-" + getColumn();
    }
}
