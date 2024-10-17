package net.staticstudios.data.meta;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.value.CachedValue;

import java.lang.reflect.Field;
import java.util.Objects;

public final class CachedValueMetadata implements SharedValueMetadata<CachedValue<?>> {
    private final DataManager dataManager;
    private final String key;
    private final String table;
    private final Class<?> type;
    private final Field field;

    public CachedValueMetadata(DataManager dataManager, String table, String key, Class<?> type, Field field) {
        this.dataManager = dataManager;
        this.table = table;
        this.key = key;
        this.type = type;
        this.field = field;
    }

    /**
     * Extract the metadata from a {@link CachedValue}.
     *
     * @param dataManager The data manager to use
     * @param parentClass The parent class that this collection is a member of
     * @param table       The table that the parent object uses
     * @param value       A dummy instance of the value
     * @return The metadata for the value
     */
    @SuppressWarnings("unused") //Used via reflection
    public static <T extends UniqueData> CachedValueMetadata extract(DataManager dataManager, Class<T> parentClass, String table, CachedValue<?> value, Field field) {
        return new CachedValueMetadata(dataManager, table, value.getKey(), value.getType(), field);
    }

    public String getKey() {
        return key;
    }

    @Override
    public Class<?> getType() {
        return type;
    }

    @Override
    public String getTable() {
        return table;
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
        var that = (CachedValueMetadata) obj;
        return Objects.equals(this.key, that.key) &&
                Objects.equals(this.type, that.type) &&
                Objects.equals(this.field, that.field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, type, field);
    }

    @Override
    public String getAddress() {
        return "cached_value-" + key;
    }
}
