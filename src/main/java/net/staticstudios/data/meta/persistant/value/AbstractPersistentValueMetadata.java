package net.staticstudios.data.meta.persistant.value;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.meta.SharedValueMetadata;
import net.staticstudios.data.shared.SharedValue;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Field;
import java.util.Objects;

public abstract class AbstractPersistentValueMetadata<V extends SharedValue<?>> implements SharedValueMetadata<V> {
    private final DataManager dataManager;
    private final String table;
    private final String column;
    private final Class<?> type;
    private final Field field;
    private final Class<? extends UniqueData> parentClass;

    public AbstractPersistentValueMetadata(DataManager dataManager, String table, String column, Class<?> type, Field field, Class<? extends UniqueData> parentClass) {
        this.dataManager = dataManager;
        this.table = table;
        this.column = column;
        this.type = type;
        this.field = field;
        this.parentClass = parentClass;
    }

    @Override
    public final String getTable() {
        return table;
    }

    public final String getColumn() {
        return column;
    }

    @Override
    public final Class<?> getType() {
        return type;
    }

    @Override
    public final Field getField() {
        return field;
    }

    @Override
    public final DataManager getDataManager() {
        return dataManager;
    }

    @Override
    public final String toString() {
        return this.getClass().getSimpleName() + "{" +
                ", table='" + table + '\'' +
                ", column='" + column + '\'' +
                ", type=" + type +
                '}';
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractPersistentValueMetadata<?> that = (AbstractPersistentValueMetadata<?>) o;

        if (!table.equals(that.table)) return false;
        if (!column.equals(that.column)) return false;
        return type.equals(that.type);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(table, column, type, field);
    }


    @Override
    public @NotNull String getMetadataAddress() {
        return "sql." + table + "." + column;
    }

    @Override
    public Class<? extends UniqueData> getParentClass() {
        return parentClass;
    }
}
