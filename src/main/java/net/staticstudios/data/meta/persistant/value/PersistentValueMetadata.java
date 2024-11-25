package net.staticstudios.data.meta.persistant.value;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.value.PersistentValue;

import java.lang.reflect.Field;

public final class PersistentValueMetadata extends AbstractPersistentValueMetadata<PersistentValue<?>> {

    public PersistentValueMetadata(DataManager dataManager, String table, String column, Class<?> type, Field field, Class<? extends UniqueData> parentClass) {
        super(dataManager, table, column, type, field, parentClass);
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
        return new PersistentValueMetadata(dataManager, table, value.getColumn(), value.getType(), field, parentClass);
    }
}
