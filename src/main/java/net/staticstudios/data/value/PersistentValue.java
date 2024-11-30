package net.staticstudios.data.value;

import net.staticstudios.data.UniqueData;
import net.staticstudios.data.meta.UniqueDataMetadata;
import net.staticstudios.data.meta.persistant.value.PersistentValueMetadata;

import java.util.UUID;
import java.util.function.Supplier;

/**
 * A persistent value is a value that is stored in a database and is synced between multiple servers.
 */
public class PersistentValue<T> extends AbstractPersistentValue<T, PersistentValueMetadata> {

    private PersistentValue(UniqueData parent, String column, Class<T> type, UpdateHandler<T> updateHandler) {
        super(parent, column, type, updateHandler, PersistentValue.class, PersistentValueMetadata.class);
    }

    /**
     * Create a new PersistentValue.
     *
     * @param parent The parent data object
     * @param type   The type of the value that will be stored
     * @param column The column name to use in the database
     * @return The new PersistentValue
     */
    @SuppressWarnings("unused")
    public static <T> PersistentValue<T> of(UniqueData parent, Class<T> type, String column) {
        return new PersistentValue<>(parent, column, type, updated -> {
        });
    }

    /**
     * Create a new PersistentValue.
     * This method allows you to specify an update handler that will be called when the value is set.
     * Note that the update handler is fired on each instance when an update is received.
     * Also note that the update handler is not guaranteed to run on a specific thread.
     *
     * @param parent        The parent data object
     * @param type          The type of the value that will be stored
     * @param column        The column name to use in the database
     * @param updateHandler The update handler to use
     * @return The new PersistentValue
     */
    @SuppressWarnings("unused")
    public static <T> PersistentValue<T> of(UniqueData parent, Class<T> type, String column, UpdateHandler<T> updateHandler) {
        return new PersistentValue<>(parent, column, type, updateHandler);
    }

    /**
     * Create a new PersistentValue with a default value.
     *
     * @param parent       The parent data object
     * @param type         The type of the value that will be stored
     * @param defaultValue The default (initial) value to use if the value is not present in the database
     * @param column       The column name to use in the database
     * @return The new PersistentValue
     */
    @SuppressWarnings("unused")
    public static <T> PersistentValue<T> withDefault(UniqueData parent, Class<T> type, T defaultValue, String column) {
        PersistentValue<T> value = new PersistentValue<>(parent, column, type, updated -> {
        });
        value.setInternal(defaultValue);
        return value;
    }

    /**
     * Create a new PersistentValue with a default value.
     * This method allows you to specify an update handler that will be called when the value is set.
     * Note that the update handler is fired on each instance when an update is received.
     * Also note that the update handler is not guaranteed to run on a specific thread.
     *
     * @param parent       The parent data object
     * @param type         The type of the value that will be stored
     * @param defaultValue The default (initial) value to use if the value is not present in the database
     * @param column       The column name to use in the database
     * @return The new PersistentValue
     */
    @SuppressWarnings("unused")
    public static <T> PersistentValue<T> withDefault(UniqueData parent, Class<T> type, T defaultValue, String column, UpdateHandler<T> updateHandler) {
        PersistentValue<T> value = new PersistentValue<>(parent, column, type, updateHandler);
        value.setInternal(defaultValue);
        return value;
    }

    /**
     * Create a new PersistentValue with a default value supplier.
     *
     * @param parent               The parent data object
     * @param type                 The type of the value that will be stored
     * @param defaultValueSupplier The supplier that will be called to get the default (initial) value to use if the value is not present in the database
     * @param column               The column name to use in the database
     * @return The new PersistentValue
     */
    @SuppressWarnings("unused")
    public static <T> PersistentValue<T> supplyDefault(UniqueData parent, Class<T> type, Supplier<T> defaultValueSupplier, String column) {
        PersistentValue<T> value = new PersistentValue<>(parent, column, type, updated -> {
        });
        value.setInternal(defaultValueSupplier.get());
        return value;
    }

    /**
     * Create a new PersistentValue with a default value supplier.
     * This method allows you to specify an update handler that will be called when the value is set.
     * Note that the update handler is fired on each instance when an update is received.
     * Also note that the update handler is not guaranteed to run on a specific thread.
     *
     * @param parent               The parent data object
     * @param type                 The type of the value that will be stored
     * @param defaultValueSupplier The supplier that will be called to get the default (initial) value to use if the value is not present in the database
     * @param column               The column name to use in the database
     * @return The new PersistentValue
     */
    @SuppressWarnings("unused")
    public static <T> PersistentValue<T> supplyDefault(UniqueData parent, Class<T> type, Supplier<T> defaultValueSupplier, String column, UpdateHandler<T> updateHandler) {
        PersistentValue<T> value = new PersistentValue<>(parent, column, type, updateHandler);
        value.setInternal(defaultValueSupplier.get());
        return value;
    }

    /**
     * Get the table that this value is stored in.
     * This is delegated to the metadata class since we want to get the table the {@link UniqueData} object, which requires accessing the {@link UniqueDataMetadata} object.
     *
     * @return The table name
     */
    @Override
    public String getTable() {
        return getMetadata().getTable();
    }

    @Override
    protected UUID getDataId() {
        return getParent().getId();
    }
}
