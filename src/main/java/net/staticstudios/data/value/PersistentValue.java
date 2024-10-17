package net.staticstudios.data.value;

import net.staticstudios.utils.ReflectionUtils;
import net.staticstudios.utils.ThreadUtils;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.DatabaseSupportedType;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.messaging.DataValueUpdateMessage;
import net.staticstudios.data.messaging.handle.DataValueUpdateMessageHandler;
import net.staticstudios.data.meta.PersistentValueMetadata;
import net.staticstudios.data.meta.SharedValueMetadata;
import net.staticstudios.data.meta.UniqueDataMetadata;
import net.staticstudios.data.shared.SharedValue;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A persistent value is a value that is stored in a database and is synced between multiple servers.
 */
public class PersistentValue<T> implements SharedValue<T> {
    private final UniqueData parent;
    private final String column;
    private final Class<T> type;
    private final UpdateHandler<T> updateHandler;
    private PersistentValueMetadata metadata;
    private T value;

    private PersistentValue(UniqueData parent, String column, Class<T> type, UpdateHandler<T> updateHandler) {
        this.parent = parent;
        this.column = column;
        this.type = type;
        this.updateHandler = updateHandler;
    }

    /**
     * Create a new PersistentValue.
     *
     * @param parent The parent data object
     * @param type   The type of the value that will be stored
     * @param column The column name to use in the database
     * @return The new PersistentValue
     */
    public static <T> PersistentValue<T> of(UniqueData parent, Class<T> type, String column) {
        return new PersistentValue<>(parent, column, type, (oldValue, newValue) -> newValue);
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
    public static <T> PersistentValue<T> withDefault(UniqueData parent, Class<T> type, T defaultValue, String column) {
        PersistentValue<T> value = new PersistentValue<>(parent, column, type, (oldVal, newVal) -> newVal);
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
    public static <T> PersistentValue<T> supplyDefault(UniqueData parent, Class<T> type, Supplier<T> defaultValueSupplier, String column) {
        PersistentValue<T> value = new PersistentValue<>(parent, column, type, (oldVal, newVal) -> newVal);
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
    public static <T> PersistentValue<T> supplyDefault(UniqueData parent, Class<T> type, Supplier<T> defaultValueSupplier, String column, UpdateHandler<T> updateHandler) {
        PersistentValue<T> value = new PersistentValue<>(parent, column, type, updateHandler);
        value.setInternal(defaultValueSupplier.get());
        return value;
    }

    @Override
    public T get() {
        return value;
    }

    @Override
    public void set(T value) {
        PersistentValueMetadata metadata = getMetadata();
        DataManager dataManager = metadata.getDataManager();
        value = updateHandler.onUpdate(this.value, value);
        setInternal(value);

        String table = getTable();
        String sql = "UPDATE " + table + " SET " + column + " = ? WHERE id = ?";

        Object serialized = dataManager.serialize(value);
        ThreadUtils.submit(() -> {
            try (Connection connection = dataManager.getConnection()) {
                DataManager.debug("Executing update: " + sql);
                PreparedStatement statement = connection.prepareStatement(sql);

                statement.setObject(1, serialized);
                statement.setObject(2, parent.getId());

                String encoded = DatabaseSupportedType.encode(serialized);

                UniqueDataMetadata uniqueDataMetadata = dataManager.getUniqueDataMetadata(parent.getClass());
                dataManager.getMessenger().broadcastMessageNoPrefix(
                        dataManager.getChannel(this),
                        DataValueUpdateMessageHandler.class,
                        new DataValueUpdateMessage(uniqueDataMetadata.getTopLevelTable(), parent.getId(), getAddress(), encoded)
                );

                statement.execute();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Set the value in the database, while providing a specific connection to use.
     * Note that this method is blocking and will not do any async operations.
     * Call {@link #set(Object)} to set the value asynchronously.
     *
     * @param connection The connection to use
     * @param value      The value to set
     */
    public void set(Connection connection, T value) {
        PersistentValueMetadata metadata = getMetadata();
        DataManager dataManager = metadata.getDataManager();
        value = updateHandler.onUpdate(this.value, value);
        setInternal(value);

        String table = getTable();
        String sql = "UPDATE " + table + " SET " + column + " = ? WHERE id = ?";

        Object serialized = dataManager.serialize(value);
        try {
            DataManager.debug("Executing update: " + sql);
            PreparedStatement statement = connection.prepareStatement(sql);

            statement.setObject(1, serialized);
            statement.setObject(2, parent.getId());

            String encoded = DatabaseSupportedType.encode(serialized);

            UniqueDataMetadata uniqueDataMetadata = dataManager.getUniqueDataMetadata(parent.getClass());
            dataManager.getMessenger().broadcastMessageNoPrefix(
                    dataManager.getChannel(this),
                    DataValueUpdateMessageHandler.class,
                    new DataValueUpdateMessage(uniqueDataMetadata.getTopLevelTable(), parent.getId(), getAddress(), encoded)
            );

            statement.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setInternal(Object value) {
        this.value = (T) value;
    }

    @Override
    public Class<T> getType() {
        return type;
    }

    @Override
    public UpdateHandler<T> getUpdateHandler() {
        return updateHandler;
    }

    @Override
    public Class<? extends SharedValueMetadata<?>> getMetadataClass() {
        return PersistentValueMetadata.class;
    }

    /**
     * Get the column that this value is stored in.
     *
     * @return The column name
     */
    public String getColumn() {
        return column;
    }

    /**
     * Get the table that this value is stored in.
     *
     * @return The table name
     */
    public String getTable() {
        return getMetadata().getTable();
    }

    private PersistentValueMetadata getMetadata() {
        if (metadata == null) {
            UniqueDataMetadata parentMetadata = parent.getDataManager().getUniqueDataMetadata(parent.getClass());
            Set<Field> allPersistentValues = ReflectionUtils.getFields(PersistentValue.class, parent.getClass());
            for (Field field : allPersistentValues) {
                PersistentValue<?> value = (PersistentValue<?>) ReflectionUtils.getFieldValue(field, parent);
                if (value == this) {
                    metadata = parentMetadata.getMetadata(PersistentValueMetadata.class, field);
                    break;
                }
            }

            if (metadata == null) {
                throw new RuntimeException("Failed to find metadata for " + this);
            }
        }

        return metadata;
    }

    @Override
    public String getAddress() {
        return getMetadata().getAddress();
    }
}
