package net.staticstudios.data.value;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.DatabaseSupportedType;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.UpdatedValue;
import net.staticstudios.data.messaging.DataValueUpdateMessage;
import net.staticstudios.data.messaging.handle.DataValueUpdateMessageHandler;
import net.staticstudios.data.meta.SharedValueMetadata;
import net.staticstudios.data.meta.UniqueDataMetadata;
import net.staticstudios.data.shared.DataWrapper;
import net.staticstudios.data.shared.SharedValue;
import net.staticstudios.utils.ReflectionUtils;
import net.staticstudios.utils.ThreadUtils;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;

public abstract class AbstractPersistentValue<T, M extends SharedValueMetadata<?>> implements SharedValue<T> {
    private final UniqueData parent;
    private final String column;
    private final Class<T> dataClass;
    private final UpdateHandler<T> updateHandler;

    private final Class<?> valueClass;
    private final Class<M> metadataClass;
    private M metadata;
    private T value;

    protected AbstractPersistentValue(UniqueData parent, String column, Class<T> dataClass, UpdateHandler<T> updateHandler, Class<?> valueClass, Class<M> metadataClass) {
        this.parent = parent;
        this.column = column;
        this.dataClass = dataClass;
        this.updateHandler = updateHandler;

        this.valueClass = valueClass;
        this.metadataClass = metadataClass;
    }

    @Override
    public T get() {
        return value;
    }

    @Override
    public void set(T value) {
        M metadata = getMetadata();
        DataManager dataManager = metadata.getDataManager();

        // Update all other local wrappers with the new value, including this one
        UUID id = getDataId();
        Collection<DataWrapper> otherWrappers = dataManager.getDataWrappers(getDataAddress(id));
        for (DataWrapper wrapper : otherWrappers) {
            AbstractPersistentValue<T, M> otherPv = (AbstractPersistentValue<T, M>) wrapper;
            T oldValue = otherPv.get();
            otherPv.setInternal(value);
            otherPv.getUpdateHandler().onUpdate(new UpdatedValue<>(oldValue, value));
        }

        ThreadUtils.submit(() -> {
            try (Connection connection = dataManager.getConnection()) {
                setRemote(parent.getDataManager(), connection, value, id);
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
        M metadata = getMetadata();
        DataManager dataManager = metadata.getDataManager();

        // Update all other local wrappers with the new value, including this one
        UUID id = getDataId();
        Collection<DataWrapper> otherWrappers = dataManager.getDataWrappers(getDataAddress(id));
        for (DataWrapper wrapper : otherWrappers) {
            AbstractPersistentValue<T, M> otherPv = (AbstractPersistentValue<T, M>) wrapper;
            T oldValue = otherPv.get();
            otherPv.setInternal(value);
            otherPv.getUpdateHandler().onUpdate(new UpdatedValue<>(oldValue, value));
        }

        try {
            setRemote(dataManager, connection, value, id);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected void setRemote(DataManager dataManager, Connection connection, T value, UUID id) throws SQLException {
        String sql = "UPDATE " + getTable() + " SET " + column + " = ? WHERE id = ?";
        Object serialized = dataManager.serialize(value);

        DataManager.debug("Executing update: " + sql);
        PreparedStatement statement = connection.prepareStatement(sql);

        statement.setObject(1, serialized);
        statement.setObject(2, id);

        String encoded = DatabaseSupportedType.encode(serialized);

        statement.execute();

        dataManager.getMessenger().broadcastMessageNoPrefix(
                dataManager.getDataChannel(getDataAddress(id)),
                DataValueUpdateMessageHandler.class,
                new DataValueUpdateMessage(getDataAddress(id), encoded)
        );

    }

    @Override
    public final void setInternal(Object value) {
        this.value = (T) value;
    }

    @Override
    public final Class<T> getType() {
        return dataClass;
    }

    @Override
    public final UpdateHandler<T> getUpdateHandler() {
        return updateHandler;
    }

    @Override
    public final Class<? extends SharedValueMetadata<?>> getMetadataClass() {
        return metadataClass;
    }

    /**
     * Get the column that this value is stored in.
     *
     * @return The column name
     */
    public final String getColumn() {
        return column;
    }


    @Override
    public final M getMetadata() {
        if (metadata == null) {
            UniqueDataMetadata parentMetadata = parent.getDataManager().getUniqueDataMetadata(parent.getClass());
            Set<Field> allPersistentValues = ReflectionUtils.getFields(valueClass, parent.getClass());
            for (Field field : allPersistentValues) {
                AbstractPersistentValue<T, M> value = (AbstractPersistentValue<T, M>) ReflectionUtils.getFieldValue(field, parent);
                if (value == this) {
                    metadata = parentMetadata.getMetadata(metadataClass, field);
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
    public String getDataAddress() {
        return getDataAddress(getDataId());
    }

    @NotNull
    public String getDataAddress(UUID id) {
        return id + ".sql." + getTable() + "." + column;
    }

    /**
     * Get the table that this value is stored in.
     *
     * @return the table that this value is stored in
     */
    public abstract String getTable();

    protected abstract UUID getDataId();

    public UniqueData getParent() {
        return parent;
    }
}
