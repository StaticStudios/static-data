package net.staticstudios.data.value;

import net.staticstudios.utils.ReflectionUtils;
import net.staticstudios.utils.ThreadUtils;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.DatabaseSupportedType;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.messaging.DataValueUpdateMessage;
import net.staticstudios.data.messaging.handle.DataValueUpdateMessageHandler;
import net.staticstudios.data.meta.CachedValueMetadata;
import net.staticstudios.data.meta.SharedValueMetadata;
import net.staticstudios.data.meta.UniqueDataMetadata;
import net.staticstudios.data.shared.SharedValue;
import redis.clients.jedis.Jedis;

import java.lang.reflect.Field;
import java.util.Objects;
import java.util.Set;

/**
 * A cached value is a value that is stored in Redis and is synced between multiple servers.
 * If a value is not present in Redis, the default value will be used.
 */
public class CachedValue<T> implements SharedValue<T> {
    private final UniqueData parent;
    private final String key;
    private final Class<T> type;
    private final UpdateHandler<T> updateHandler;
    private final T defaultValue;
    private CachedValueMetadata metadata;
    private T value;

    private CachedValue(UniqueData parent, String key, Class<T> type, T defaultValue, UpdateHandler<T> updateHandler) {
        this.parent = parent;
        this.key = key;
        this.type = type;
        this.defaultValue = defaultValue;
        this.updateHandler = updateHandler;
    }

    /**
     * Create a new CachedValue.
     *
     * @param parent       The parent data object
     * @param type         The type of the value that will be stored
     * @param defaultValue The default value to use if the value is not present in Redis
     * @param key          The key to use in Redis. (note this will only be part of the real key as other information will be included to ensure uniqueness)
     * @return The new CachedValue
     */
    public static <T> CachedValue<T> of(UniqueData parent, Class<T> type, T defaultValue, String key) {
        return new CachedValue<>(parent, key, type, defaultValue, (oldValue, newValue) -> newValue);
    }

    /**
     * Create a new CachedValue.
     * This method allows you to specify an update handler that will be called when the value is set.
     * Note that the update handler is fired on each instance when an update is received.
     * Also note that the update handler is not guaranteed to run on a specific thread.
     *
     * @param parent        The parent data object
     * @param type          The type of the value that will be stored
     * @param defaultValue  The default value to use if the value is not present in Redis
     * @param key           The key to use in Redis. (note this will only be part of the real key as other information will be included to ensure uniqueness)
     * @param updateHandler The update handler to use when setting the value
     * @return The new CachedValue
     */
    public static <T> CachedValue<T> of(UniqueData parent, Class<T> type, T defaultValue, String key, UpdateHandler<T> updateHandler) {
        return new CachedValue<>(parent, key, type, defaultValue, updateHandler);
    }

    @Override
    public T get() {
        return value == null ? defaultValue : value;
    }

    @Override
    public void set(T value) {
        CachedValueMetadata metadata = getMetadata();
        DataManager dataManager = metadata.getDataManager();
        value = updateHandler.onUpdate(this.value, value);
        setInternal(value);


        Object serialized = dataManager.serialize(value);
        ThreadUtils.submit(() -> {
            String encoded = DatabaseSupportedType.encode(serialized);
            if (this.value != null) {

                try (Jedis jedis = dataManager.getJedis()) {
                    jedis.set(dataManager.buildCachedValueKey(key, parent), encoded);
                }

            } else {
                try (Jedis jedis = dataManager.getJedis()) {
                    jedis.del(dataManager.buildCachedValueKey(key, parent));
                }
            }
            UniqueDataMetadata uniqueDataMetadata = dataManager.getUniqueDataMetadata(parent.getClass());
            dataManager.getMessenger().broadcastMessageNoPrefix(
                    dataManager.getChannel(this),
                    DataValueUpdateMessageHandler.class,
                    new DataValueUpdateMessage(uniqueDataMetadata.getTopLevelTable(), parent.getId(), getAddress(), encoded)
            );
        });
    }

    /**
     * Set the value in Redis, while providing the jedis instance to use.
     * Note that this method is blocking and will not do any async operations.
     * Call {@link #set(Object)} to set the value asynchronously.
     *
     * @param jedis The jedis instance to use
     * @param value The value to set
     */
    public void set(Jedis jedis, T value) {
        CachedValueMetadata metadata = getMetadata();
        DataManager dataManager = metadata.getDataManager();
        value = updateHandler.onUpdate(this.value, value);
        setInternal(value);

        Object serialized = dataManager.serialize(value);
        String encoded = DatabaseSupportedType.encode(serialized);
        if (this.value != null) {
            jedis.set(dataManager.buildCachedValueKey(key, parent), encoded);
        } else {
            jedis.del(dataManager.buildCachedValueKey(key, parent));
        }


        UniqueDataMetadata uniqueDataMetadata = dataManager.getUniqueDataMetadata(parent.getClass());
        dataManager.getMessenger().broadcastMessageNoPrefix(
                dataManager.getChannel(this),
                DataValueUpdateMessageHandler.class,
                new DataValueUpdateMessage(uniqueDataMetadata.getTopLevelTable(), parent.getId(), getAddress(), encoded)
        );
    }

    @Override
    public void setInternal(Object value) {
        if (Objects.equals(defaultValue, value)) {
            this.value = null;
            return;
        }

        if (!type.isInstance(value)) {
            throw new IllegalArgumentException("Value is not of type " + type.getName());
        }

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
        return CachedValueMetadata.class;
    }

    /**
     * Get the key used in Redis (note this will only be part of the real key as other information will be included to ensure uniqueness).
     * Call {@link DataManager#buildCachedValueKey(String, UniqueData)} to get the full key used.
     *
     * @return The key used in Redis
     */
    public String getKey() {
        return key;
    }

    /**
     * Get the default value to use if the value is not present in Redis.
     *
     * @return The default value
     */
    public T getDefaultValue() {
        return defaultValue;
    }

    private CachedValueMetadata getMetadata() {
        if (metadata == null) {
            UniqueDataMetadata parentMetadata = parent.getDataManager().getUniqueDataMetadata(parent.getClass());
            Set<Field> allCachedValues = ReflectionUtils.getFields(CachedValue.class, parent.getClass());
            for (Field field : allCachedValues) {
                CachedValue<?> value = (CachedValue<?>) ReflectionUtils.getFieldValue(field, parent);
                if (value == this) {
                    metadata = parentMetadata.getMetadata(CachedValueMetadata.class, field);
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
