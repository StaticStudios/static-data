package net.staticstudios.data;

import net.staticstudios.data.data.DataHolder;
import net.staticstudios.data.data.value.InitialCachedValue;
import net.staticstudios.data.data.value.Value;
import net.staticstudios.data.impl.CachedValueManager;
import net.staticstudios.data.key.RedisKey;
import net.staticstudios.data.primative.Primitive;
import net.staticstudios.data.primative.Primitives;
import net.staticstudios.data.util.DataDoesNotExistException;
import net.staticstudios.data.util.DeletionStrategy;
import net.staticstudios.data.util.ValueUpdate;
import net.staticstudios.data.util.ValueUpdateHandler;
import net.staticstudios.utils.ThreadUtils;
import org.jetbrains.annotations.Blocking;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

/**
 * Represents a value that lives in Redis.
 * Data stored in {@link CachedValue} objects should be considered volatile and may be deleted at any time.
 *
 * @param <T> the type of data stored in this value
 */
public class CachedValue<T> implements Value<T> {
    private final String identifyingKey;
    private final Class<T> dataType;
    private final DataHolder holder;
    private final DataManager dataManager;
    private int expirySeconds = -1;
    private Supplier<T> fallbackValue;
    private DeletionStrategy deletionStrategy;

    private CachedValue(String key, Class<T> dataType, DataHolder holder, DataManager dataManager) {
        if (!holder.getDataManager().isSupportedType(dataType)) {
            throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }

        this.identifyingKey = key;
        this.dataType = dataType;
        this.holder = holder;
        this.dataManager = dataManager;
    }

    /**
     * Create a new {@link CachedValue} object.
     *
     * @param holder   the holder of this value
     * @param dataType the type of data stored in this value
     * @param key      the identifying key for this value (note that this is only part of what is used to construct the Redis key, see {@link RedisKey#toString()})
     * @param <T>      the type of data stored in this value
     * @return the new {@link CachedValue} object
     */
    public static <T> CachedValue<T> of(UniqueData holder, Class<T> dataType, String key) {
        CachedValue<T> cv = new CachedValue<>(key, dataType, holder, holder.getDataManager());
        cv.deletionStrategy = DeletionStrategy.CASCADE;
        return cv;
    }

    /**
     * Set the initial value for this object
     *
     * @param value the initial value
     * @return the initial value object
     */
    public InitialCachedValue initial(@Nullable T value) {
        return new InitialCachedValue(this, value);
    }

    /**
     * Add an update handler to this value.
     * Note that update handlers are run asynchronously.
     *
     * @param updateHandler the update handler
     * @return this
     */
    @SuppressWarnings("unchecked")
    public CachedValue<T> onUpdate(ValueUpdateHandler<T> updateHandler) {
        dataManager.registerValueUpdateHandler(this.getKey(), update -> ThreadUtils.submit(() -> updateHandler.handle((ValueUpdate<T>) update)));
        return this;
    }

    /**
     * Set the expiry time for this value.
     * After the expiry time has passed, the value will be deleted from Redis, and this {@link CachedValue} object will return the fallback value.
     *
     * @param seconds the number of seconds before the value expires, or -1 to disable expiry
     * @return this
     */
    public CachedValue<T> withExpiry(int seconds) {
        this.expirySeconds = seconds;
        return this;
    }

    /**
     * Fallback values are similar to default values, however they are not stored.
     * The fallback value is returned by {@link #get()} if the value does not exist, or it is null.
     * Unlike default values, fallback values are never stored.
     *
     * @param fallbackValue the value to fall back on
     * @return this
     */
    public CachedValue<T> withFallback(Supplier<T> fallbackValue) {
        this.fallbackValue = fallbackValue;
        return this;
    }

    /**
     * Fallback values are similar to default values, however they are not stored.
     * The fallback value is returned by {@link #get()} if the value does not exist, or it is null.
     * Unlike default values, fallback values are never stored.
     *
     * @param fallbackValue the value to fall back on
     * @return this
     */
    public CachedValue<T> withFallback(T fallbackValue) {
        this.fallbackValue = () -> fallbackValue;
        return this;
    }

    @SuppressWarnings("unchecked")
    private T getFallbackValue() {
        T fallbackValue = this.fallbackValue == null ? null : this.fallbackValue.get();
        if (fallbackValue == null) {
            if (!Primitives.isPrimitive(dataType)) {
                return null;
            }

            Primitive<?> primitive = Primitives.getPrimitive(dataType);

            if (primitive.isNullable()) {
                return null;
            }

            return (T) primitive.getDefaultValue();
        }

        return fallbackValue;
    }

    /**
     * Get the expiry time for this value.
     *
     * @return the expiry time in seconds, or -1 if expiry is disabled
     */
    public int getExpirySeconds() {
        return expirySeconds;
    }

    @Override
    public RedisKey getKey() {
        return new RedisKey(this);
    }

    public T get() {
        T value;
        try {
            value = getDataManager().get(this);
        } catch (DataDoesNotExistException e) {
            value = null;
        }

        return value == null ? getFallbackValue() : value;
    }

    public void set(T value) {
        CachedValueManager manager = dataManager.getCachedValueManager();
        dataManager.cache(this.getKey(), dataType, value, Instant.now());
        getDataManager().submitAsyncTask((connection, jedis) -> manager.setInRedis(jedis, List.of(initial(value))));
    }

    /**
     * Set the value of this object.
     * This method blocks until the value has been set in Redis.
     *
     * @param value the value to set
     */
    @Blocking
    public void setNow(T value) {
        CachedValueManager manager = dataManager.getCachedValueManager();
        dataManager.cache(this.getKey(), dataType, value, Instant.now());
        getDataManager().submitBlockingTask((connection, jedis) -> manager.setInRedis(jedis, List.of(initial(value))));
    }

    /**
     * Get the identifying key for this value.
     * Note that this is only part of what is used to construct the Redis key, see {@link RedisKey#toString()}
     *
     * @return the identifying key
     */
    public String getIdentifyingKey() {
        return identifyingKey;
    }

    @Override
    public Class<T> getDataType() {
        return dataType;
    }

    @Override
    public DataManager getDataManager() {
        return dataManager;
    }

    @Override
    public DataHolder getHolder() {
        return holder;
    }

    @Override
    public String toString() {
        return "CachedValue{" +
                "identifyingKey='" + identifyingKey +
                '}';
    }

    @Override
    public int hashCode() {
        return getKey().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof CachedValue<?> other)) {
            return false;
        }

        return getKey().equals(other.getKey());
    }

    @Override
    public CachedValue<T> deletionStrategy(DeletionStrategy strategy) {
        this.deletionStrategy = strategy;
        return this;
    }

    @Override
    public @NotNull DeletionStrategy getDeletionStrategy() {
        return deletionStrategy == null ? DeletionStrategy.NO_ACTION : deletionStrategy;
    }
}
