package net.staticstudios.data.data.value.redis;

import net.staticstudios.data.DataDoesNotExistException;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.ValueUpdateHandler;
import net.staticstudios.data.data.DataHolder;
import net.staticstudios.data.data.UniqueData;
import net.staticstudios.data.data.value.Value;
import net.staticstudios.data.impl.RedisValueManager;
import net.staticstudios.data.key.RedisKey;
import net.staticstudios.data.primative.Primitive;
import net.staticstudios.data.primative.Primitives;

import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

public class RedisValue<T> implements Value<T> {
    private final String identifyingKey;
    private final Class<T> dataType;
    private final DataHolder holder;
    private final DataManager dataManager;
    private int expirySeconds = -1;
    private Supplier<T> fallbackValue;

    public RedisValue(String key, Class<T> dataType, DataHolder holder, DataManager dataManager) {
        if (!holder.getDataManager().isSupportedType(dataType)) {
            throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }

        this.identifyingKey = key;
        this.dataType = dataType;
        this.holder = holder;
        this.dataManager = dataManager;
    }

    public static <T> RedisValue<T> of(UniqueData holder, Class<T> dataType, String key) {
        return new RedisValue<>(key, dataType, holder, holder.getDataManager());
    }

    public InitialRedisValue initial(T value) {
        return new InitialRedisValue(this, value);
    }

    public RedisValue<T> onUpdate(ValueUpdateHandler<T> updateHandler) {
        dataManager.registerValueUpdateHandler(this.getKey(), updateHandler);
        return this;
    }

    public RedisValue<T> withExpiry(int seconds) {
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
    public RedisValue<T> withFallback(Supplier<T> fallbackValue) {
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
    public RedisValue<T> withFallback(T fallbackValue) {
        this.fallbackValue = () -> fallbackValue;
        return this;
    }
    
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


    public int getExpirySeconds() {
        return expirySeconds;
    }

    @Override
    public RedisKey getKey() {
        return new RedisKey(this);
    }

    public T get() {
        try {
            return getDataManager().get(this);
        } catch (DataDoesNotExistException e) {
            return getFallbackValue();
        }
    }

    public void set(T value) {
        RedisValueManager manager = RedisValueManager.getInstance();
        dataManager.cache(this.getKey(), dataType, value, Instant.now());

        try {
            manager.setInRedis(List.of(initial(value)));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

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
        return "RedisValue{" +
                "identifyingKey='" + identifyingKey +
                '}';
    }
}
