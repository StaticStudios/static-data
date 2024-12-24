package net.staticstudios.data.data.value.redis;

import net.staticstudios.data.data.InitialValue;

public class InitialRedisValue implements InitialValue<RedisValue<?>, Object> {
    private final RedisValue<?> value;
    private final Object initialDataValue;

    public InitialRedisValue(RedisValue<?> value, Object initialDataValue) {
        this.value = value;
        this.initialDataValue = initialDataValue;
    }

    public RedisValue<?> getValue() {
        return value;
    }

    public Object getInitialDataValue() {
        return initialDataValue;
    }
}
