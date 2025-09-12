package net.staticstudios.data.data.value;

import net.staticstudios.data.CachedValue;
import net.staticstudios.data.data.InitialValue;

public class InitialCachedValue implements InitialValue<CachedValue<?>, Object> {
    private final CachedValue<?> value;
    private final Object initialDataValue;

    public InitialCachedValue(CachedValue<?> value, Object initialDataValue) {
        this.value = value;
        this.initialDataValue = initialDataValue;
    }

    public CachedValue<?> getValue() {
        return value;
    }

    public Object getInitialDataValue() {
        return initialDataValue;
    }
}
