package net.staticstudios.data.data.value.persistent;

import net.staticstudios.data.data.InitialValue;

public class InitialPersistentValue implements InitialValue<PersistentValue<?>, Object> {
    private final PersistentValue<?> value;
    private final Object initialDataValue;

    public InitialPersistentValue(PersistentValue<?> value, Object initialDataValue) {
        this.value = value;
        this.initialDataValue = initialDataValue;
    }

    public PersistentValue<?> getValue() {
        return value;
    }

    public Object getInitialDataValue() {
        return initialDataValue;
    }
}
