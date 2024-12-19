package net.staticstudios.data.data;

public class InitialPersistentData implements InitialData<PersistentValue<?>, Object> {
    private final PersistentValue<?> data;
    private final Object value;

    public InitialPersistentData(PersistentValue<?> data, Object value) {
        this.data = data;
        this.value = value;
    }

    public PersistentValue<?> getValue() {
        return data;
    }

    public Object getDataValue() {
        return value;
    }
}
