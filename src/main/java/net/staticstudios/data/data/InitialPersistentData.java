package net.staticstudios.data.data;

public class InitialPersistentData implements InitialData<PersistentData<?>, Object> {
    private final PersistentData<?> data;
    private final Object value;

    public InitialPersistentData(PersistentData<?> data, Object value) {
        this.data = data;
        this.value = value;
    }

    @Override
    public PersistentData<?> getData() {
        return data;
    }

    @Override
    public Object getValue() {
        return value;
    }
}
