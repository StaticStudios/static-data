package net.staticstudios.data.v2;

public class InitialPersistentData implements InitialData<PersistentData<?>> {
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
