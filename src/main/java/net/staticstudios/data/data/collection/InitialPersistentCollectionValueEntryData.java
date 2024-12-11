package net.staticstudios.data.data.collection;

import net.staticstudios.data.data.InitialData;

public class InitialPersistentCollectionValueEntryData implements InitialData<PersistentValueCollection<?>, Object> {
    private final PersistentValueCollection<?> keyed;
    private final Object value;

    public InitialPersistentCollectionValueEntryData(PersistentValueCollection<?> keyed, Object value) {
        this.keyed = keyed;
        this.value = value;
    }

    @Override
    public PersistentValueCollection<?> getKeyed() {
        return keyed;
    }

    @Override
    public Object getValue() {
        return value;
    }
}
