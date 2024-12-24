package net.staticstudios.data.mock.primative;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.data.PersistentValue;
import net.staticstudios.data.data.UniqueData;

import java.util.UUID;

public class LongPrimitiveTestObject extends UniqueData {
    private final PersistentValue<Long> value = PersistentValue.of(this, Long.class, "value");

    private LongPrimitiveTestObject(DataManager dataManager, UUID id) {
        super(dataManager, "primitive", "long_test", id);
    }

    public static LongPrimitiveTestObject createSync(DataManager dataManager, Long initialValue) {
        LongPrimitiveTestObject obj = new LongPrimitiveTestObject(dataManager, UUID.randomUUID());
        dataManager.insert(obj, obj.value.initial(initialValue));

        return obj;
    }

    public void setValue(Long value) {
        this.value.set(value);
    }
}
