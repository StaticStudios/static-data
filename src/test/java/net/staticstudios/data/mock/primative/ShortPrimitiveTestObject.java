package net.staticstudios.data.mock.primative;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.data.value.persistent.PersistentValue;
import net.staticstudios.data.data.UniqueData;

import java.util.UUID;

public class ShortPrimitiveTestObject extends UniqueData {
    private final PersistentValue<Short> value = PersistentValue.of(this, Short.class, "value");

    private ShortPrimitiveTestObject(DataManager dataManager, UUID id) {
        super(dataManager, "primitive", "short_test", id);
    }

    public static ShortPrimitiveTestObject createSync(DataManager dataManager, Short initialValue) {
        ShortPrimitiveTestObject obj = new ShortPrimitiveTestObject(dataManager, UUID.randomUUID());
        dataManager.insert(obj, obj.value.initial(initialValue));

        return obj;
    }

    public void setValue(Short value) {
        this.value.set(value);
    }
}
