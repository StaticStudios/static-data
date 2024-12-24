package net.staticstudios.data.mock.primative;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.data.value.persistent.PersistentValue;
import net.staticstudios.data.data.UniqueData;

import java.util.UUID;

public class BytePrimitiveTestObject extends UniqueData {
    private final PersistentValue<Byte> value = PersistentValue.of(this, Byte.class, "value");

    private BytePrimitiveTestObject(DataManager dataManager, UUID id) {
        super(dataManager, "primitive", "byte_test", id);
    }

    public static BytePrimitiveTestObject createSync(DataManager dataManager, Byte initialValue) {
        BytePrimitiveTestObject obj = new BytePrimitiveTestObject(dataManager, UUID.randomUUID());
        dataManager.insert(obj, obj.value.initial(initialValue));

        return obj;
    }

    public void setValue(Byte value) {
        this.value.set(value);
    }
}
