package net.staticstudios.data.mock.primative;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.data.value.persistent.PersistentValue;
import net.staticstudios.data.data.UniqueData;

import java.util.UUID;

public class IntegerPrimitiveTestObject extends UniqueData {
    private final PersistentValue<Integer> value = PersistentValue.of(this, Integer.class, "value");

    private IntegerPrimitiveTestObject(DataManager dataManager, UUID id) {
        super(dataManager, "primitive", "integer_test", id);
    }

    public static IntegerPrimitiveTestObject createSync(DataManager dataManager, Integer initialValue) {
        IntegerPrimitiveTestObject obj = new IntegerPrimitiveTestObject(dataManager, UUID.randomUUID());
        dataManager.insert(obj, obj.value.initial(initialValue));

        return obj;
    }

    public void setValue(Integer value) {
        this.value.set(value);
    }
}
