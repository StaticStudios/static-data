package net.staticstudios.data.mock.primative;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.data.PersistentValue;
import net.staticstudios.data.data.UniqueData;

import java.util.UUID;

public class BooleanPrimitiveTestObject extends UniqueData {
    private final PersistentValue<Boolean> value = PersistentValue.of(this, Boolean.class, "value");

    private BooleanPrimitiveTestObject(DataManager dataManager, UUID id) {
        super(dataManager, "primitive", "boolean_test", id);
    }

    public static BooleanPrimitiveTestObject createSync(DataManager dataManager, Boolean initialValue) {
        BooleanPrimitiveTestObject obj = new BooleanPrimitiveTestObject(dataManager, UUID.randomUUID());
        dataManager.insert(obj, obj.value.initial(initialValue));

        return obj;
    }

    public void setValue(Boolean value) {
        this.value.set(value);
    }
}
