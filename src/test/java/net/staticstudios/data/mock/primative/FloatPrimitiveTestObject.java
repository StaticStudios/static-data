package net.staticstudios.data.mock.primative;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.data.value.persistent.PersistentValue;
import net.staticstudios.data.data.UniqueData;

import java.util.UUID;

public class FloatPrimitiveTestObject extends UniqueData {
    private final PersistentValue<Float> value = PersistentValue.of(this, Float.class, "value");

    private FloatPrimitiveTestObject(DataManager dataManager, UUID id) {
        super(dataManager, "primitive", "float_test", id);
    }

    public static FloatPrimitiveTestObject createSync(DataManager dataManager, Float initialValue) {
        FloatPrimitiveTestObject obj = new FloatPrimitiveTestObject(dataManager, UUID.randomUUID());
        dataManager.insert(obj, obj.value.initial(initialValue));

        return obj;
    }

    public void setValue(Float value) {
        this.value.set(value);
    }
}
