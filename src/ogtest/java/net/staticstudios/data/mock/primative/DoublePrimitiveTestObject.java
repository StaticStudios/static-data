package net.staticstudios.data.mock.primative;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.PersistentValue;
import net.staticstudios.data.UniqueData;

import java.util.UUID;

public class DoublePrimitiveTestObject extends UniqueData {
    private final PersistentValue<Double> value = PersistentValue.of(this, Double.class, "value");

    private DoublePrimitiveTestObject(DataManager dataManager, UUID id) {
        super(dataManager, "primitive", "double_test", id);
    }

    public static DoublePrimitiveTestObject createSync(DataManager dataManager, Double initialValue) {
        DoublePrimitiveTestObject obj = new DoublePrimitiveTestObject(dataManager, UUID.randomUUID());
        dataManager.insert(obj, obj.value.initial(initialValue));

        return obj;
    }

    public void setValue(Double value) {
        this.value.set(value);
    }
}
