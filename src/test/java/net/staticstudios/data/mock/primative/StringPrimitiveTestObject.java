package net.staticstudios.data.mock.primative;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.data.PersistentValue;
import net.staticstudios.data.data.UniqueData;

import java.util.UUID;

public class StringPrimitiveTestObject extends UniqueData {
    private final PersistentValue<String> value = PersistentValue.of(this, String.class, "value");

    private StringPrimitiveTestObject(DataManager dataManager, UUID id) {
        super(dataManager, "primitive", "string_test", id);
    }

    public static StringPrimitiveTestObject createSync(DataManager dataManager, String initialValue) {
        StringPrimitiveTestObject obj = new StringPrimitiveTestObject(dataManager, UUID.randomUUID());
        dataManager.insert(obj, obj.value.initial(initialValue));

        return obj;
    }

    public void setValue(String value) {
        this.value.set(value);
    }
}
