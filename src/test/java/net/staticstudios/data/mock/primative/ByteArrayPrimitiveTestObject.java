package net.staticstudios.data.mock.primative;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.data.value.persistent.PersistentValue;
import net.staticstudios.data.data.UniqueData;

import java.util.UUID;

public class ByteArrayPrimitiveTestObject extends UniqueData {
    private final PersistentValue<byte[]> value = PersistentValue.of(this, byte[].class, "value");

    private ByteArrayPrimitiveTestObject(DataManager dataManager, UUID id) {
        super(dataManager, "primitive", "byte_array_test", id);
    }

    public static ByteArrayPrimitiveTestObject createSync(DataManager dataManager, byte[] initialValue) {
        ByteArrayPrimitiveTestObject obj = new ByteArrayPrimitiveTestObject(dataManager, UUID.randomUUID());
        dataManager.insert(obj, obj.value.initial(initialValue));

        return obj;
    }

    public void setValue(byte[] value) {
        this.value.set(value);
    }
}
