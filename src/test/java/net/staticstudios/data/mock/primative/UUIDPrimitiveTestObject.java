package net.staticstudios.data.mock.primative;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.data.value.persistent.PersistentValue;
import net.staticstudios.data.data.UniqueData;

import java.util.UUID;

public class UUIDPrimitiveTestObject extends UniqueData {
    private final PersistentValue<UUID> value = PersistentValue.of(this, UUID.class, "value");

    private UUIDPrimitiveTestObject(DataManager dataManager, UUID id) {
        super(dataManager, "primitive", "uuid_test", id);
    }

    public static UUIDPrimitiveTestObject createSync(DataManager dataManager, UUID initialValue) {
        UUIDPrimitiveTestObject obj = new UUIDPrimitiveTestObject(dataManager, UUID.randomUUID());
        dataManager.insert(obj, obj.value.initial(initialValue));

        return obj;
    }

    public void setValue(UUID value) {
        this.value.set(value);
    }
}
