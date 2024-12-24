package net.staticstudios.data.mock.primative;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.data.PersistentValue;
import net.staticstudios.data.data.UniqueData;

import java.sql.Timestamp;
import java.util.UUID;

public class TimestampPrimitiveTestObject extends UniqueData {
    private final PersistentValue<Timestamp> value = PersistentValue.of(this, Timestamp.class, "value");

    private TimestampPrimitiveTestObject(DataManager dataManager, UUID id) {
        super(dataManager, "primitive", "timestamp_test", id);
    }

    public static TimestampPrimitiveTestObject createSync(DataManager dataManager, Timestamp initialValue) {
        TimestampPrimitiveTestObject obj = new TimestampPrimitiveTestObject(dataManager, UUID.randomUUID());
        dataManager.insert(obj, obj.value.initial(initialValue));

        return obj;
    }

    public void setValue(Timestamp value) {
        this.value.set(value);
    }
}
