package net.staticstudios.data.mock.primative;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.PersistentValue;
import net.staticstudios.data.UniqueData;

import java.util.UUID;

public class CharacterPrimitiveTestObject extends UniqueData {
    private final PersistentValue<Character> value = PersistentValue.of(this, Character.class, "value");

    private CharacterPrimitiveTestObject(DataManager dataManager, UUID id) {
        super(dataManager, "primitive", "character_test", id);
    }

    public static CharacterPrimitiveTestObject createSync(DataManager dataManager, Character initialValue) {
        CharacterPrimitiveTestObject obj = new CharacterPrimitiveTestObject(dataManager, UUID.randomUUID());
        dataManager.insert(obj, obj.value.initial(initialValue));

        return obj;
    }

    public void setValue(Character value) {
        this.value.set(value);
    }
}
