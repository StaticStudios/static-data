package net.staticstudios.data.mock;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.PersistentValue;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.parse.Column;
import net.staticstudios.data.parse.Data;

import java.util.UUID;

@Data(schema = "public", table = "users", idColumn = "id")
public class MockUser extends UniqueData {
    private final UUID id;

    public @Column(value = "name") PersistentValue<String> name = PersistentValue.of(this, String.class).withDefault("Unknown");
    public @Column(value = "age", nullable = true) PersistentValue<Integer> age;

    public MockUser(DataManager dataManager, UUID id) {
        super(dataManager);
        this.id = id;
    }

    public static MockUser create(DataManager dataManager, String name) {
        MockUser user = new MockUser(dataManager, UUID.randomUUID());
        dataManager.init(user); //todo: can do this in insert
        dataManager.insert(user, false); //todo: set the name and age
        return user;
    }

    @Override
    public UUID getId() {
        return id;
    }
}
