package net.staticstudios.data.mocks;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.Table;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.value.PersistentValue;

import java.util.UUID;

@Table("public.users")
public abstract class MockUser extends UniqueData {
    private final PersistentValue<String> name = PersistentValue.of(this, String.class, "name");

    @SuppressWarnings("unused")
    protected MockUser() {
    }

    public MockUser(DataManager dataManager, UUID id, String name) {
        super(dataManager, id);
        this.name.setInternal(name);
    }

    public void setName(String name) {
        this.name.set(name);
    }

    public String getName() {
        return name.get();
    }
}
