package net.staticstudios.data.mocks.user;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.Table;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.value.CachedValue;
import net.staticstudios.data.value.PersistentValue;
import net.staticstudios.data.mocks.MockOnlineStatus;

import java.util.UUID;

@Table("public.users")
public abstract class MockUser extends UniqueData {
    private final PersistentValue<String> name = PersistentValue.of(this, String.class, "name");
    private final CachedValue<MockOnlineStatus> onlineStatus = CachedValue.of(this, MockOnlineStatus.class, MockOnlineStatus.OFFLINE, "online_status");

    @SuppressWarnings("unused")
    protected MockUser() {
    }

    public MockUser(DataManager dataManager, UUID id, String name) {
        super(dataManager, id);
        this.name.setInternal(name);
        this.onlineStatus.setInternal(MockOnlineStatus.ONLINE);
    }

    public void setName(String name) {
        this.name.set(name);
    }

    public String getName() {
        return name.get();
    }

    public MockOnlineStatus getOnlineStatus() {
        return onlineStatus.get();
    }

    public void setOnlineStatus(MockOnlineStatus onlineStatus) {
        this.onlineStatus.set(onlineStatus);
    }
}
