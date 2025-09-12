package net.staticstudios.data.mock.deletions;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.PersistentValue;
import net.staticstudios.data.UniqueData;

import java.util.UUID;

public class MinecraftServer extends UniqueData {
    private final PersistentValue<String> name = PersistentValue.of(this, String.class, "name");

    private MinecraftServer(DataManager dataManager, UUID id) {
        super(dataManager, "minecraft", "servers", id);
    }

    public static MinecraftServer createSync(DataManager dataManager, String name) {
        MinecraftServer server = new MinecraftServer(dataManager, UUID.randomUUID());
        dataManager.insert(server, server.name.initial(name));

        return server;
    }

    public String getName() {
        return name.get();
    }

    public void setName(String name) {
        this.name.set(name);
    }
}
