package net.staticstudios.data.mock.deletions;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.data.UniqueData;
import net.staticstudios.data.data.value.persistent.PersistentValue;

import java.util.UUID;

public class MinecraftSkin extends UniqueData {
    private final PersistentValue<String> name = PersistentValue.of(this, String.class, "name");

    private MinecraftSkin(DataManager dataManager, UUID id) {
        super(dataManager, "minecraft", "skins", id);
    }

    public static MinecraftSkin createSync(DataManager dataManager, String name) {
        MinecraftSkin server = new MinecraftSkin(dataManager, UUID.randomUUID());
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
