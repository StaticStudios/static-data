package net.staticstudios.data.mock;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.data.UniqueData;
import net.staticstudios.data.data.collection.PersistentCollection;
import net.staticstudios.data.data.collection.SimplePersistentCollection;
import net.staticstudios.data.data.value.persistent.PersistentValue;
import org.jetbrains.annotations.Blocking;

import java.util.UUID;

public class Island extends UniqueData {
    private final SimplePersistentCollection<Player> members = PersistentCollection.oneToMany(this, Player.class, "public", "players", "island_id");
    private final PersistentValue<String> name = PersistentValue.of(this, String.class, "name");


    private Island(DataManager dataManager, UUID id) {
        super(dataManager, "public", "islands", id);
    }

    public static Island get(DataManager dataManager, UUID id) {
        return new Island(dataManager, id);
    }

    @Blocking
    public static Island create(DataManager dataManager, String name) {
        Island island = new Island(dataManager, UUID.randomUUID());
        dataManager.insert(island, island.name.initial(name));

        return island;
    }

    public String getName() {
        return name.get();
    }

    public void setName(String name) {
        this.name.set(name);
    }

    public SimplePersistentCollection<Player> getMembers() {
        return members;
    }
}
