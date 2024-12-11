package net.staticstudios.data;

import net.staticstudios.data.data.PersistentValue;
import net.staticstudios.data.data.UniqueData;
import org.jetbrains.annotations.Blocking;

import java.util.UUID;

public class Backpack extends UniqueData {
    private final PersistentValue<Integer> size = PersistentValue.of(this, Integer.class, "size");
    private final PersistentValue<UUID> playerId = PersistentValue.of(this, UUID.class, "player_id");

    /**
     * Create a new player object
     */
    private Backpack(DataManager dataManager, UUID id) {
        super(dataManager, "public", "backpacks", id);
    }

    public static Backpack get(DataManager dataManager, UUID id) {
        return new Backpack(dataManager, id);
    }

    @Blocking
    public static Backpack create(DataManager dataManager, int size, UUID playerId) {
        Backpack backpack = new Backpack(dataManager, UUID.randomUUID());
        dataManager.insert(backpack, backpack.size.initial(size), backpack.playerId.initial(playerId));


        return backpack;
    }

    public Integer getSize() {
        return size.get();
    }

    public void setSize(Integer size) {
        this.size.set(size);
    }
}
