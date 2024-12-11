package net.staticstudios.data;

import net.staticstudios.data.v2.DataManager;
import net.staticstudios.data.v2.OtherData;
import net.staticstudios.data.v2.PersistentValue;
import net.staticstudios.data.v2.UniqueData;
import org.jetbrains.annotations.Blocking;

import java.util.UUID;

public class Player extends UniqueData {
    private final PersistentValue<String> name = PersistentValue.of(this, String.class, "name");
    private final PersistentValue<String> nickname = PersistentValue.of(this, String.class, "nickname");
    private final OtherData<Backpack> backpack = OtherData.of(this, Backpack.class, "public", "backpacks", "player_id");
//    private final OneToOne<Backpack> backpack = OtherData.of(this, Backpack.class, "public", "backpacks", "player_id");
//    private final OneToMany<Backpack> backpack = OtherData.of(this, Backpack.class, "public", "backpacks", "player_id");

    /**
     * Create a new player object
     */
    private Player(DataManager dataManager, UUID id) {
        super(dataManager, "public", "players", id);
    }

    public static Player get(DataManager dataManager, UUID id) {
        return dataManager.get("public.players", id);
    }

    @Blocking
    public static Player create(DataManager dataManager, String name) {
        Player player = new Player(dataManager, UUID.randomUUID());
        dataManager.insert(player, player.name.initial(name));

        Backpack backpack = Backpack.create(dataManager, 9, player.getId());
        player.setBackpack(player.getId());

        return player;
    }

    public void setBackpack(UUID id) {
        this.backpack.setId(id);
    }

    public String getName() {
        return name.get();
    }

    public int getBackpackSize() {
        return this.backpack.get().getSize();
    }

    public void setName(String name) {
        this.name.set(name);
    }
}
