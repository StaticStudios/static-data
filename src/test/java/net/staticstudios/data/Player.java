package net.staticstudios.data;

import net.staticstudios.data.data.PersistentValue;
import net.staticstudios.data.data.UniqueData;
import net.staticstudios.data.data.collection.PersistentCollection;
import org.jetbrains.annotations.Blocking;

import java.util.Collection;
import java.util.UUID;

public class Player extends UniqueData {
    private final PersistentValue<String> name = PersistentValue.of(this, String.class, "name");
    private final PersistentCollection<HomeLocation> homeHomeLocations = PersistentCollection.of(this, HomeLocation.class, "public", "home_locations", "player_id");
    private final PersistentCollection<Integer> favoriteNumbers = PersistentCollection.of(this, Integer.class, "public", "favorite_numbers", "player_id", "number");

    /**
     * Create a new player object
     * //    private final PersistentValue<String> nickname = PersistentValue.of(this, String.class, "nickname");
     * //    private final OtherData<Backpack> backpack = OtherData.of(this, Backpack.class, "public", "backpacks", "player_id");
     */
    private Player(DataManager dataManager, UUID id) {
        super(dataManager, "public", "players", id);
    }

//    public static Player get(DataManager dataManager, UUID id) {
//        return dataManager.get("public.players", id);
//    }

    @Blocking
    public static Player create(DataManager dataManager, String name) {
        Player player = new Player(dataManager, UUID.randomUUID());
        dataManager.insert(player, player.name.initial(name));

//        Backpack backpack = Backpack.create(dataManager, 9, player.getId());
//        player.setBackpack(player.getId());

        return player;
    }

//    public void setBackpack(UUID id) {
//        this.backpack.setId(id);
//    }

    public String getName() {
        return name.get();
    }

//    public Backpack getBackpack() {
//        return this.backpack.get();
//    }

    public void setName(String name) {
        this.name.set(name);
    }

    public Collection<HomeLocation> getHomeLocations() {
        return homeHomeLocations;
    }

    public Collection<Integer> getFavoriteNumbers() {
        return favoriteNumbers;
    }
}
