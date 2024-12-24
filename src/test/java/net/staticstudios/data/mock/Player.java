package net.staticstudios.data.mock;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.data.value.persistent.PersistentValue;
import net.staticstudios.data.data.Reference;
import net.staticstudios.data.data.UniqueData;
import net.staticstudios.data.data.collection.PersistentCollection;

import java.util.Collection;
import java.util.UUID;

public class Player extends UniqueData {
    private final PersistentValue<String> name = PersistentValue.of(this, String.class, "name");
    private final PersistentValue<String> nickname = PersistentValue.foreign(this, String.class, "public.players_2.nickname", "player_id");
    private final Reference<Backpack> backpack = Reference.of(this, Backpack.class, "backpack_id");
    private final Reference<Island> island = Reference.of(this, Island.class, "island_id");


    private final PersistentCollection<HomeLocation> homeLocations = PersistentCollection.oneToMany(this, HomeLocation.class, "public", "home_locations", "player_id");
    private final PersistentCollection<Integer> favoriteNumbers = PersistentCollection.of(this, Integer.class, "public", "favorite_numbers", "player_id", "number");

    private Player(DataManager dataManager, UUID id) {
        super(dataManager, "public", "players", id);
    }

    public static Player createAsync(DataManager dataManager, String name) {
        Player player = new Player(dataManager, UUID.randomUUID());
        dataManager.insertAsync(player, player.name.initial(name));

        return player;
    }

    public static Player createSync(DataManager dataManager, String name) {
        Player player = new Player(dataManager, UUID.randomUUID());
        dataManager.insert(player, player.name.initial(name));

        return player;
    }

    public void setBackpack(Backpack backpack) {
        this.backpack.set(backpack);
    }

    public String getName() {
        return name.get();
    }

    public Backpack getBackpack() {
        return this.backpack.get();
    }

    public void setName(String name) {
        this.name.set(name);
    }

    public void setNickname(String nickname) {
        this.nickname.set(nickname);
    }

    public String getNickname() {
        return nickname.get();
    }

    public Collection<HomeLocation> getHomeLocations() {
        return homeLocations;
    }

    public Collection<Integer> getFavoriteNumbers() {
        return favoriteNumbers;
    }

    public Island getIsland() {
        return this.island.get();
    }
}
