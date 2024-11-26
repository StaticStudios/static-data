package net.staticstudios.data.mocks.game;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.Table;
import net.staticstudios.data.mocks.MockBlockLocation;
import net.staticstudios.data.mocks.MockLocation;
import net.staticstudios.data.mocks.MockUser;
import net.staticstudios.data.value.PersistentCollection;
import net.staticstudios.data.value.PersistentValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

@Table("public.players")
public abstract class MockGenericPlayer extends MockUser {
    private final PersistentCollection<MockLocation> homeLocations = PersistentCollection.of(this, MockLocation.class, "home_locations", "player_id");
    private final PersistentValue<Long> money = PersistentValue.withDefault(this, Long.class, 0L, "money");
    private final PersistentValue<MockBlockLocation> favoriteBlock = PersistentValue.withDefault(this, MockBlockLocation.class, new MockBlockLocation(0, 0, 0), "favorite_block");

    @SuppressWarnings("unused")
    protected MockGenericPlayer() {
        super();
    }

    public MockGenericPlayer(DataManager dataManager, UUID id, String name, MockLocation... homeLocations) {
        super(dataManager, id, name);

        List<MockLocation> homeLocationsList = new ArrayList<>(homeLocations.length);
        homeLocationsList.addAll(Arrays.asList(homeLocations));
        this.homeLocations.setInitialElements(homeLocationsList);
    }

    public void addHomeLocation(DataManager dataManager, int x, int y, int z) {
        homeLocations.add(new MockLocation(dataManager, x, y, z));
    }

    public void setMoney(long money) {
        this.money.set(money);
    }

    public void setFavoriteBlock(int x, int y, int z) {
        favoriteBlock.set(new MockBlockLocation(x, y, z));
    }

    public PersistentCollection<MockLocation> getHomeLocations() {
        return homeLocations;
    }

    public void removeHomeLocation(DataManager dataManager, int x, int y, int z) {
        homeLocations.remove(getHomeLocations().stream().filter(location -> location.getX() == x && location.getY() == y && location.getZ() == z).findFirst().orElse(null));
    }

    public long getMoney() {
        return money.get();
    }

    public MockBlockLocation getFavoriteBlock() {
        return favoriteBlock.get();
    }

    @Override
    public String toString() {
        return "MockPlayer{" +
                "id=" + getId() +
                "homeLocations=" + homeLocations +
                ", money=" + money +
                ", favoriteBlock=" + favoriteBlock +
                "} " + super.toString();

    }
}
