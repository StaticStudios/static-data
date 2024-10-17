package net.staticstudios.data.mocks.game.skyblock;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.Table;
import net.staticstudios.data.value.PersistentValue;
import net.staticstudios.data.mocks.MockLocation;
import net.staticstudios.data.mocks.game.MockGenericPlayer;

import java.util.UUID;

@Table("skyblock.players")
public class MockSkyblockPlayer extends MockGenericPlayer {
    private final PersistentValue<Long> skyCoins = PersistentValue.withDefault(this, Long.class, 100L, "sky_coins");

    @SuppressWarnings("unused")
    private MockSkyblockPlayer() {
        super();
    }

    public MockSkyblockPlayer(DataManager dataManager, String name, MockLocation... homeLocations) {
        this(dataManager, UUID.randomUUID(), name, homeLocations);
    }

    public MockSkyblockPlayer(DataManager dataManager, UUID id, String name, MockLocation... homeLocations) {
        super(dataManager, id, name, homeLocations);
        dataManager.insert(this);
    }

    public long getSkyCoins() {
        return skyCoins.get();
    }

    public void setSkyCoins(long skyCoins) {
        this.skyCoins.set(skyCoins);
    }
}
