package net.staticstudios.data.mocks.game.prison;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.Table;
import net.staticstudios.data.value.PersistentValue;
import net.staticstudios.data.mocks.MockLocation;
import net.staticstudios.data.mocks.game.MockGenericPlayer;

import java.util.UUID;

@Table("prison.players")
public class MockPrisonPlayer extends MockGenericPlayer {
    private final PersistentValue<Long> prisonCoins = PersistentValue.withDefault(this, Long.class, 100L, "prison_coins");

    @SuppressWarnings("unused")
    private MockPrisonPlayer() {
        super();
    }

    public MockPrisonPlayer(DataManager dataManager, String name, MockLocation... homeLocations) {
        this(dataManager, UUID.randomUUID(), name, homeLocations);
    }

    public MockPrisonPlayer(DataManager dataManager, UUID id, String name, MockLocation... homeLocations) {
        super(dataManager, id, name, homeLocations);
        dataManager.insert(this);
    }

    public long getPrisonCoins() {
        return prisonCoins.get();
    }

    public void setPrisonCoins(long prisonCoins) {
        this.prisonCoins.set(prisonCoins);
    }
}
