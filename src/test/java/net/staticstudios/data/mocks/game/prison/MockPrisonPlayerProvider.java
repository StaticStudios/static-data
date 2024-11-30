package net.staticstudios.data.mocks.game.prison;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.DataProvider;

public class MockPrisonPlayerProvider extends DataProvider<MockPrisonPlayer> {
    public MockPrisonPlayerProvider(DataManager dataManager) {
        super(dataManager, MockPrisonPlayer.class);
    }
}
