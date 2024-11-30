package net.staticstudios.data.mocks.game.skyblock;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.DataProvider;

public class MockSkyblockPlayerProvider extends DataProvider<MockSkyblockPlayer> {
    public MockSkyblockPlayerProvider(DataManager dataManager) {
        super(dataManager, MockSkyblockPlayer.class);
    }
}
