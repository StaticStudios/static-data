package net.staticstudios.data.mocks.game.skyblock;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.mocks.game.MockPlayerProvider;

public class MockSkyblockPlayerProvider extends MockPlayerProvider<MockSkyblockPlayer> {
    public MockSkyblockPlayerProvider(DataManager dataManager) {
        super(dataManager);
    }

    @Override
    public Class<MockSkyblockPlayer> getDataType() {
        return MockSkyblockPlayer.class;
    }
}
