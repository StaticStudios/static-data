package net.staticstudios.data.mocks.game.prison;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.mocks.game.MockPlayerProvider;

public class MockPrisonPlayerProvider extends MockPlayerProvider<MockPrisonPlayer> {
    public MockPrisonPlayerProvider(DataManager dataManager) {
        super(dataManager);
    }

    @Override
    public Class<MockPrisonPlayer> getDataType() {
        return MockPrisonPlayer.class;
    }
}
