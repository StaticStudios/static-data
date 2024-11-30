package net.staticstudios.data.mocks.discord;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.DataProvider;

import java.util.UUID;

public class MockDiscordUserStatsProvider extends DataProvider<MockDiscordUserStats> {
    public MockDiscordUserStatsProvider(DataManager dataManager) {
        super(dataManager, MockDiscordUserStats.class);
    }

    public MockDiscordUserStats createStats() {
        MockDiscordUserStats stats = new MockDiscordUserStats(getDataManager(), UUID.randomUUID());
        set(stats);
        return stats;
    }
}
