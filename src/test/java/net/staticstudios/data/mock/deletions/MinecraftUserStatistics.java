package net.staticstudios.data.mock.deletions;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.data.UniqueData;

import java.util.UUID;

public class MinecraftUserStatistics extends UniqueData {
    private MinecraftUserStatistics(DataManager dataManager, UUID id) {
        super(dataManager, "minecraft", "user_stats", id);
    }

    public static MinecraftUserStatistics createSync(DataManager dataManager, UUID userId) {
        MinecraftUserStatistics stats = new MinecraftUserStatistics(dataManager, userId);
        dataManager.insert(stats);

        return stats;
    }
}
