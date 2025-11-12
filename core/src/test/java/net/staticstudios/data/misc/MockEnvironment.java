package net.staticstudios.data.misc;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.StaticDataConfig;

public record MockEnvironment(
        StaticDataConfig config,
        DataManager dataManager
) {
}
