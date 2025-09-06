package net.staticstudios.data.misc;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.util.DataSourceConfig;

public record MockEnvironment(
        DataSourceConfig dataSourceConfig,
        DataManager dataManager
) {
}
