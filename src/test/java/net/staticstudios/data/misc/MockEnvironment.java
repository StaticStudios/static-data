package net.staticstudios.data.misc;

import com.zaxxer.hikari.HikariConfig;
import net.staticstudios.data.DataManager;

public record MockEnvironment(
        HikariConfig hikariConfig,
        DataManager dataManager
) {
}
