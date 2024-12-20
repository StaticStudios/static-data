package net.staticstudios.data;

import com.zaxxer.hikari.HikariConfig;

public record MockEnvironment(
        HikariConfig hikariConfig,
        DataManager dataManager
) {
}
