package net.staticstudios.data.util;

public record DataSourceConfig(
        String databaseHost,
        int databasePort,
        String databaseName,
        String databaseUsername,
        String databasePassword,
        String redisHost,
        int redisPort
) {
}
