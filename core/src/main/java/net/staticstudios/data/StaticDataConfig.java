package net.staticstudios.data;

import com.google.common.base.Preconditions;
import net.staticstudios.utils.ThreadUtils;

import java.util.function.Consumer;

public record StaticDataConfig(String postgresHost,
                               int postgresPort,
                               String postgresDatabase,
                               String postgresUsername,
                               String postgresPassword,
                               String redisHost,
                               int redisPort,
                               Consumer<Runnable> updateHandlerExecutor
) {

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String postgresHost;
        private int postgresPort = 5432;
        private String postgresDatabase;
        private String postgresUsername;
        private String postgresPassword;
        private String redisHost;
        private int redisPort = 6379;
        private Consumer<Runnable> updateHandlerExecutor = ThreadUtils::submit;


        public Builder postgresHost(String postgresHost) {
            this.postgresHost = postgresHost;
            return this;
        }

        public Builder postgresPort(int postgresPort) {
            this.postgresPort = postgresPort;
            return this;
        }

        public Builder postgresDatabase(String postgresDatabase) {
            this.postgresDatabase = postgresDatabase;
            return this;
        }

        public Builder postgresUsername(String postgresUsername) {
            this.postgresUsername = postgresUsername;
            return this;
        }

        public Builder postgresPassword(String postgresPassword) {
            this.postgresPassword = postgresPassword;
            return this;
        }

        public Builder redisHost(String redisHost) {
            this.redisHost = redisHost;
            return this;
        }

        public Builder redisPort(int redisPort) {
            this.redisPort = redisPort;
            return this;
        }

        public Builder updateHandlerExecutor(Consumer<Runnable> updateHandlerExecutor) {
            this.updateHandlerExecutor = updateHandlerExecutor;
            return this;
        }

        public StaticDataConfig build() {
            Preconditions.checkNotNull(postgresHost, "Postgres host must be set");
            Preconditions.checkNotNull(postgresDatabase, "Postgres database must be set");
            Preconditions.checkNotNull(postgresUsername, "Postgres username must be set");
            Preconditions.checkNotNull(postgresPassword, "Postgres password must be set");
            Preconditions.checkNotNull(redisHost, "Redis host must be set");
            Preconditions.checkNotNull(updateHandlerExecutor, "Update handler executor must be set");

            return new StaticDataConfig(
                    postgresHost,
                    postgresPort,
                    postgresDatabase,
                    postgresUsername,
                    postgresPassword,
                    redisHost,
                    redisPort,
                    updateHandlerExecutor
            );
        }
    }
}
