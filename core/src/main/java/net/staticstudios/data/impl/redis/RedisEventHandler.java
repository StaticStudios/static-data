package net.staticstudios.data.impl.redis;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface RedisEventHandler {

    void handle(RedisEvent event, @NotNull String key, @Nullable String value);
}
