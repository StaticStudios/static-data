package net.staticstudios.data.impl.redis;

import net.staticstudios.data.util.DataSourceConfig;
import net.staticstudios.data.util.RedisUtils;
import net.staticstudios.data.util.TaskQueue;
import net.staticstudios.utils.ShutdownStage;
import net.staticstudios.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

public class RedisListener extends JedisPubSub {
    private static final Logger logger = LoggerFactory.getLogger(RedisListener.class);
    private final Set<String> listenedPartialKeys = ConcurrentHashMap.newKeySet();
    private final Map<Pattern, RedisEventHandler> handlers = new ConcurrentHashMap<>();
    private final TaskQueue taskQueue;

    public RedisListener(DataSourceConfig ds, TaskQueue taskQueue) {
        this.taskQueue = taskQueue;
        Thread listenerThread = new Thread(() -> {
            try (Jedis jedis = new Jedis(ds.redisHost(), ds.redisPort())) {
                jedis.psubscribe(this, Arrays.stream(RedisEvent.values()).map(e -> "__keyevent@0__:" + e.name().toLowerCase()).toArray(String[]::new));
            } catch (JedisConnectionException e) {
                if (ThreadUtils.isShuttingDown()) {
                    return;
                }
                logger.error("Redis connection lost in listener thread", e);
            }
        });
        listenerThread.start();

        ThreadUtils.onShutdownRunSync(ShutdownStage.CLEANUP, () -> {
            this.punsubscribe();
            listenerThread.interrupt();
        });
    }


    public void listen(String partialKey, RedisEventHandler handler) {
        if (listenedPartialKeys.add(partialKey)) {
            handlers.put(RedisUtils.globToRegex(partialKey), handler);
        }
    }

    @Override
    public void onPMessage(String pattern, String channel, String key) {
        logger.trace("Received message: {} on channel: {} with pattern: {}", key, channel, pattern);
        String eventString = channel.split(":")[1];
        RedisEvent event = RedisEvent.valueOf(eventString.toUpperCase());
        if (!key.startsWith("static-data:")) {
            return;
        }

        for (Map.Entry<Pattern, RedisEventHandler> entry : handlers.entrySet()) {
            if (entry.getKey().matcher(key).matches()) {
                switch (event) {
                    case SET -> taskQueue.submitTask((connection, jedis) -> {
                        String encoded = jedis.get(key);
                        if (encoded == null) {
                            return;
                        }
                        entry.getValue().handle(event, key, encoded);
                    });
                    case DEL, EXPIRED -> entry.getValue().handle(event, key, null);
                }
                return;
            }
        }
    }
}
