package net.staticstudios.data.impl.redis;

import net.staticstudios.data.util.DataSourceConfig;
import net.staticstudios.data.util.TaskQueue;
import net.staticstudios.data.util.redis.RedisUtils;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

public class RedisListener extends JedisPubSub {
    private static final Logger logger = LoggerFactory.getLogger(RedisListener.class);
    private final Set<String> listenedPartialKeys = ConcurrentHashMap.newKeySet();
    private final Map<Pattern, RedisEventHandler> handlers = new ConcurrentHashMap<>();
    private final Map<String, Integer> ignoredLocalDeleteEvents = new ConcurrentHashMap<>();
    private final CompletableFuture<Void> subscriptionReady = new CompletableFuture<>();
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
                subscriptionReady.completeExceptionally(e);
                logger.error("Redis connection lost in listener thread", e);
            }
        });
        listenerThread.start();

        try {
            subscriptionReady.get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("Timed out waiting for the Redis event subscription", e);
        }

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
        if (event == RedisEvent.DEL && consumeLocalDeleteEvent(key)) {
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

    @Override
    public void onPSubscribe(String pattern, int subscribedChannels) {
        subscriptionReady.complete(null);
    }

    public void expectLocalDeleteEvent(String key) {
        ignoredLocalDeleteEvents.merge(key, 1, Integer::sum);
    }

    public void cancelLocalDeleteEvent(String key) {
        consumeLocalDeleteEvent(key);
    }

    private boolean consumeLocalDeleteEvent(String key) {
        AtomicBoolean consumed = new AtomicBoolean(false);
        ignoredLocalDeleteEvents.computeIfPresent(key, (ignoredKey, count) -> {
            consumed.set(true);
            return count == 1 ? null : count - 1;
        });
        return consumed.get();
    }
}
