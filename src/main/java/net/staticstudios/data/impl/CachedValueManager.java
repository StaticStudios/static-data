package net.staticstudios.data.impl;

import net.staticstudios.data.CachedValue;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.data.Data;
import net.staticstudios.data.data.value.InitialCachedValue;
import net.staticstudios.data.key.RedisKey;
import net.staticstudios.data.primative.Primitives;
import net.staticstudios.data.util.DataSourceConfig;
import net.staticstudios.data.util.DeleteContext;
import net.staticstudios.utils.ShutdownStage;
import net.staticstudios.utils.ThreadUtils;
import org.jetbrains.annotations.Blocking;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class CachedValueManager {
    private static final Logger logger = LoggerFactory.getLogger(CachedValueManager.class);
    private final DataManager dataManager;

    public CachedValueManager(DataManager dataManager, DataSourceConfig ds) {
        this.dataManager = dataManager;

        RedisListener listener = new RedisListener();
        Thread listenerThread = new Thread(() -> {
            try (Jedis jedis = new Jedis(ds.redisHost(), ds.redisPort())) {
                jedis.psubscribe(listener, Arrays.stream(RedisEvent.values()).map(e -> "__keyevent@0__:" + e.name().toLowerCase()).toArray(String[]::new));
            }
        });
        listenerThread.start();

        ThreadUtils.onShutdownRunSync(ShutdownStage.CLEANUP, () -> {
            listener.punsubscribe();
            listenerThread.interrupt();
        });
    }

    public void deleteFromCache(DeleteContext context) {
        for (Data<?> data : context.toDelete()) {
            if (data instanceof CachedValue<?> cv) {
                dataManager.uncache(cv.getKey());
            }
        }
    }

    @Blocking
    public void deleteFromRedis(Jedis jedis, DeleteContext context) {
        for (Data<?> data : context.toDelete()) {
            if (data instanceof CachedValue<?> cv) {
                jedis.del(cv.getKey().toString());
                logger.trace("Deleted {}", cv.getKey());
            }
        }
    }

    @Blocking
    public void setInRedis(Jedis jedis, List<InitialCachedValue> initialData) {
        if (initialData.isEmpty()) {
            return;
        }

        for (InitialCachedValue initial : initialData) {
            RedisKey key = initial.getValue().getKey();
            if (initial.getInitialDataValue() == null) {
                jedis.del(key.toString());
                logger.trace("Deleted {}", key);
                continue;
            }
            Object serialized = dataManager.serialize(initial.getInitialDataValue());

            if (initial.getValue().getExpirySeconds() > 0) {
                jedis.setex(key.toString(), initial.getValue().getExpirySeconds(), Primitives.encode(serialized));
                logger.trace("Set {} to {} with expiry of {} seconds", key, serialized, initial.getValue().getExpirySeconds());
            } else {
                jedis.set(key.toString(), Primitives.encode(serialized));
                logger.trace("Set {} to {}", key, serialized);
            }
        }
    }


    @Blocking
    public void loadAllFromRedis(Jedis jedis, CachedValue<?> dummyValue) {
        Set<String> matchedKeys = jedis.keys(dummyValue.getKey().toPartialKey());
        for (String matchedKey : matchedKeys) {
            if (!RedisKey.isRedisKey(matchedKey)) {
                continue;
            }
            String encoded = jedis.get(matchedKey);

            Object serialized = dataManager.decode(dummyValue.getDataType(), encoded);
            Object deserialized = dataManager.deserialize(dummyValue.getDataType(), serialized);
            dataManager.cache(RedisKey.fromString(matchedKey), dummyValue.getDataType(), deserialized, Instant.now());
        }
    }

    enum RedisEvent {
        SET,
        DEL,
        EXPIRED
    }

    class RedisListener extends JedisPubSub {
        @Override
        public void onPMessage(String pattern, String channel, String key) {
            logger.trace("Received message: {} on channel: {} with pattern: {}", key, channel, pattern);
            String eventString = channel.split(":")[1];
            RedisEvent event = RedisEvent.valueOf(eventString.toUpperCase());
            if (!RedisKey.isRedisKey(key)) {
                return;
            }
            RedisKey redisKey = RedisKey.fromString(key);
            assert redisKey != null;
            Instant now = Instant.now();

            switch (event) {
                case SET -> dataManager.submitAsyncTask((connection, jedis) ->
                        dataManager.getDummyValues(redisKey.getHolderSchema() + "." + redisKey.getHolderTable()).stream()
                                .filter(v -> v.getClass() == CachedValue.class)
                                .map(v -> (CachedValue<?>) v)
                                .filter(v -> v.getKey().getIdentifyingKey().equals(redisKey.getIdentifyingKey()))
                                .findFirst()
                                .ifPresent(dummyValue -> {
                                    String encoded = jedis.get(key);
                                    Object serialized = dataManager.decode(dummyValue.getDataType(), encoded);
                                    Object deserialized = dataManager.deserialize(dummyValue.getDataType(), serialized);
                                    dataManager.cache(redisKey, dummyValue.getDataType(), deserialized, now);
                                })
                );
                case DEL, EXPIRED -> dataManager.uncache(redisKey);
            }
        }
    }
}
