package net.staticstudios.data.v2;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.pool.HikariPool;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class DataManager {
    private final PersistentDataManager persistentDataManager;
    private final Cache<DataKey, Object> cache;
    private final HikariPool connectionPool;

    public DataManager(HikariConfig poolConfig) {
        this.cache = Caffeine.newBuilder()
//                .expireAfterAccess(10, TimeUnit.MINUTES)
//                .maximumSize(10_000)
                .build();
        //todo: think about lists and maps

        dataObjects = new ConcurrentHashMap<>();
        this.persistentDataManager = new PersistentDataManager(this);

        this.connectionPool = new HikariPool(poolConfig);
    }

    @SuppressWarnings("unchecked")
    public <T extends UniqueData> T get(String schemaTable, UUID id) {
        var map = getAll(schemaTable);
        if (map == null) {
            return null;
        }

        return (T) map.get(id);
    }

    public Map<UUID, UniqueData> getAll(String schemaTable) {
        return dataObjects.get(schemaTable);
    }

    public <I extends InitialData<?>> void insert(UniqueData holder, I... initialData) {
        List<InitialPersistentData> initialPersistentData = new ArrayList<>();

        for (InitialData<?> data : initialData) {
            if (data instanceof InitialPersistentData) {
                initialPersistentData.add((InitialPersistentData) data);
            } else {
                throw new IllegalArgumentException("Unsupported initial data type: " + data.getClass());
            }
        }

        try {
            persistentDataManager.insertIntoDataSource(holder, initialPersistentData);
            String schemaTable = holder.getSchema() + "." + holder.getTable();
            dataObjects.computeIfAbsent(schemaTable, k -> new ConcurrentHashMap<>()).put(holder.getId(), holder);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
//        redisDataManager.insertIntoDataSource(holder, initialRedisData);
    }

    public DataTypeManager<?, ?> getDataTypeManager(Class<? extends DataTypeManager<?, ?>> clazz) {
        if (clazz == PersistentDataManager.class) {
            return persistentDataManager;
        }

        throw new IllegalArgumentException("Unsupported data type manager class: " + clazz);
    }

    @SuppressWarnings("unchecked")
    public <T> T getOrLookupDataValue(Data<T> data) {
        DataKey key = data.getKey();

        return (T) cache.get(key, k -> getValueFromDataSource(data));
    }

    @SuppressWarnings("unchecked")
    public <T> void cache(DataKey key, T value) {
        cache.put(key, value);
    }

    public Connection getConnection() throws SQLException {
        return connectionPool.getConnection();
    }

    public Jedis getRedisConnection() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @SuppressWarnings("unchecked")
    private <T> T getValueFromDataSource(Data<T> data) {
        try {
            return (T) getDataTypeManager(data.getDataTypeManagerClass()).unsafeLoadFromDataSource(data);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
