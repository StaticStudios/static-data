package net.staticstudios.data;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.pool.HikariPool;
import net.staticstudios.data.data.Data;
import net.staticstudios.data.data.InitialData;
import net.staticstudios.data.data.InitialPersistentData;
import net.staticstudios.data.data.UniqueData;
import net.staticstudios.data.impl.DataTypeManager;
import net.staticstudios.data.impl.PersistentCollectionValueManager;
import net.staticstudios.data.impl.PersistentDataManager;
import net.staticstudios.data.impl.PostgresListener;
import net.staticstudios.utils.ThreadUtils;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class DataManager {
    private static final Object NULL_MARKER = new Object();
    private final Map<Class<?>, DataTypeManager<?, ?>> dataTypeManagers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<DataKey, Object> cache;
    private final ConcurrentHashMap<UUID, UniqueData> uniqueDataCache;
    private final HikariPool connectionPool;

    public DataManager(HikariConfig poolConfig) {
        this.cache = new ConcurrentHashMap<>();
        this.uniqueDataCache = new ConcurrentHashMap<>();

        PostgresListener pgListener = new PostgresListener();
        this.dataTypeManagers.put(PersistentDataManager.class, new PersistentDataManager(this, pgListener));
        this.dataTypeManagers.put(PersistentCollectionValueManager.class, new PersistentCollectionValueManager(this, pgListener));

        this.connectionPool = new HikariPool(poolConfig);
    }


    @SafeVarargs
    public final <I extends InitialData<?, ?>> void insert(UniqueData holder, I... initialData) {
        List<InitialPersistentData> initialPersistentData = new ArrayList<>();

        for (InitialData<?, ?> data : initialData) {
            if (data instanceof InitialPersistentData) {
                initialPersistentData.add((InitialPersistentData) data);
            } else {
                throw new IllegalArgumentException("Unsupported initial data type: " + data.getClass());
            }
            //todo: redis values
        }

        try {
            PersistentDataManager persistentDataManager = getDataTypeManager(PersistentDataManager.class);
            persistentDataManager.insertIntoDataSource(holder, initialPersistentData);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
//        redisDataManager.insertIntoDataSource(holder, initialRedisData);

        addUniqueData(holder);
    }

    @SafeVarargs
    public final <I extends InitialData<?, ?>> void insertAsync(UniqueData holder, I... initialData) {
        List<InitialPersistentData> initialPersistentData = new ArrayList<>();

        for (InitialData<?, ?> data : initialData) {
            if (data instanceof InitialPersistentData) {
                initialPersistentData.add((InitialPersistentData) data);
            } else {
                throw new IllegalArgumentException("Unsupported initial data type: " + data.getClass());
            }
            //todo: redis values
        }

        ThreadUtils.submit(() -> {
            try {
                PersistentDataManager persistentDataManager = getDataTypeManager(PersistentDataManager.class);
                persistentDataManager.insertIntoDataSource(holder, initialPersistentData);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
//        redisDataManager.insertIntoDataSource(holder, initialRedisData);

        addUniqueData(holder);
    }

    @SuppressWarnings("unchecked")
    public <T extends DataTypeManager<?, ?>> T getDataTypeManager(Class<T> clazz) {
        return (T) dataTypeManagers.get(clazz);
    }

    public <T> T get(Data<T> data) throws DataDoesNotExistException {
        return get(data.getKey());
    }

    @SuppressWarnings("unchecked")
    public <T> T get(DataKey key) throws DataDoesNotExistException {
        Object value = cache.get(key);
        if (value == NULL_MARKER) {
            return null;
        }

        if (value != null) {
            return (T) value;
        }

        throw new DataDoesNotExistException("Data does not exist: " + key);
    }

    public UniqueData getUniqueData(UUID id) throws DataDoesNotExistException {
        if (uniqueDataCache.containsKey(id)) {
            return uniqueDataCache.get(id);
        }

        throw new DataDoesNotExistException("Data does not exist: " + id);
    }

    public void addUniqueData(UniqueData data) {
        //todo: also add to a map of class -> uniquedata[] for getall
        uniqueDataCache.put(data.getId(), data);
    }

    public void removeUniqueData(UniqueData data) {
        uniqueDataCache.remove(data.getId());
    }

    public <T> void cache(DataKey key, T value) {
        if (value == null) {
            cache.put(key, NULL_MARKER);
            return;
        }
        cache.put(key, value);
    }

    public void uncache(DataKey key) {
        cache.remove(key);
    }

    public Connection getConnection() throws SQLException {
        return connectionPool.getConnection();
    }

    public Jedis getRedisConnection() {
        throw new UnsupportedOperationException("Not implemented");
    }
}
