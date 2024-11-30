package net.staticstudios.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public abstract class DataProvider<T extends UniqueData> {
    private final DataManager dataManager;
    private final Class<T> dataType;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final ConcurrentHashMap<UUID, T> dataMap = new ConcurrentHashMap<>();

    public DataProvider(DataManager dataManager, Class<T> dataType) {
        this(dataManager, dataType, true);
    }

    public DataProvider(DataManager dataManager, Class<T> dataType, boolean loadAll) {
        this.dataManager = dataManager;
        this.dataType = dataType;
        dataManager.registerDataProvider(this);

        if (loadAll) {
            loadAllSync(dataManager);
        }
    }

    public Class<T> getDataType() {
        return dataType;
    }

    public Collection<T> getAll() {
        return dataMap.values();
    }

    public T get(UUID id) {
        return dataMap.get(id);
    }

    @SuppressWarnings("unchecked")
    public void set(UniqueData data) {
        if (!getDataType().isInstance(data)) {
            throw new IllegalArgumentException("Data must be of type " + getDataType().getSimpleName());
        }

        dataManager.insertIntoDataWrapperLookupTable(data);
        dataMap.put(data.getId(), (T) data);
    }

    public void remove(UUID id) {
        UniqueData data = dataMap.get(id);
        if (data != null) {
            dataManager.removeFromDataWrapperLookupTable(data);
        }

        dataMap.remove(id);
    }

    public void delete(UUID id, DeletionType deletionType) {
        UniqueData data = dataMap.get(id);
        if (data == null) {
            return;
        }

        dataManager.removeFromDataWrapperLookupTable(data);
        dataMap.remove(id);
        dataManager.delete(data, deletionType);
    }

    public void delete(Connection connection, Jedis jedis, UUID id, DeletionType deletionType) throws SQLException {
        UniqueData data = dataMap.get(id);
        if (data != null) {
            dataManager.removeFromDataWrapperLookupTable(data);
        }

        dataMap.remove(id);
        dataManager.delete(connection, jedis, data, deletionType);
    }

    public Logger getLogger() {
        return logger;
    }

    public void loadAllSync(DataManager dataManager) {
        long start = System.currentTimeMillis();
        getLogger().info("Loading {}s...", getDataType().getSimpleName());
        try {
            Collection<T> results = dataManager.selectAll(getDataType());
            clear();
            results.forEach(this::set);
            getLogger().info("Loaded {} {}s in {}ms", getAll().size(), getDataType().getSimpleName(), System.currentTimeMillis() - start);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void clear() {
        for (T data : dataMap.values()) {
            dataManager.removeFromDataWrapperLookupTable(data);
        }

        dataMap.clear();
    }

    public DataManager getDataManager() {
        return dataManager;
    }
}
