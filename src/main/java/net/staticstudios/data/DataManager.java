package net.staticstudios.data;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.pool.HikariPool;
import net.staticstudios.data.data.Data;
import net.staticstudios.data.data.InitialData;
import net.staticstudios.data.data.InitialPersistentData;
import net.staticstudios.data.data.UniqueData;
import net.staticstudios.data.impl.PersistentCollectionManager;
import net.staticstudios.data.impl.PersistentValueManager;
import net.staticstudios.data.impl.PostgresListener;
import net.staticstudios.data.key.CellKey;
import net.staticstudios.data.key.DataKey;
import net.staticstudios.data.key.DatabaseKey;
import net.staticstudios.data.util.ReflectionUtils;
import net.staticstudios.utils.ThreadUtils;
import org.jetbrains.annotations.Blocking;
import org.jetbrains.annotations.NotNull;
import redis.clients.jedis.Jedis;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DataManager {
    private static final Object NULL_MARKER = new Object();
    private final Map<DataKey, Object> cache;
    private final Multimap<Class<? extends UniqueData>, UUID> uniqueDataIds;
    private final Map<Class<? extends UniqueData>, Map<UUID, UniqueData>> uniqueDataCache;
    private final HikariPool connectionPool;
    private final Set<Class<? extends UniqueData>> loadedIntoCache = new HashSet<>();

    public DataManager(HikariConfig poolConfig) {
        this.cache = new ConcurrentHashMap<>();
        this.uniqueDataCache = new ConcurrentHashMap<>();
        this.uniqueDataIds = Multimaps.synchronizedSetMultimap(HashMultimap.create());

        PostgresListener pgListener = new PostgresListener(poolConfig);

        PersistentCollectionManager.instantiate(this, pgListener);
        PersistentValueManager.instantiate(this, pgListener);

        this.connectionPool = new HikariPool(poolConfig);
    }

    public void logSQL(String sql) {
        System.out.println(sql);
    }

    @Blocking
    public <T extends UniqueData> List<T> loadAll(Class<T> clazz) {
        try {
            Set<Class<? extends UniqueData>> dependencies = new HashSet<>();
            extractDependencies(clazz, dependencies);
            dependencies.removeIf(loadedIntoCache::contains);

            Multimap<UniqueData, DatabaseKey> databaseKeys = Multimaps.newSetMultimap(new HashMap<>(), HashSet::new);

            for (Class<? extends UniqueData> dependency : dependencies) {
                List<Data<?>> dataDependencies = extractDataDependencies(dependency);


                for (Data<?> data : dataDependencies) {
                    DataKey key = data.getKey();
                    if (key instanceof DatabaseKey dbKey) {
                        databaseKeys.put(data.getHolder().getRootHolder(), dbKey);
                    }
                }
            }

            try (Connection connection = getConnection()) {
                for (UniqueData dummyHolder : databaseKeys.keySet()) {
                    loadUniqueData(connection, dummyHolder);
                    loadPersistentDataIntoCache(connection, databaseKeys);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            loadedIntoCache.addAll(dependencies);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        //to load data, we must first find a list of dependencies, and recursively grab their dependencies. after we've found all of them, we should load all data, and then we can establish relationships

//        try (Connection connection = getConnection()) {
//
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }

        return getAll(clazz);
    }

    public <T extends UniqueData> List<T> getAll(Class<T> clazz) {
        return uniqueDataIds.get(clazz).stream().map(uniqueDataCache::get).map(clazz::cast).toList();
    }

    private void extractDependencies(Class<? extends UniqueData> clazz, @NotNull Set<Class<? extends UniqueData>> dependencies) throws Exception {
        dependencies.add(clazz);
        Constructor<?> constructor = clazz.getDeclaredConstructor(DataManager.class, UUID.class);
        constructor.setAccessible(true);

        if (!UniqueData.class.isAssignableFrom(clazz)) {
            return;
        }

        UniqueData dummy = (UniqueData) constructor.newInstance(this, UUID.randomUUID());

        for (Field field : ReflectionUtils.getFields(clazz)) {
            field.setAccessible(true);

            Class<?> type = field.getType();

            if (Data.class.isAssignableFrom(type)) {
                Data<?> data = (Data<?>) field.get(dummy);
                Class<?> dataType = data.getDataType();

                if (UniqueData.class.isAssignableFrom(dataType)) {
                    extractDependencies((Class<? extends UniqueData>) dataType, dependencies);
                }
            }
        }
    }

    private List<Data<?>> extractDataDependencies(Class<? extends UniqueData> clazz) throws Exception {
        Constructor<?> constructor = clazz.getDeclaredConstructor(DataManager.class, UUID.class);
        constructor.setAccessible(true);

        UniqueData dummy = (UniqueData) constructor.newInstance(this, UUID.randomUUID());

        List<Data<?>> dependencies = new ArrayList<>();

        for (Field field : dummy.getClass().getDeclaredFields()) { //todo: we need to use some utility to support superclasses
            field.setAccessible(true);

            Class<?> type = field.getType();

            if (Data.class.isAssignableFrom(type)) {
                try {
                    Data<?> data = (Data<?>) field.get(dummy);
                    dependencies.add(data);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        return dependencies;
    }

    //todo: this can be named better
    private void loadPersistentDataIntoCache(Connection connection, Multimap<UniqueData, DatabaseKey> databaseKeys) {
        System.out.println("Loading persistent data into cache");
//        System.out.println("Keys: " + databaseKeys.entries());

//        Multimap<String, String> valueColumnsToLoad = Multimaps.newListMultimap(new HashMap<>(), ArrayList::new);
//        Multimap<String, Pair<String, CollectionKey>> collectionColumnsToLoad = Multimaps.newListMultimap(new HashMap<>(), ArrayList::new);

//        for (DatabaseKey key : databaseKeys) {
//            if (key instanceof ColumnKey columnKey) {
//                for (String column : columnKey.getColumns()) {
//                    valueColumnsToLoad.put(key.getSchema() + "." + key.getTable(), column);
//                }
//            } else if (key instanceof CollectionKey collectionKey) {
//                for (String column : key.getColumns()) {
//                    collectionColumnsToLoad.put(key.getSchema() + "." + key.getTable(), Pair.of(column, collectionKey));
//                }
//            }
//        }

        for (Map.Entry<UniqueData, Collection<DatabaseKey>> entry : databaseKeys.asMap().entrySet()) {
            UniqueData dummyHolder = entry.getKey();
            Multimap<DataKey, CellKey> persistentDataColumns = Multimaps.newListMultimap(new HashMap<>(), ArrayList::new);

            for (DatabaseKey key : entry.getValue()) {
                if (key instanceof CellKey cellKey) {
                    persistentDataColumns.put(new DataKey(cellKey.getSchema(), cellKey.getTable(), cellKey.getColumn(), cellKey.getIdColumn()), cellKey);
                }
            }
            try {
                PersistentValueManager persistentValueManager = PersistentValueManager.getInstance();
                for (DataKey key : persistentDataColumns.keySet()) {
                    persistentValueManager.loadAllFromDataSource(connection, dummyHolder, persistentDataColumns.get(key));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }


        //todo: pull this out and load based on a join on the holder's primary key column
//        for (Map.Entry<String, Collection<Pair<String, CollectionKey>>> entry : collectionColumnsToLoad.asMap().entrySet()) {
//            String schemaTable = entry.getKey();
//            Collection<Pair<String, CollectionKey>> pairs = entry.getValue();
//            List<String> columns = new ArrayList<>();
//            columns.add("id");
//            for (Pair<String, CollectionKey> pair : pairs) {
//                columns.add(pair.first());
//            }
//
//            columns = new ArrayList<>(new HashSet<>(columns));
//
//
//            StringBuilder sqlBuilder = new StringBuilder("SELECT ");
//
//            for (int i = 0; i < columns.size(); i++) {
//                sqlBuilder.append(columns.get(i));
//                if (i < columns.size() - 1) {
//                    sqlBuilder.append(", ");
//                }
//            }
//            sqlBuilder.append(" FROM ").append(schemaTable);
//
//            String sql = sqlBuilder.toString();
//            System.out.println("SQL: " + sql);
//
//            try (PreparedStatement statement = connection.prepareStatement(sql)) {
//                ResultSet resultSet = statement.executeQuery();
//                while (resultSet.next()) {
//                    UUID id = resultSet.getObject("id", UUID.class);
//                    UUID linkingId = resultSet.getObject(pairs.iterator().next().second().getLinkingColumn(), UUID.class);
//
//                    if (linkingId == null) {
//                        continue;
//                    }
//
//                    for (Pair<String, CollectionKey> pair : pairs) {
//                        String column = pair.first();
//                        CollectionKey collectionKey = pair.second();
//                        Object value = resultSet.getObject(column); //todo: deserialize
//                        cache(new CollectionEntryKey(collectionKey.getSchema(), collectionKey.getTable(), collectionKey.getLinkingColumn(), collectionKey.getDataColumn(), linkingId, id), value);
//                    }
//                }
//            }
//        }

    }

    public void dump() {
        //print each cache entry line by line
        for (Map.Entry<DataKey, Object> entry : cache.entrySet()) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }
    }

    private void loadUniqueData(Connection connection, UniqueData dummyHolder) throws SQLException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        String sql = "SELECT " + dummyHolder.getIdentifier().getColumn() + " FROM " + dummyHolder.getSchema() + "." + dummyHolder.getTable();

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                UUID id = resultSet.getObject(dummyHolder.getIdentifier().getColumn(), UUID.class);
                Class<? extends UniqueData> clazz = dummyHolder.getClass();
                Constructor<? extends UniqueData> constructor = clazz.getDeclaredConstructor(DataManager.class, UUID.class);
                constructor.setAccessible(true);
                UniqueData instance = constructor.newInstance(this, id);
                addUniqueData(instance);
            }
        }
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
            PersistentValueManager persistentValueManager = PersistentValueManager.getInstance();
            persistentValueManager.setInDataSource(initialPersistentData);
            for (InitialPersistentData data : initialPersistentData) {
                persistentValueManager.updateCache(data.getValue(), data.getDataValue());
            }
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
        PersistentValueManager persistentValueManager = PersistentValueManager.getInstance();
        for (InitialPersistentData data : initialPersistentData) {
            persistentValueManager.updateCache(data.getValue(), data.getDataValue());
        }

        ThreadUtils.submit(() -> {
            try {
                persistentValueManager.setInDataSource(initialPersistentData);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

//        redisDataManager.insertIntoDataSource(holder, initialRedisData);

        addUniqueData(holder);
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

        throw new DataDoesNotExistException("Data does not exist ");
    }

    public <T extends UniqueData> T getUniqueData(Class<T> clazz, UUID id) throws DataDoesNotExistException {
        Map<UUID, UniqueData> uniqueData = uniqueDataCache.get(clazz);
        if (uniqueData != null) {
            UniqueData data = uniqueData.get(id);
            if (data != null) {
                return clazz.cast(data);
            }
        }

        throw new DataDoesNotExistException("Data does not exist: " + id);
    }

    public void addUniqueData(UniqueData data) {
        uniqueDataIds.put(data.getClass(), data.getId());
        uniqueDataCache.computeIfAbsent(data.getClass(), k -> new ConcurrentHashMap<>()).put(data.getId(), data);
    }

    public void removeUniqueData(UniqueData data) {
        uniqueDataIds.remove(data.getClass(), data.getId());
        uniqueDataCache.get(data.getClass()).remove(data.getId());
    }

    public <T> void cache(DataKey key, T value) {
//        System.out.println("caching " + key + " -> " + value);
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
