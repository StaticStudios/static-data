package net.staticstudios.data;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.pool.HikariPool;
import net.staticstudios.data.data.*;
import net.staticstudios.data.data.collection.PersistentCollection;
import net.staticstudios.data.impl.PersistentCollectionManager;
import net.staticstudios.data.impl.PersistentValueManager;
import net.staticstudios.data.impl.pg.PostgresListener;
import net.staticstudios.data.impl.pg.PostgresOperation;
import net.staticstudios.data.key.CellKey;
import net.staticstudios.data.key.DataKey;
import net.staticstudios.data.key.DatabaseKey;
import net.staticstudios.data.primative.Primitives;
import net.staticstudios.data.util.ReflectionUtils;
import net.staticstudios.utils.ShutdownStage;
import net.staticstudios.utils.ThreadUtils;
import org.jetbrains.annotations.Blocking;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class DataManager {
    private static final Object NULL_MARKER = new Object();
    private final Logger logger = LoggerFactory.getLogger(DataManager.class);
    private final Map<DataKey, CacheEntry> cache;
    private final Multimap<Class<? extends UniqueData>, UUID> uniqueDataIds;
    private final Map<Class<? extends UniqueData>, Map<UUID, UniqueData>> uniqueDataCache;
    private final Multimap<String, Value<?>> dummyValueMap;
    private final Multimap<String, PersistentCollection<?>> dummyPersistentCollectionMap;
    private final Multimap<String, UniqueData> dummyUniqueDataMap;
    private final HikariPool connectionPool;
    private final Set<Class<? extends UniqueData>> loadedDependencies = new HashSet<>();
    private final List<ValueSerializer<?, ?>> valueSerializers;
    private final PostgresListener pgListener;

    public DataManager(HikariConfig poolConfig) {
        this.cache = new ConcurrentHashMap<>();
        this.uniqueDataCache = new ConcurrentHashMap<>();
        this.dummyValueMap = Multimaps.synchronizedSetMultimap(HashMultimap.create());
        this.dummyPersistentCollectionMap = Multimaps.synchronizedSetMultimap(HashMultimap.create());
        this.dummyUniqueDataMap = Multimaps.synchronizedSetMultimap(HashMultimap.create());
        this.uniqueDataIds = Multimaps.synchronizedSetMultimap(HashMultimap.create());
        this.valueSerializers = new CopyOnWriteArrayList<>();

        pgListener = new PostgresListener(poolConfig);

        PersistentCollectionManager.instantiate(this, pgListener);
        PersistentValueManager.instantiate(this, pgListener);

        this.connectionPool = new HikariPool(poolConfig);

        pgListener.addHandler(notification -> {
            if (notification.getOperation() == PostgresOperation.INSERT) {
                logger.trace("Handing INSERT operation");
                Collection<? extends UniqueData> dummyUniqueData = dummyUniqueDataMap.get(notification.getSchema() + "." + notification.getTable());

                for (UniqueData dummy : dummyUniqueData) {
                    String identifierColumn = dummy.getIdentifier().getColumn();
                    UUID id = UUID.fromString(notification.getData().newDataValueMap().get(identifierColumn));

                    UniqueData instance = createInstance(dummy.getClass(), id);
                    addUniqueData(instance);
                }
            }

            if (notification.getOperation() == PostgresOperation.DELETE) {
                logger.trace("Handling DELETE operation");
                Collection<? extends UniqueData> dummyUniqueData = dummyUniqueDataMap.get(notification.getSchema() + "." + notification.getTable());

                for (UniqueData dummy : dummyUniqueData) {
                    String identifierColumn = dummy.getIdentifier().getColumn();
                    UUID id = UUID.fromString(notification.getData().oldDataValueMap().get(identifierColumn));
                    removeUniqueData(dummy.getClass(), id);
                }
            }
        });

        ThreadUtils.onShutdownRunSync(ShutdownStage.CLEANUP, () -> {
            try {
                connectionPool.shutdown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void logSQL(String sql) {
        logger.debug("Executing SQL: {}", sql);
    }

    public Collection<Value<?>> getDummyValues(String schemaTable) {
        return dummyValueMap.get(schemaTable);
    }

    public Collection<UniqueData> getDummyUniqueData(String schemaTable) {
        return dummyUniqueDataMap.get(schemaTable);
    }

    public Collection<PersistentCollection<?>> getDummyPersistentCollections(String schemaTable) {
        return dummyPersistentCollectionMap.get(schemaTable);
    }

    @Blocking
    public <T extends UniqueData> List<T> loadAll(Class<T> clazz) {
        try {
            // A dependency is just another UniqueData that is referenced in some way.
            // This clazz is also treated as a dependency, these will all be loaded at the same time
            Set<Class<? extends UniqueData>> dependencies = new HashSet<>();
            extractDependencies(clazz, dependencies);
            dependencies.removeIf(loadedDependencies::contains);

            Multimap<UniqueData, DatabaseKey> dummyDatabaseKeys = Multimaps.newSetMultimap(new HashMap<>(), HashSet::new);
            Multimap<UniqueData, PersistentCollection<?>> dummyPersistentCollections = Multimaps.newSetMultimap(new HashMap<>(), HashSet::new);

            for (Class<? extends UniqueData> dependency : dependencies) {
                // A data dependency is some data field on one of our dependencies
                List<Data<?>> dataDependencies = extractDataDependencies(dependency);

                for (Data<?> data : dataDependencies) {
                    DataKey key = data.getKey();
                    if (key instanceof DatabaseKey dbKey) {
                        dummyDatabaseKeys.put(data.getHolder().getRootHolder(), dbKey);
                    }

                    // This is our first time seeing this value, so we add it to the map for later use
                    if (data instanceof PersistentValue<?> value) {
                        dummyValueMap.put(value.getSchema() + "." + value.getTable(), value);
                    }

                    if (data instanceof PersistentCollection<?> collection) {
                        dummyPersistentCollectionMap.put(collection.getSchema() + "." + collection.getTable(), collection);
                        dummyPersistentCollections.put(data.getHolder().getRootHolder(), collection);
                    }
                }
            }

            try (Connection connection = getConnection()) {
                // Load all the unique data first
                for (UniqueData dummyHolder : dummyDatabaseKeys.keySet()) {
                    loadUniqueData(connection, dummyHolder);
                }

                //Load PersistentValues
                for (UniqueData dummyHolder : dummyDatabaseKeys.keySet()) {
                    Collection<DatabaseKey> keys = dummyDatabaseKeys.get(dummyHolder);

                    // Use a multimap so we can easily group CellKeys together
                    // The actual key (DataKey) for this map is solely used for grouping entries with the same schema, table, data column, and id column
                    Multimap<DataKey, CellKey> persistentDataColumns = Multimaps.newListMultimap(new HashMap<>(), ArrayList::new);

                    for (DatabaseKey key : keys) {
                        if (key instanceof CellKey cellKey) {
                            persistentDataColumns.put(new DataKey(cellKey.getSchema(), cellKey.getTable(), cellKey.getColumn(), cellKey.getIdColumn()), cellKey);
                        }
                    }

                    PersistentValueManager manager = PersistentValueManager.getInstance();
                    for (DataKey key : persistentDataColumns.keySet()) {
                        manager.loadAllFromDatabase(connection, dummyHolder, persistentDataColumns.get(key));
                    }
                }

                //Load PersistentCollections
                for (UniqueData dummyHolder : dummyPersistentCollections.keySet()) {
                    Collection<PersistentCollection<?>> dummyCollections = dummyPersistentCollections.get(dummyHolder);

                    PersistentCollectionManager manager = PersistentCollectionManager.getInstance();
                    for (PersistentCollection<?> dummyCollection : dummyCollections) {
                        manager.loadAllFromDatabase(connection, dummyHolder, dummyCollection);
                    }
                }

            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            loadedDependencies.addAll(dependencies);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        //to load data, we must first find a list of dependencies, and recursively grab their dependencies. after we've found all of them, we should load all data, and then we can establish relationships

//        try (Connection connection = getConnection()) {
//
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }

        // All the entries we either just load or were previously loaded due to them being a dependency are now in the cache, so just return them via getAll()
        return getAll(clazz);
    }

    @Blocking
    @SafeVarargs
    public final <I extends InitialValue<?, ?>> void insert(UniqueData holder, I... initialData) {
        //todo: all other Values that are not specified here should be set to NULL_MARKER in the cache. the database may then set a default value
        Map<PersistentValue<?>, InitialPersistentValue> initialValues = new HashMap<>();

        for (Field field : ReflectionUtils.getFields(holder.getClass())) {
            field.setAccessible(true);

            if (Data.class.isAssignableFrom(field.getType())) {
                try {
                    Data<?> data = (Data<?>) field.get(holder);
                    if (data instanceof PersistentValue<?> pv) {
                        initialValues.put(pv, new InitialPersistentValue(pv, pv.getDefaultValue()));
                    }
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        for (InitialValue<?, ?> data : initialData) {
            if (data instanceof InitialPersistentValue initial) {
                initialValues.put(initial.getValue(), initial);
            } else {
                throw new IllegalArgumentException("Unsupported initial data type: " + data.getClass());
            }
            //todo: redis values
        }

        //todo: tell the collection manager we inserted data to see if it wants to track it

        try {
            PersistentValueManager persistentValueManager = PersistentValueManager.getInstance();
            persistentValueManager.setInDatabase(new ArrayList<>(initialValues.values()));
            for (InitialPersistentValue data : initialValues.values()) {
                persistentValueManager.updateCache(data.getValue(), data.getInitialDataValue());

                UniqueData pvHolder = data.getValue().getHolder().getRootHolder();
                //update the id column too
                persistentValueManager.updateCache(
                        pvHolder.getSchema(),
                        pvHolder.getTable(),
                        pvHolder.getIdentifier().getColumn(),
                        pvHolder.getId(),
                        pvHolder.getIdentifier().getColumn(),
                        pvHolder.getId()
                );
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        addUniqueData(holder);
    }

    @SafeVarargs
    public final <I extends InitialValue<?, ?>> void insertAsync(UniqueData holder, I... initialData) {
        List<InitialPersistentValue> initialPersistentData = new ArrayList<>();

        for (InitialValue<?, ?> data : initialData) {
            if (data instanceof InitialPersistentValue) {
                initialPersistentData.add((InitialPersistentValue) data);
            } else {
                throw new IllegalArgumentException("Unsupported initial data type: " + data.getClass());
            }
            //todo: redis values
        }
        PersistentValueManager persistentValueManager = PersistentValueManager.getInstance();
        for (InitialPersistentValue data : initialPersistentData) {
            persistentValueManager.updateCache(data.getValue(), data.getInitialDataValue());

            UniqueData pvHolder = data.getValue().getHolder().getRootHolder();
            //update the id column too
            persistentValueManager.updateCache(
                    pvHolder.getSchema(),
                    pvHolder.getTable(),
                    pvHolder.getIdentifier().getColumn(),
                    pvHolder.getId(),
                    pvHolder.getIdentifier().getColumn(),
                    pvHolder.getId()
            );
        }

        ThreadUtils.submit(() -> {
            try {
                persistentValueManager.setInDatabase(initialPersistentData);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

//        redisDataManager.insertIntoDataSource(holder, initialRedisData);

        addUniqueData(holder);
    }

    public <T extends UniqueData> void delete(T holder) {
        //todo: we need to delete collections as well
        PersistentValueManager persistentValueManager = PersistentValueManager.getInstance();

        for (Field field : ReflectionUtils.getFields(holder.getClass())) {
            field.setAccessible(true);

            if (Data.class.isAssignableFrom(field.getType())) {
                try {
                    Data<?> data = (Data<?>) field.get(holder);
                    if (data instanceof PersistentValue<?> value) {
                        persistentValueManager.uncache(value);
                        persistentValueManager.uncache(
                                value.getSchema(),
                                value.getTable(),
                                value.getIdColumn(),
                                holder.getId(),
                                value.getIdColumn()
                        );
                    }
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        removeUniqueData(holder.getClass(), holder.getId());

        ThreadUtils.submit(() -> {
            try (Connection connection = getConnection()) {
                String sql = "DELETE FROM " + holder.getSchema() + "." + holder.getTable() + " WHERE " + holder.getIdentifier().getColumn() + " = ?";
                logSQL(sql);

                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setObject(1, holder.getId());
                    statement.execute();
                }

            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public <T extends UniqueData> List<T> getAll(Class<T> clazz) {
        return uniqueDataIds.get(clazz).stream().map(id -> getUniqueData(clazz, id)).toList();
    }

    private void extractDependencies(Class<? extends UniqueData> clazz, @NotNull Set<Class<? extends UniqueData>> dependencies) throws Exception {
        dependencies.add(clazz);

        if (!UniqueData.class.isAssignableFrom(clazz)) {
            return;
        }

        UniqueData dummy = createInstance(clazz, null);

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
        UniqueData dummy = createInstance(clazz, null);

        List<Data<?>> dependencies = new ArrayList<>();

        for (Field field : ReflectionUtils.getFields(clazz)) {
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

        dummyUniqueDataMap.put(dummy.getSchema() + "." + dummy.getTable(), dummy);

        return dependencies;
    }

    //todo: this can be named better
    private void loadPersistentDataIntoCache(Connection connection, Multimap<UniqueData, DatabaseKey> databaseKeys) {
//        System.out.println("Loading persistent data into cache");
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
        for (Map.Entry<DataKey, CacheEntry> entry : cache.entrySet()) {
            logger.debug("{} -> {}", entry.getKey(), entry.getValue().value());
        }
    }

    private void loadUniqueData(Connection connection, UniqueData dummyHolder) throws SQLException {
        String sql = "SELECT " + dummyHolder.getIdentifier().getColumn() + " FROM " + dummyHolder.getSchema() + "." + dummyHolder.getTable();

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                UUID id = resultSet.getObject(dummyHolder.getIdentifier().getColumn(), UUID.class);
                UniqueData instance = createInstance(dummyHolder.getClass(), id);
                addUniqueData(instance);
            }
        }
    }

    public <T> T get(Data<T> data) throws DataDoesNotExistException {
        return get(data.getKey());
    }

    @SuppressWarnings("unchecked")
    public <T> T get(DataKey key) throws DataDoesNotExistException {
        CacheEntry cacheEntry = cache.get(key);

        if (cacheEntry == null) {
            throw new DataDoesNotExistException("Data does not exist in cache: " + key);
        }

        Object value = cacheEntry.value();
        if (value == NULL_MARKER) {
            return null;
        }

        return (T) value;
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
        logger.trace("Adding unique data to cache: {}({})", data.getClass(), data.getId());
        uniqueDataIds.put(data.getClass(), data.getId());
        uniqueDataCache.computeIfAbsent(data.getClass(), k -> new ConcurrentHashMap<>()).put(data.getId(), data);
    }

    public void removeUniqueData(Class<? extends UniqueData> clazz, UUID id) {
        try {
            logger.trace("Removing unique data from cache: {}({})", clazz, id);
            uniqueDataIds.remove(clazz, id);
            uniqueDataCache.get(clazz).remove(id);
        } catch (DataDoesNotExistException e) {
            logger.trace("Data does not exist in cache: {}({})", clazz, id);
        }
    }

    /**
     * Add an object to the cache. Null values are permitted.
     * If an entry with the given key already exists in the cache,
     * it will only be replaced if this value's instant is newer than the existing entry's instant.
     *
     * @param key     The key to cache the value under
     * @param value   The value to cache
     * @param instant The instant at which the value was set
     */
    public <T> void cache(DataKey key, T value, Instant instant) {
        logger.trace("Caching value: {} -> {}", key, value);
        CacheEntry existing = cache.get(key);

        if (existing != null && existing.instant().isAfter(instant)) {
            logger.debug("Not caching value: {} -> {}, existing entry is newer. {} vs {} (existing)", key, value, instant, existing.instant());
            return;
        }

        if (value == null) {
            cache.put(key, CacheEntry.of(NULL_MARKER, instant));
            return;
        }
        cache.put(key, CacheEntry.of(value, instant));
    }

    public void uncache(DataKey key) {
        cache.remove(key);
    }

    public Connection getConnection() throws SQLException {
        return connectionPool.getConnection();
    }

    public void registerSerializer(ValueSerializer<?, ?> serializer) {
        if (Primitives.isPrimitive(serializer.getDeserializedType())) {
            throw new IllegalArgumentException("Cannot register a serializer for a primitive type");
        }

        if (!Primitives.isPrimitive(serializer.getSerializedType())) {
            throw new IllegalArgumentException("Cannot register a ValueSerializer that serializes to a non-primitive type");
        }


        for (ValueSerializer<?, ?> v : valueSerializers) {
            if (v.getDeserializedType().isAssignableFrom(serializer.getDeserializedType())) {
                throw new IllegalArgumentException("A serializer for " + serializer.getDeserializedType() + " is already registered! (" + v.getClass() + ")");
            }
        }

        valueSerializers.add(serializer);
    }

    public boolean isSupportedType(Class<?> type) {
        if (Primitives.isPrimitive(type)) {
            return true;
        }

        for (ValueSerializer<?, ?> serializer : valueSerializers) {
            if (serializer.getDeserializedType().isAssignableFrom(type)) {
                return true;
            }
        }

        return false;
    }

    public <T> T deserialize(Class<T> type, Object serialized) {
        if (serialized == null) {
            return null;
        }
        if (Primitives.isPrimitive(type)) {
            return (T) serialized;
        }

        for (ValueSerializer<?, ?> serializer : valueSerializers) {
            if (serializer.getDeserializedType().isAssignableFrom(type)) {
                return (T) serializer.unsafeDeserialize(serialized);
            }
        }

        throw new IllegalArgumentException("No serializer found for type: " + type);
    }

    public <T> Object serialize(T deserialized) {
        if (deserialized == null) {
            return null;
        }

        if (Primitives.isPrimitive(deserialized.getClass())) {
            return deserialized;
        }

        for (ValueSerializer<?, ?> serializer : valueSerializers) {
            if (serializer.getDeserializedType().isAssignableFrom(deserialized.getClass())) {
                return serializer.unsafeSerialize(deserialized);
            }
        }

        throw new IllegalArgumentException("No serializer found for type: " + deserialized.getClass());
    }

    public Object decode(Class<?> type, String value) {
        if (Primitives.isPrimitive(type)) {
            return Primitives.decode(type, value);
        }

        ValueSerializer<?, ?> serializer = valueSerializers.stream()
                .filter(s -> s.getDeserializedType().isAssignableFrom(type))
                .findFirst()
                .orElseThrow();

        Class<?> serializedType = serializer.getSerializedType();

        return Primitives.decode(serializedType, value);
    }

    public UniqueData createInstance(Class<? extends UniqueData> clazz, UUID id) {
        logger.trace("Creating instance of {} with id {}", clazz, id);
        try {
            Constructor<? extends UniqueData> constructor = clazz.getDeclaredConstructor(DataManager.class, UUID.class);
            constructor.setAccessible(true);
            return constructor.newInstance(this, id);
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException |
                 InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    public PostgresListener getPostgresListener() {
        return pgListener;
    }
}
