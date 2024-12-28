package net.staticstudios.data;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.pool.HikariPool;
import net.staticstudios.data.data.Data;
import net.staticstudios.data.data.InitialValue;
import net.staticstudios.data.data.UniqueData;
import net.staticstudios.data.data.collection.PersistentManyToManyCollection;
import net.staticstudios.data.data.collection.PersistentUniqueDataCollection;
import net.staticstudios.data.data.collection.PersistentValueCollection;
import net.staticstudios.data.data.collection.SimplePersistentCollection;
import net.staticstudios.data.data.value.Value;
import net.staticstudios.data.data.value.persistent.InitialPersistentValue;
import net.staticstudios.data.data.value.persistent.PersistentValue;
import net.staticstudios.data.data.value.redis.CachedValue;
import net.staticstudios.data.data.value.redis.InitialCachedValue;
import net.staticstudios.data.impl.CachedValueManager;
import net.staticstudios.data.impl.PersistentCollectionManager;
import net.staticstudios.data.impl.PersistentValueManager;
import net.staticstudios.data.impl.pg.PostgresListener;
import net.staticstudios.data.impl.pg.PostgresOperation;
import net.staticstudios.data.key.CellKey;
import net.staticstudios.data.key.DataKey;
import net.staticstudios.data.key.DatabaseKey;
import net.staticstudios.data.primative.Primitives;
import net.staticstudios.data.util.ReflectionUtils;
import net.staticstudios.utils.JedisProvider;
import net.staticstudios.utils.ShutdownStage;
import net.staticstudios.utils.ThreadUtils;
import org.jetbrains.annotations.Blocking;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

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
    private final Multimap<DataKey, ValueUpdateHandler<?>> valueUpdateHandlers;
    private final Multimap<Class<? extends UniqueData>, UUID> uniqueDataIds;
    private final Map<Class<? extends UniqueData>, Map<UUID, UniqueData>> uniqueDataCache;
    private final Multimap<String, Value<?>> dummyValueMap;
    private final Multimap<String, SimplePersistentCollection<?>> dummySimplePersistentCollectionMap;
    private final Multimap<String, PersistentManyToManyCollection<?>> dummyPersistentManyToManyCollectionMap;
    private final Multimap<String, UniqueData> dummyUniqueDataMap;
    private final Map<Class<?>, UniqueData> dummyInstances;
    private final HikariPool connectionPool;
    private final Set<Class<? extends UniqueData>> loadedDependencies = new HashSet<>();
    private final List<ValueSerializer<?, ?>> valueSerializers;
    private final PostgresListener pgListener;
    private final String applicationName;
    private final JedisProvider jedisProvider;
    private final PersistentValueManager persistentValueManager;
    private final PersistentCollectionManager persistentCollectionManager;
    private final CachedValueManager cachedValueManager;

    public DataManager(HikariConfig poolConfig, JedisProvider jedisProvider) {
        this.applicationName = "static_data_manager-" + UUID.randomUUID();
        this.cache = new ConcurrentHashMap<>();
        this.valueUpdateHandlers = Multimaps.synchronizedListMultimap(ArrayListMultimap.create());
        this.uniqueDataCache = new ConcurrentHashMap<>();
        this.dummyValueMap = Multimaps.synchronizedSetMultimap(HashMultimap.create());
        this.dummySimplePersistentCollectionMap = Multimaps.synchronizedSetMultimap(HashMultimap.create());
        this.dummyPersistentManyToManyCollectionMap = Multimaps.synchronizedSetMultimap(HashMultimap.create());
        this.dummyUniqueDataMap = Multimaps.synchronizedSetMultimap(HashMultimap.create());
        this.dummyInstances = new ConcurrentHashMap<>();
        this.uniqueDataIds = Multimaps.synchronizedSetMultimap(HashMultimap.create());
        this.valueSerializers = new CopyOnWriteArrayList<>();
        this.jedisProvider = jedisProvider;

        pgListener = new PostgresListener(this, poolConfig);

        pgListener.addHandler(notification -> { //Call insert handler first so that we can access the data in the other handlers
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
        });

        this.persistentValueManager = new PersistentValueManager(this, pgListener);
        this.persistentCollectionManager = new PersistentCollectionManager(this, pgListener);
        this.cachedValueManager = new CachedValueManager(this);

        pgListener.addHandler(notification -> { //Call delete handler last so that we can access the data in the other handlers
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

        HikariConfig customizedPoolConfig = new HikariConfig();
        poolConfig.copyStateTo(customizedPoolConfig);
        customizedPoolConfig.addDataSourceProperty("ApplicationName", applicationName);

        this.connectionPool = new HikariPool(customizedPoolConfig);

        ThreadUtils.onShutdownRunSync(ShutdownStage.CLEANUP, () -> {
            try {
                connectionPool.shutdown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            cache.clear();
            uniqueDataCache.clear();
            uniqueDataIds.clear();
        });
    }

    public PersistentValueManager getPersistentValueManager() {
        return persistentValueManager;
    }

    public PersistentCollectionManager getPersistentCollectionManager() {
        return persistentCollectionManager;
    }

    public CachedValueManager getCachedValueManager() {
        return cachedValueManager;
    }

    public String getApplicationName() {
        return applicationName;
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

    public Collection<SimplePersistentCollection<?>> getDummyPersistentCollections(String schemaTable) {
        return dummySimplePersistentCollectionMap.get(schemaTable);
    }

    @Blocking
    public <T extends UniqueData> List<T> loadAll(Class<T> clazz) {
        logger.debug("Loading all data for class: {}", clazz);
        try {
            // A dependency is just another UniqueData that is referenced in some way.
            // This clazz is also treated as a dependency, these will all be loaded at the same time
            Set<Class<? extends UniqueData>> dependencies = new HashSet<>();
            extractDependencies(clazz, dependencies);
            dependencies.removeIf(loadedDependencies::contains);

            List<UniqueData> dummyHolders = new ArrayList<>();
            Multimap<UniqueData, DatabaseKey> dummyDatabaseKeys = Multimaps.newSetMultimap(new HashMap<>(), HashSet::new);
            Multimap<UniqueData, SimplePersistentCollection<?>> dummySimplePersistentCollections = Multimaps.newSetMultimap(new HashMap<>(), HashSet::new);
            Multimap<UniqueData, PersistentManyToManyCollection<?>> dummyPersistentManyToManyCollections = Multimaps.newSetMultimap(new HashMap<>(), HashSet::new);
            Multimap<UniqueData, CachedValue<?>> dummyCachedValues = Multimaps.newSetMultimap(new HashMap<>(), HashSet::new);


            for (Class<? extends UniqueData> dependency : dependencies) {
                // A data dependency is some data field on one of our dependencies
                List<Data<?>> dataDependencies = extractDataDependencies(dependency);

                dummyHolders.add(createInstance(dependency, null));

                for (Data<?> data : dataDependencies) {
                    DataKey key = data.getKey();
                    if (key instanceof DatabaseKey dbKey) {
                        dummyDatabaseKeys.put(data.getHolder().getRootHolder(), dbKey);
                    }

                    // This is our first time seeing this value, so we add it to the map for later use
                    if (data instanceof PersistentValue<?> value) {
                        dummyValueMap.put(value.getSchema() + "." + value.getTable(), value);
                    }

                    if (data instanceof SimplePersistentCollection<?> collection) {
                        dummySimplePersistentCollectionMap.put(collection.getSchema() + "." + collection.getTable(), collection);
                        dummySimplePersistentCollections.put(data.getHolder().getRootHolder(), collection);
                    }

                    if (data instanceof PersistentManyToManyCollection<?> collection) {
                        dummyPersistentManyToManyCollectionMap.put(collection.getSchema() + "." + collection.getJunctionTable(), collection);
                        dummyPersistentManyToManyCollections.put(data.getHolder().getRootHolder(), collection);
                    }

                    if (data instanceof CachedValue<?> cachedValue) {
                        dummyValueMap.put(data.getHolder().getRootHolder().getSchema() + "." + data.getHolder().getRootHolder().getTable(), cachedValue);
                        dummyCachedValues.put(data.getHolder().getRootHolder(), cachedValue);
                    }
                }
            }

            try (Connection connection = getConnection()) {
                // Load all the unique data first
                for (UniqueData dummyHolder : dummyHolders) {
                    loadUniqueData(connection, dummyHolder);
                }

                //Load PersistentValues
                for (UniqueData dummyHolder : dummyHolders) {
                    Collection<DatabaseKey> keys = dummyDatabaseKeys.get(dummyHolder);

                    // Use a multimap so we can easily group CellKeys together
                    // The actual key (DataKey) for this map is solely used for grouping entries with the same schema, table, data column, and id column
                    Multimap<DataKey, CellKey> persistentDataColumns = Multimaps.newListMultimap(new HashMap<>(), ArrayList::new);

                    for (DatabaseKey key : keys) {
                        if (key instanceof CellKey cellKey) {
                            persistentDataColumns.put(new DataKey(cellKey.getSchema(), cellKey.getTable(), cellKey.getColumn(), cellKey.getIdColumn()), cellKey);
                        }
                    }

                    for (DataKey key : persistentDataColumns.keySet()) {
                        persistentValueManager.loadAllFromDatabase(connection, dummyHolder, persistentDataColumns.get(key));
                    }
                }

                //Load SimplePersistentCollections
                for (UniqueData dummyHolder : dummyHolders) {
                    Collection<SimplePersistentCollection<?>> dummyCollections = dummySimplePersistentCollections.get(dummyHolder);

                    for (SimplePersistentCollection<?> dummyCollection : dummyCollections) {
                        persistentCollectionManager.loadAllFromDatabase(connection, dummyHolder, dummyCollection);
                    }
                }

                //Load PersistentManyToManyCollections
                for (UniqueData dummyHolder : dummyHolders) {
                    Collection<PersistentManyToManyCollection<?>> dummyCollections = dummyPersistentManyToManyCollections.get(dummyHolder);

                    for (PersistentManyToManyCollection<?> dummyCollection : dummyCollections) {
                        persistentCollectionManager.loadJunctionTablesFromDatabase(connection, dummyCollection);
                    }
                }

                //Load CachedValues
                for (UniqueData dummyHolder : dummyHolders) {
                    Collection<CachedValue<?>> dummyValues = dummyCachedValues.get(dummyHolder);

                    for (CachedValue<?> dummyValue : dummyValues) {
                        cachedValueManager.loadAllFromRedis(dummyValue);
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
        InsertContext context = buildInsertContext(holder, initialData);

        try (
                Connection connection = getConnection();
                Jedis jedis = jedisProvider.getJedis()
        ) {
            insertIntoDataSource(connection, jedis, context);
            insertIntoCache(context);
        } catch (SQLException e) {
            logger.error("Error inserting data", e);
            throw new RuntimeException(e);
        }
    }

    @SafeVarargs
    public final <I extends InitialValue<?, ?>> void insertAsync(UniqueData holder, I... initialData) {
        InsertContext context = buildInsertContext(holder, initialData);
        insertIntoCache(context);

        ThreadUtils.submit(() -> {
            try (
                    Connection connection = getConnection();
                    Jedis jedis = jedisProvider.getJedis()
            ) {
                insertIntoDataSource(connection, jedis, context);
            } catch (SQLException e) {
                logger.error("Error inserting data", e);
                throw new RuntimeException(e);
            }
        });
    }

    @SafeVarargs
    private <I extends InitialValue<?, ?>> InsertContext buildInsertContext(UniqueData holder, I... initialData) {
        Map<PersistentValue<?>, InitialPersistentValue> initialPersistentValues = new HashMap<>();
        Map<CachedValue<?>, InitialCachedValue> initialCachedValues = new HashMap<>();

        for (Field field : ReflectionUtils.getFields(holder.getClass())) {
            field.setAccessible(true);

            if (Data.class.isAssignableFrom(field.getType())) {
                try {
                    Data<?> data = (Data<?>) field.get(holder);
                    if (data instanceof PersistentValue<?> pv) {
                        initialPersistentValues.put(pv, new InitialPersistentValue(pv, pv.getDefaultValue()));
                    }
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        for (InitialValue<?, ?> data : initialData) {
            if (data instanceof InitialPersistentValue initial) {
                initialPersistentValues.put(initial.getValue(), initial);
            } else if (data instanceof InitialCachedValue initial) {
                if (initial.getInitialDataValue() != null) {
                    initialCachedValues.put(initial.getValue(), initial);
                }
            } else {
                throw new IllegalArgumentException("Unsupported initial data type: " + data.getClass());
            }
        }

        for (Map.Entry<PersistentValue<?>, InitialPersistentValue> initial : initialPersistentValues.entrySet()) {
            PersistentValue<?> pv = initial.getKey();
            InitialPersistentValue value = initial.getValue();

            if (Primitives.isPrimitive(pv.getDataType()) && !Primitives.getPrimitive(pv.getDataType()).isNullable()) {
                Preconditions.checkNotNull(value.getInitialDataValue(), "Initial data value cannot be null for primitive type: " + pv.getDataType());
            }
        }

        for (Map.Entry<CachedValue<?>, InitialCachedValue> initial : initialCachedValues.entrySet()) {
            CachedValue<?> cv = initial.getKey();
            InitialCachedValue value = initial.getValue();

            if (Primitives.isPrimitive(cv.getDataType()) && !Primitives.getPrimitive(cv.getDataType()).isNullable()) {
                Preconditions.checkNotNull(value.getInitialDataValue(), "Initial data value cannot be null for primitive type: " + cv.getDataType());
            }
        }

        return new InsertContext(holder, initialPersistentValues, initialCachedValues);
    }

    private void insertIntoCache(InsertContext context) {
        addUniqueData(context.holder());

        for (InitialPersistentValue data : context.initialPersistentValues().values()) {
            UniqueData pvHolder = data.getValue().getHolder().getRootHolder();
            CellKey idColumn = new CellKey(
                    pvHolder.getSchema(),
                    pvHolder.getTable(),
                    pvHolder.getIdentifier().getColumn(),
                    pvHolder.getId(),
                    pvHolder.getIdentifier().getColumn()
            );

            //do not call PersistentValueManager#updateCache since we need to cache both values
            //before PersistentCollectionManager#handlePersistentValueCacheUpdated is called, otherwise we get unexpected behavior
            cache(data.getValue().getKey(), data.getValue().getDataType(), data.getInitialDataValue(), Instant.now());
            cache(idColumn, UUID.class, context.holder().getId(), Instant.now());


            //Alert the collection manager of this change so it can update what it's keeping track of
            persistentCollectionManager.handlePersistentValueCacheUpdated(
                    data.getValue().getSchema(),
                    data.getValue().getTable(),
                    data.getValue().getColumn(),
                    context.holder().getId(),
                    data.getValue().getIdColumn(),
                    null,
                    data.getInitialDataValue()
            );

            persistentCollectionManager.handlePersistentValueCacheUpdated(
                    idColumn.getSchema(),
                    idColumn.getTable(),
                    idColumn.getColumn(),
                    pvHolder.getId(),
                    idColumn.getIdColumn(),
                    null,
                    pvHolder.getId()
            );
        }

        for (InitialCachedValue data : context.initialCachedValues().values()) {
            cache(data.getValue().getKey(), data.getValue().getDataType(), data.getInitialDataValue(), Instant.now());
        }
    }

    private void insertIntoDataSource(Connection connection, Jedis jedis, InsertContext context) throws SQLException {
        persistentValueManager.setInDatabase(connection, new ArrayList<>(context.initialPersistentValues().values()));
        cachedValueManager.setInRedis(jedis, new ArrayList<>(context.initialCachedValues().values()));
    }

    public <T extends UniqueData> void delete(T holder) {
        List<Data<?>> dataList = new ArrayList<>();

        for (Field field : ReflectionUtils.getFields(holder.getClass())) {
            field.setAccessible(true);

            if (Data.class.isAssignableFrom(field.getType())) {
                try {
                    Data<?> data = (Data<?>) field.get(holder);
                    dataList.add(data);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        deleteFromCache(holder, dataList);
        removeUniqueData(holder.getClass(), holder.getId());
        //todo: remove add, remove, and update handlers
        //todo: handle foreign PVs
        //todo: handle CVs
        //todo: handle PVCs
        //todo: handle PUDCs
        //todo: handle PMTMCs

        ThreadUtils.submit(() -> {
            try (Connection connection = getConnection();
                 Jedis jedis = jedisProvider.getJedis()
            ) {
                deleteFromDataSource(connection, jedis, holder);
            } catch (SQLException e) {
                logger.error("Error deleting data", e);
                throw new RuntimeException(e);
            }
        });
    }

    private void deleteFromCache(UniqueData holder, List<Data<?>> dataList) {
        for (Data<?> data : dataList) {
            if (data instanceof PersistentValue<?> value) {
                persistentValueManager.uncache(value);

                //Uncache the id column as well
                persistentValueManager.uncache(
                        value.getSchema(),
                        value.getTable(),
                        value.getIdColumn(),
                        holder.getId(),
                        value.getIdColumn()
                );
            } else if (data instanceof CachedValue<?> value) {
                cache.remove(value.getKey());
            } else if (data instanceof PersistentValueCollection<?> collection) {
                persistentCollectionManager.removeEntriesFromCache(collection, persistentCollectionManager.getCollectionEntries(collection));
            } else if (data instanceof PersistentUniqueDataCollection<?> collection) {
                persistentCollectionManager.removeEntriesFromInternalMap(collection, persistentCollectionManager.getCollectionEntries(collection));
            }
        }

    }

    private void deleteFromDataSource(Connection connection, Jedis jedis, UniqueData holder) throws SQLException {
        String sql = "DELETE FROM " + holder.getSchema() + "." + holder.getTable() + " WHERE " + holder.getIdentifier().getColumn() + " = ?";
        logSQL(sql);

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, holder.getId());
            statement.execute();
        }
    }

    public <T extends UniqueData> List<T> getAll(Class<T> clazz) {
        return uniqueDataIds.get(clazz).stream().map(id -> get(clazz, id)).toList();
    }

    private void extractDependencies(Class<? extends UniqueData> clazz, @NotNull Set<Class<? extends UniqueData>> dependencies) throws Exception {
        if (dependencies.contains(clazz)) {
            return;
        }

        dependencies.add(clazz);

        if (!UniqueData.class.isAssignableFrom(clazz)) {
            return;
        }

        UniqueData dummy = createInstance(clazz, null);
        dummyInstances.put(clazz, dummy);

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

    public void dump() {
        for (Map.Entry<DataKey, CacheEntry> entry : cache.entrySet()) {
            logger.debug("{} -> {}", entry.getKey(), entry.getValue().value());
        }
    }

    private void loadUniqueData(Connection connection, UniqueData dummyHolder) throws SQLException {
        String sql = "SELECT " + dummyHolder.getIdentifier().getColumn() + " FROM " + dummyHolder.getSchema() + "." + dummyHolder.getTable();
        logSQL(sql);

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

    public <T extends UniqueData> T get(Class<T> clazz, UUID id) throws DataDoesNotExistException {
        Map<UUID, UniqueData> uniqueData = uniqueDataCache.get(clazz);
        if (uniqueData != null) {
            UniqueData data = uniqueData.get(id);
            if (data != null) {
                return clazz.cast(data);
            }
        }

        throw new DataDoesNotExistException("UniqueData does not exist: " + id);
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
     * @param key           The key to cache the value under
     * @param valueDataType The type of the value
     * @param value         The value to cache
     * @param instant       The instant at which the value was set
     */
    public <T> void cache(DataKey key, Class<?> valueDataType, T value, Instant instant) {
        if (value != null && !valueDataType.isInstance(value)) {
            throw new IllegalArgumentException("Value is not of the correct type! Expected: " + valueDataType + ", got: " + value.getClass());
        }
        if (Primitives.isPrimitive(valueDataType) && !Primitives.getPrimitive(valueDataType).isNullable()) {
            Preconditions.checkNotNull(value, "Value cannot be null for primitive type: " + valueDataType);
        }

        CacheEntry existing = cache.get(key);

        if (existing != null && existing.instant().isAfter(instant)) {
            logger.trace("Not caching value: {} -> {}, existing entry is newer. {} vs {} (existing)", key, value, instant, existing.instant());
            return;
        } else {
            logger.trace("Caching value: {} -> {}", key, value);
        }

        if (existing != null) {
            logger.debug("Caching value to replace existing entry. Difference in instants: {} vs {} (existing)", instant, existing.instant());
        }

        cache.put(key, CacheEntry.of(Objects.requireNonNullElse(value, NULL_MARKER), instant));

        Collection<ValueUpdateHandler<?>> updateHandlers = valueUpdateHandlers.get(key);

        Object oldValue = existing == null ? null : existing.value() == NULL_MARKER ? null : existing.value();
        Object newValue = value == NULL_MARKER ? null : value;

        if (Objects.equals(oldValue, newValue)) {
            return;
        }

        for (ValueUpdateHandler<?> updateHandler : updateHandlers) {
            try {
                updateHandler.unsafeHandle(oldValue, newValue);
            } catch (Exception e) {
                logger.error("Error handling value update. Key: {}", key, e);
            }
        }
    }

    public void uncache(DataKey key) {
        Collection<ValueUpdateHandler<?>> updateHandlers = valueUpdateHandlers.get(key);
        CacheEntry existing = cache.get(key);

        for (ValueUpdateHandler<?> updateHandler : updateHandlers) {
            try {
                updateHandler.unsafeHandle(existing.value(), null);
            } catch (Exception e) {
                logger.error("Error handling value update. Key: {}", key, e);
            }
        }

        cache.remove(key);
        valueUpdateHandlers.removeAll(key);
    }

    public void registerValueUpdateHandler(DataKey key, ValueUpdateHandler<?> handler) {
        valueUpdateHandlers.put(key, handler);
    }

    public Connection getConnection() throws SQLException {
        return connectionPool.getConnection();
    }

    public JedisProvider getJedisProvider() {
        return jedisProvider;
    }

    public Jedis getJedis() {
        return jedisProvider.getJedis();
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

    public String getIdColumn(Class<? extends UniqueData> clazz) {
        if (!dummyInstances.containsKey(clazz)) {
            try {
                dummyInstances.put(clazz, createInstance(clazz, null));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return dummyInstances.get(clazz).getIdentifier().getColumn();
    }
}
