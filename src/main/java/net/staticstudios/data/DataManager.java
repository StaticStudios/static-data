package net.staticstudios.data;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import net.staticstudios.data.data.Data;
import net.staticstudios.data.data.InitialValue;
import net.staticstudios.data.data.collection.PersistentManyToManyCollection;
import net.staticstudios.data.data.collection.PersistentUniqueDataCollection;
import net.staticstudios.data.data.collection.SimplePersistentCollection;
import net.staticstudios.data.data.value.InitialCachedValue;
import net.staticstudios.data.data.value.InitialPersistentValue;
import net.staticstudios.data.data.value.Value;
import net.staticstudios.data.impl.CachedValueManager;
import net.staticstudios.data.impl.PersistentCollectionManager;
import net.staticstudios.data.impl.PersistentValueManager;
import net.staticstudios.data.impl.pg.PostgresListener;
import net.staticstudios.data.impl.pg.PostgresOperation;
import net.staticstudios.data.key.CellKey;
import net.staticstudios.data.key.DataKey;
import net.staticstudios.data.key.DatabaseKey;
import net.staticstudios.data.primative.Primitives;
import net.staticstudios.data.util.TaskQueue;
import net.staticstudios.data.util.*;
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
import java.util.stream.Collectors;

/**
 * Manages all data operations.
 */
public class DataManager extends SQLLogger {
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
    private final Set<Class<? extends UniqueData>> loadedDependencies = new HashSet<>();
    private final List<ValueSerializer<?, ?>> valueSerializers;
    private final PostgresListener pgListener;
    private final String applicationName;
    private final PersistentValueManager persistentValueManager;
    private final PersistentCollectionManager persistentCollectionManager;
    private final CachedValueManager cachedValueManager;

    private final TaskQueue taskQueue;


    /**
     * Create a new data manager.
     *
     * @param dataSourceConfig the data source configuration
     */
    public DataManager(DataSourceConfig dataSourceConfig) {
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

        pgListener = new PostgresListener(this, dataSourceConfig);

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
        this.cachedValueManager = new CachedValueManager(this, dataSourceConfig);

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


        this.taskQueue = new TaskQueue(dataSourceConfig, applicationName);

        ThreadUtils.onShutdownRunSync(ShutdownStage.CLEANUP, () -> {
            cache.clear();
            uniqueDataCache.clear();
            uniqueDataIds.clear();
        });
    }

    /**
     * Submit a blocking task to the priority task queue.
     * This method is blocking and will wait for the task to complete before returning.
     *
     * @param task The task to submit
     */
    @Blocking
    public void submitBlockingTask(ConnectionConsumer task) {
        taskQueue.submitTask(task).join();
    }

    public void submitAsyncTask(ConnectionConsumer task) {
        taskQueue.submitTask(task)
                .exceptionally(e -> {
                    logger.error("Error submitting async task", e);
                    return null;
                });
    }

    /**
     * Submit a blocking task to the priority task queue.
     * This method is blocking and will wait for the task to complete before returning.
     *
     * @param task The task to submit
     */
    @Blocking
    public void submitBlockingTask(ConnectionJedisConsumer task) {
        taskQueue.submitTask(task).join();
    }

    public void submitAsyncTask(ConnectionJedisConsumer task) {
        taskQueue.submitTask(task);
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

    public Collection<Value<?>> getDummyValues(String schemaTable) {
        return dummyValueMap.get(schemaTable);
    }

    public Collection<UniqueData> getDummyUniqueData(String schemaTable) {
        return dummyUniqueDataMap.get(schemaTable);
    }

    public UniqueData getDummyInstance(Class<?> clazz) {
        return dummyInstances.get(clazz);
    }

    public Collection<SimplePersistentCollection<?>> getDummyPersistentCollections(String schemaTable) {
        return dummySimplePersistentCollectionMap.get(schemaTable);
    }

    public Collection<PersistentManyToManyCollection<?>> getDummyPersistentManyToManyCollection(String schemaTable) {
        return dummyPersistentManyToManyCollectionMap.get(schemaTable);
    }

    public Collection<PersistentManyToManyCollection<?>> getAllDummyPersistentManyToManyCollections() {
        return new HashSet<>(dummyPersistentManyToManyCollectionMap.values());
    }

    /**
     * Load all the data for a given class from the datasource(s) into the cache.
     * This method will recursively load all dependencies of the given class.
     * Note that calling this method registers a specific data type.
     * Without calling this method, the data manager will have no knowledge of the data type.
     * This should be called at the start of the application to ensure all data types are registered.
     * This method is inherently blocking.
     *
     * @param clazz The class to load data for
     * @param <T>   The type of data to load
     * @return A list of all the loaded data
     */
    @Blocking
    public <T extends UniqueData> List<T> loadAll(Class<T> clazz) {
        logger.debug("Registering: {}", clazz.getName());
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

                    if (data instanceof Reference<?> reference) {
                        PersistentValue<?> value = reference.getBackingValue();
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
                        dummyValueMap.put(cachedValue.getSchema() + "." + cachedValue.getTable(), cachedValue);
                        dummyCachedValues.put(data.getHolder().getRootHolder(), cachedValue);
                    }
                }
            }

            submitBlockingTask((connection, jedis) -> {
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
                        cachedValueManager.loadAllFromRedis(jedis, dummyValue);
                    }
                }
            });

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

    /**
     * Create a new batch insert operation.
     *
     * @return The batch insert operation
     */
    public final BatchInsert batchInsert() {
        return new BatchInsert(this);
    }

    /**
     * Synchronously insert data into the datasource(s) and cache.
     *
     * @param batch The batch of data to insert
     */
    @Blocking
    public final void insertBatch(BatchInsert batch, List<ConnectionConsumer> intermediateActions, List<Runnable> preInsertActions, List<Runnable> postInsertActions) {
        submitBlockingTask((connection, jedis) -> {
            connection.setAutoCommit(false);
            for (InsertContext context : batch.getInsertionContexts()) {
                insertIntoDataSource(connection, jedis, context);
            }
            for (ConnectionConsumer action : intermediateActions) {
                action.accept(connection);
            }
            connection.setAutoCommit(true);

            for (InsertContext context : batch.getInsertionContexts()) {
                insertIntoCache(context);
            }

            for (Runnable action : preInsertActions) {
                try {
                    action.run();
                } catch (Exception e) {
                    logger.error("Error running pre-insert action", e);
                }
            }

            for (Runnable action : postInsertActions) {
                try {
                    action.run();
                } catch (Exception e) {
                    logger.error("Error running post-insert action", e);
                }
            }
        });
    }

    /**
     * Asynchronously insert data into the datasource(s) and cache.
     * The data will be instantly inserted into the cache, and then the datasource(s) will be updated asynchronously.
     * This method is non-blocking and will return immediately.
     * The cached data can be accessed immediately after this method is called.
     *
     * @param batch The batch of data to insert
     */
    public final void insertBatchAsync(BatchInsert batch, List<ConnectionConsumer> intermediateActions, List<Runnable> preInsertActions, List<Runnable> postInsertActions) {
        for (InsertContext context : batch.getInsertionContexts()) {
            insertIntoCache(context);
        }
        for (Runnable action : preInsertActions) {
            try {
                action.run();
            } catch (Exception e) {
                logger.error("Error running pre-insert action", e);
            }
        }

        submitAsyncTask((connection, jedis) -> {
            connection.setAutoCommit(false);
            for (InsertContext context : batch.getInsertionContexts()) {
                insertIntoDataSource(connection, jedis, context);
            }
            for (ConnectionConsumer action : intermediateActions) {
                action.accept(connection);
            }
            connection.setAutoCommit(true);

            for (Runnable action : postInsertActions) {
                try {
                    action.run();
                } catch (Exception e) {
                    logger.error("Error running post-insert action", e);
                }
            }
        });
    }

    /**
     * Synchronously insert data into the datasource(s) and cache.
     *
     * @param holder      The root holder of the data to insert
     * @param initialData The initial data to insert
     */
    @Blocking
    public final void insert(UniqueData holder, InitialValue<?, ?>... initialData) {
        InsertContext context = buildInsertContext(holder, initialData);

        submitBlockingTask((connection, jedis) -> {
            insertIntoDataSource(connection, jedis, context);
            insertIntoCache(context);
        });
    }

    /**
     * Asynchronously insert data into the datasource(s) and cache.
     * The data will be instantly inserted into the cache, and then the datasource(s) will be updated asynchronously.
     * This method is non-blocking and will return immediately.
     * The cached data can be accessed immediately after this method is called.
     *
     * @param holder      The root holder of the data to insert
     * @param initialData The initial data to insert
     */
    public final void insertAsync(UniqueData holder, InitialValue<?, ?>... initialData) {
        InsertContext context = buildInsertContext(holder, initialData);
        insertIntoCache(context);

        submitAsyncTask((connection, jedis) -> {
            insertIntoDataSource(connection, jedis, context);
        });
    }

    public InsertContext buildInsertContext(UniqueData holder, InitialValue<?, ?>... initialData) {
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
        //todo: REFACTOR: similar to deletions, delegate this to the managers

        List<InitialPersistentDataWrapper> initialPersistentDataWrappers = new ArrayList<>();
        for (InitialPersistentValue data : context.initialPersistentValues().values()) {
            InsertionStrategy insertionStrategy = data.getValue().getInsertionStrategy();

            //do not call PersistentValueManager#updateCache since we need to cache both values
            //before PersistentCollectionManager#handlePersistentValueCacheUpdated is called, otherwise we get unexpected behavior
            boolean updateCache = !cache.containsKey(data.getValue().getKey()) || insertionStrategy == InsertionStrategy.OVERWRITE_EXISTING;
            Object oldValue;

            try {
                oldValue = get(data.getValue().getKey());
                if (oldValue == NULL_MARKER) {
                    oldValue = null;
                }
            } catch (DataDoesNotExistException e) {
                oldValue = null;
            }
            initialPersistentDataWrappers.add(new InitialPersistentDataWrapper(data, updateCache, oldValue));

            if (updateCache) {
                cache(data.getValue().getKey(), data.getValue().getDataType(), data.getInitialDataValue(), Instant.now(), false);
            }
        }

        for (InitialPersistentDataWrapper wrapper : initialPersistentDataWrappers) {
            InitialPersistentValue data = wrapper.data;
            boolean updateCache = wrapper.updateCache;
            Object oldValue = wrapper.oldValue;
            UniqueData pvHolder = data.getValue().getHolder().getRootHolder();
            CellKey idColumn = new CellKey(
                    pvHolder.getSchema(),
                    pvHolder.getTable(),
                    pvHolder.getIdentifier().getColumn(),
                    pvHolder.getId(),
                    pvHolder.getIdentifier().getColumn()
            );

            //Alert the collection manager of this change so it can update what it's keeping track of
            if (updateCache) {
                persistentCollectionManager.handlePersistentValueCacheUpdated(
                        data.getValue().getSchema(),
                        data.getValue().getTable(),
                        data.getValue().getColumn(),
                        context.holder().getId(),
                        data.getValue().getIdColumn(),
                        oldValue,
                        data.getInitialDataValue()
                );
            }

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
            cache(data.getValue().getKey(), data.getValue().getDataType(), data.getInitialDataValue(), Instant.now(), false);
        }
    }

    private void insertIntoDataSource(Connection connection, Jedis jedis, InsertContext context) throws SQLException {
        if (context.initialPersistentValues().isEmpty() && context.initialCachedValues().isEmpty()) {
            String sql = "INSERT INTO " + context.holder().getSchema() + "." + context.holder().getTable() + " (" + context.holder().getIdentifier().getColumn() + ") VALUES (?)";
            logSQL(sql);

            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setObject(1, context.holder().getId());
                statement.executeUpdate();
            }
            return;
        }
        persistentValueManager.insertInDatabase(connection, context.holder(), new ArrayList<>(context.initialPersistentValues().values()));
        cachedValueManager.setInRedis(jedis, new ArrayList<>(context.initialCachedValues().values()));
    }

    /**
     * Delete data from the datasource(s) and cache.
     * This method will immediately delete the data from the cache, and then asynchronously delete the data from the datasource(s).
     * This method is non-blocking and will return immediately after deleting the data from the cache.
     * Data will be recursively deleted based off of the {@link DeletionStrategy} of each member of the data object.
     *
     * @param holder The root holder of the data to delete
     */
    public void delete(UniqueData holder) {
        DeleteContext context = buildDeleteContext(holder);
        logger.trace("Deleting: {}", context);
        deleteFromCache(context);
        //todo: i really dislike that update handlers are called when things are deleted. revisit this

        submitAsyncTask((connection, jedis) -> deleteFromDataSource(connection, jedis, context));
    }

    /**
     * Synchronously delete data from the datasource(s) and cache.
     * Data will be recursively deleted based off of the {@link DeletionStrategy} of each member of the data object.
     * This method is inherently blocking.
     *
     * @param holder The root holder of the data to delete
     */
    @Blocking
    public void deleteSync(UniqueData holder) {
        DeleteContext context = buildDeleteContext(holder);
        logger.trace("Deleting: {}", context);
        deleteFromCache(context);

        submitBlockingTask((connection, jedis) -> deleteFromDataSource(connection, jedis, context));
    }

    private DeleteContext buildDeleteContext(UniqueData holder) {
        Set<Data<?>> toDelete = new HashSet<>();
        Set<UniqueData> holders = new HashSet<>();
        extractDataToDelete(holder, holders, toDelete);
        Map<DataKey, Object> oldValues = new HashMap<>();
        for (Data<?> data : toDelete) {
            if (data instanceof PersistentValue<?> || data instanceof CachedValue<?>) {
                try {
                    Object value = get(data.getKey());
                    oldValues.put(data.getKey(), value == NULL_MARKER ? null : value);
                } catch (DataDoesNotExistException e) {
                    // This is fine, it just means the value was null
                }
            }
        }

        return new DeleteContext(holders, toDelete, oldValues);
    }

    private void extractDataToDelete(UniqueData holder, Set<UniqueData> holders, Set<Data<?>> toDelete) {
        if (holders.contains(holder)) {
            return;
        }
        holders.add(holder);

        //Add the root holder's id column to the list of things to delete, just in case the holder is empty
        toDelete.add(PersistentValue.of(holder.getRootHolder(), UUID.class, holder.getRootHolder().getIdentifier().getColumn()));

        for (Field field : ReflectionUtils.getFields(holder.getClass())) {
            field.setAccessible(true);

            if (Data.class.isAssignableFrom(field.getType())) {
                try {
                    Data<?> data = (Data<?>) field.get(holder);

                    //Always delete the backing value since it's in the same table as the holder
                    if (data instanceof Reference<?> reference) {
                        toDelete.add(reference.getBackingValue());
                    }

                    if (data.getDeletionStrategy() == DeletionStrategy.NO_ACTION) {
                        continue;
                    }

                    if (data instanceof Reference<?> reference) {
                        UUID id = reference.getForeignId();
                        if (id != null) {
                            UniqueData foreignData = reference.get();
                            if (foreignData != null) {
                                extractDataToDelete(foreignData, holders, toDelete);
                            }
                        }
                    }

                    if (data instanceof PersistentUniqueDataCollection<?> collection) {
                        if (collection.getDeletionStrategy() == DeletionStrategy.CASCADE) {
                            for (UniqueData dataInCollection : collection) {
                                extractDataToDelete(dataInCollection, holders, toDelete);
                            }
                        }
                    }

                    if (data instanceof PersistentManyToManyCollection<?> collection) {
                        if (collection.getDeletionStrategy() == DeletionStrategy.CASCADE) {
                            for (UniqueData dataInCollection : collection) {
                                extractDataToDelete(dataInCollection, holders, toDelete);
                            }
                        }
                    }

                    toDelete.add(data);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void deleteFromCache(DeleteContext context) {
        persistentCollectionManager.deleteFromCache(context);
        persistentValueManager.deleteFromCache(context);
        cachedValueManager.deleteFromCache(context);

        for (UniqueData holder : context.holders()) {
            removeUniqueData(holder.getClass(), holder.getId());
        }
    }

    @Blocking
    private void deleteFromDataSource(Connection connection, Jedis jedis, DeleteContext context) throws SQLException {
        for (UniqueData holder : context.holders()) {
            String sql = "DELETE FROM " + holder.getSchema() + "." + holder.getTable() + " WHERE " + holder.getIdentifier().getColumn() + " = ?";
            logSQL(sql);

            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setObject(1, holder.getId());
                statement.executeUpdate();
            }
        }
        persistentValueManager.deleteFromDatabase(connection, context);
        persistentCollectionManager.deleteFromDatabase(connection, context);
        cachedValueManager.deleteFromRedis(jedis, context);
    }

    /**
     * Get all data of a given type from the cache.
     *
     * @param clazz The class of data to get
     * @param <T>   The type of data to get
     * @return A list of all the data of the given type in the cache
     */
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

    public synchronized void removeFromCacheIf(Predicate<DataKey> predicate) {
        Set<DataKey> keysToRemove = cache.keySet().stream().filter(predicate).collect(Collectors.toSet());
        keysToRemove.forEach(cache::remove);
    }

    /**
     * Dump the internal cache to the log.
     */
    public void dump() {
        logger.debug("Dumping cache:");
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

        logger.info("Loaded {} instances of {}", uniqueDataIds.get(dummyHolder.getClass()).size(), dummyHolder.getClass().getName());
    }

    /**
     * Get a data object from the cache.
     *
     * @param data The data object to get
     * @param <T>  The value type stored in the data object
     * @return The data object from the cache
     * @throws DataDoesNotExistException If the data object does not exist in the cache
     */
    public <T> T get(Data<T> data) throws DataDoesNotExistException {
        return get(data.getKey());
    }

    /**
     * Get a data object from the cache.
     *
     * @param key The key of the data object to get
     * @param <T> The value type stored in the data object
     * @return The data object from the cache
     * @throws DataDoesNotExistException If the data object does not exist in the cache
     */
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

    /**
     * Get a {@link UniqueData} object from the cache.
     *
     * @param clazz The class of the data object to get
     * @param id    The id of the data object to get
     * @param <T>   The type of data object to get
     * @return The data object from the cache, or null if it does not exist
     */
    public <T extends UniqueData> T get(Class<T> clazz, UUID id) {
        if (id == null) {
            return null;
        }
        Map<UUID, UniqueData> uniqueData = uniqueDataCache.get(clazz);
        if (uniqueData != null) {
            UniqueData data = uniqueData.get(id);
            if (data != null) {
                return clazz.cast(data);
            }
        }

        return null;
    }

    public void addUniqueData(UniqueData data) {
        logger.trace("Adding unique data to cache: {}({})", data.getClass(), data.getId());

        CellKey idColumn = new CellKey(
                data.getSchema(),
                data.getTable(),
                data.getIdentifier().getColumn(),
                data.getId(),
                data.getIdentifier().getColumn()
        );
        cache(idColumn, UUID.class, data.getId(), Instant.now(), false);

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
    public <T> void cache(DataKey key, Class<?> valueDataType, T value, Instant instant, boolean callUpdateHandlers) {
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
            logger.trace("Caching value to replace existing entry. Difference in instants: {} vs {} (existing)", instant, existing.instant());
        }

        cache.put(key, CacheEntry.of(Objects.requireNonNullElse(value, NULL_MARKER), instant));

        Collection<ValueUpdateHandler<?>> updateHandlers = valueUpdateHandlers.get(key);

        Object oldValue = existing == null ? null : existing.value() == NULL_MARKER ? null : existing.value();
        Object newValue = value == NULL_MARKER ? null : value;

        if (Objects.equals(oldValue, newValue)) {
            return;
        }

        if (callUpdateHandlers) {

            for (ValueUpdateHandler<?> updateHandler : updateHandlers) {
                try {
                    updateHandler.unsafeHandle(oldValue, newValue);
                } catch (Exception e) {
                    logger.error("Error handling value update. Key: {}", key, e);
                }
            }
        }
    }

    public int getCacheSize() {
        return cache.size();
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

    public Class<?> getSerializedDataType(Class<?> deserializedType) {
        if (Primitives.isPrimitive(deserializedType)) {
            return deserializedType;
        }

        for (ValueSerializer<?, ?> serializer : valueSerializers) {
            if (serializer.getDeserializedType().isAssignableFrom(deserializedType)) {
                return serializer.getSerializedType();
            }
        }

        throw new IllegalArgumentException("No serializer found for type: " + deserializedType);
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

    /**
     * Block the calling thread until all previously enqueued tasks have been completed
     */
    @Blocking
    public void flushTaskQueue() {
        //This will add a task to the queue and block until it's done
        submitBlockingTask(connection -> {
            //Ignore
        });
    }

    private record InitialPersistentDataWrapper(InitialPersistentValue data, boolean updateCache, Object oldValue) {
    }
}
