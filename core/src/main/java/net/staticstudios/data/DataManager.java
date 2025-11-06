package net.staticstudios.data;

import com.google.common.base.Preconditions;
import com.google.common.collect.MapMaker;
import net.staticstudios.data.impl.data.*;
import net.staticstudios.data.impl.h2.H2DataAccessor;
import net.staticstudios.data.impl.pg.PostgresListener;
import net.staticstudios.data.insert.InsertContext;
import net.staticstudios.data.parse.*;
import net.staticstudios.data.primative.Primitives;
import net.staticstudios.data.util.*;
import net.staticstudios.data.util.TaskQueue;
import net.staticstudios.data.utils.Link;
import net.staticstudios.utils.ThreadUtils;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Blocking;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class DataManager {
    private static Boolean useGlobal = null;
    private static DataManager instance;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final String applicationName;
    private final DataAccessor dataAccessor;
    private final SQLBuilder sqlBuilder;
    private final TaskQueue taskQueue;
    private final ConcurrentHashMap<Class<? extends UniqueData>, UniqueDataMetadata> uniqueDataMetadataMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Class<? extends UniqueData>, Map<ColumnValuePairs, UniqueData>> uniqueDataInstanceCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Map<Class<? extends UniqueData>, List<ValueUpdateHandlerWrapper<?, ?>>>> updateHandlers = new ConcurrentHashMap<>();
    private final PostgresListener postgresListener;
    private final Set<PersistentValueMetadata> registeredUpdateHandlersForColumns = Collections.synchronizedSet(new HashSet<>());
    private final List<ValueSerializer<?, ?>> valueSerializers = new CopyOnWriteArrayList<>();
    //todo: custom types are serialized and deserialized all the time currently, we should have a cache for these. caffeine with time based eviction sounds good.

    public DataManager(DataSourceConfig dataSourceConfig) {
        this(dataSourceConfig, true);
    }

    public DataManager(DataSourceConfig dataSourceConfig, boolean setGlobal) {
        if (setGlobal) {
            if (Boolean.FALSE.equals(DataManager.useGlobal)) {
                throw new IllegalStateException("DataManager global instance has been disabled");
            }
            Preconditions.checkArgument(instance == null, "DataManager instance already exists");
            instance = this;
        }
        DataManager.useGlobal = setGlobal;
        applicationName = "static_data_manager_v3-" + UUID.randomUUID();
        postgresListener = new PostgresListener(this, dataSourceConfig);
        sqlBuilder = new SQLBuilder(this);
        this.taskQueue = new TaskQueue(dataSourceConfig, applicationName);
        dataAccessor = new H2DataAccessor(this, postgresListener, taskQueue);

        //todo: when we reconnect to postgres, refresh the internal cache from the source

        //todo: support for CachedValues
    }

    public static DataManager getInstance() {
        Preconditions.checkState(DataManager.instance != null, "Global DataManager instance has not been initialized");
        return DataManager.instance;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public DataAccessor getDataAccessor() {
        return dataAccessor;
    }

    public SQLBuilder getSQLBuilder() {
        return sqlBuilder;
    }

    public InsertContext createInsertContext() {
        return new InsertContext(this);
    }

    public void addUpdateHandler(String schema, String table, String column, ValueUpdateHandlerWrapper<?, ?> handler) {//todo: allow us to specify what data type to convert the data to. this is useful when this method is called externally
        String key = schema + "." + table + "." + column;
        updateHandlers.computeIfAbsent(key, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(handler.getHolderClass(), k -> new CopyOnWriteArrayList<>())
                .add(handler);
    }

    @ApiStatus.Internal
    public void callUpdateHandlers(List<String> columnNames, String schema, String table, String column, Object[] oldSerializedValues, Object[] newSerializedValues) {
        logger.trace("Calling update handlers for {}.{}.{} with old values {} and new values {}", schema, table, column, Arrays.toString(oldSerializedValues), Arrays.toString(newSerializedValues));
        Map<Class<? extends UniqueData>, List<ValueUpdateHandlerWrapper<?, ?>>> handlersForColumn = updateHandlers.get(schema + "." + table + "." + column);
        if (handlersForColumn == null) {
            return;
        }

        int columnIndex = columnNames.indexOf(column);
        Preconditions.checkArgument(columnIndex != -1, "Column %s not found in provided name names %s", column, columnNames);

        for (Map.Entry<Class<? extends UniqueData>, List<ValueUpdateHandlerWrapper<?, ?>>> entry : handlersForColumn.entrySet()) {
            Class<? extends UniqueData> holderClass = entry.getKey();
            UniqueDataMetadata metadata = getMetadata(holderClass);
            List<ValueUpdateHandlerWrapper<?, ?>> handlers = entry.getValue();
            ColumnValuePair[] idColumns = new ColumnValuePair[metadata.idColumns().size()];
            for (ColumnMetadata idColumn : metadata.idColumns()) {
                boolean found = false;
                for (int i = 0; i < columnNames.size(); i++) {
                    if (idColumn.name().equals(columnNames.get(i))) {
                        idColumns[metadata.idColumns().indexOf(idColumn)] = new ColumnValuePair(idColumn.name(), oldSerializedValues[i]);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw new IllegalArgumentException("Not all ID columnsInReferringTable were provided for UniqueData class " + holderClass.getName() + ". Required: " + metadata.idColumns() + ", Provided: " + columnNames);
                }
            }
            UniqueData instance = getInstance(holderClass, idColumns);
            for (ValueUpdateHandlerWrapper<?, ?> wrapper : handlers) {
                Class<?> dataType = wrapper.getDataType();
                Object deserializedOldValue = deserialize(dataType, oldSerializedValues[columnIndex]);
                Object deserializedNewValue = deserialize(dataType, newSerializedValues[columnIndex]);

                //todo: allow configuring where to submit update handlers to. note that we cannot call them immediately since we are inside a transaction.
                ThreadUtils.submit(() -> wrapper.unsafeHandle(instance, deserializedOldValue, deserializedNewValue));
            }
        }
    }

    @ApiStatus.Internal
    public void registerUpdateHandler(PersistentValueMetadata metadata, Collection<ValueUpdateHandlerWrapper<?, ?>> handlers) {
        if (registeredUpdateHandlersForColumns.add(metadata)) {
            for (ValueUpdateHandlerWrapper<?, ?> handler : handlers) {
                addUpdateHandler(metadata.getSchema(), metadata.getTable(), metadata.getColumn(), handler);
            }
        }
    }

    public List<ValueUpdateHandlerWrapper<?, ?>> getUpdateHandlers(String schema, String table, String column, Class<? extends UniqueData> holderClass) {
        String key = schema + "." + table + "." + column;
        if (updateHandlers.containsKey(key) && updateHandlers.get(key).containsKey(holderClass)) {
            return updateHandlers.get(key).get(holderClass);
        }
        return Collections.emptyList();
    }

    @SafeVarargs
    public final void load(Class<? extends UniqueData>... classes) {
        for (Class<? extends UniqueData> clazz : classes) {
            extractMetadata(clazz);
        }
        List<DDLStatement> defs = new ArrayList<>();
        for (Class<? extends UniqueData> clazz : classes) {
            defs.addAll(sqlBuilder.parse(clazz));
        }

        for (DDLStatement ddl : defs) {
            try {
                dataAccessor.runDDL(ddl);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        try {
            dataAccessor.postDDL();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void extractMetadata(Class<? extends UniqueData> clazz) {
        Preconditions.checkArgument(!uniqueDataMetadataMap.containsKey(clazz), "UniqueData class %s has already been parsed", clazz.getName());
        Data dataAnnotation = clazz.getAnnotation(Data.class);
        Preconditions.checkNotNull(dataAnnotation, "UniqueData class %s is missing @Data annotation", clazz.getName());

        List<ColumnMetadata> idColumns = new ArrayList<>();
        for (Field field : ReflectionUtils.getFields(clazz, PersistentValue.class)) {
            IdColumn idColumnAnnotation = field.getAnnotation(IdColumn.class);
            if (idColumnAnnotation == null) {
                continue;
            }
            idColumns.add(new ColumnMetadata(
                    ValueUtils.parseValue(dataAnnotation.schema()),
                    ValueUtils.parseValue(dataAnnotation.table()),
                    ValueUtils.parseValue(idColumnAnnotation.name()),
                    ReflectionUtils.getGenericType(field),
                    false,
                    false,
                    ""
            ));
        }
        Preconditions.checkArgument(!idColumns.isEmpty(), "UniqueData class %s must have at least one @IdColumn annotated PersistentValue field", clazz.getName());
        String schema = ValueUtils.parseValue(dataAnnotation.schema());
        String table = ValueUtils.parseValue(dataAnnotation.table());
        Map<Field, PersistentCollectionMetadata> persistentCollectionMetadataMap = new HashMap<>();
        persistentCollectionMetadataMap.putAll(PersistentOneToManyCollectionImpl.extractMetadata(clazz));
        persistentCollectionMetadataMap.putAll(PersistentManyToManyCollectionImpl.extractMetadata(clazz));
        persistentCollectionMetadataMap.putAll(PersistentOneToManyValueCollectionImpl.extractMetadata(clazz, schema));
        UniqueDataMetadata metadata = new UniqueDataMetadata(clazz, schema, table, idColumns, PersistentValueImpl.extractMetadata(schema, table, clazz), ReferenceImpl.extractMetadata(clazz), persistentCollectionMetadataMap);
        uniqueDataMetadataMap.put(clazz, metadata);

        for (Field field : ReflectionUtils.getFields(clazz, Relation.class)) {
            Class<?> genericType = ReflectionUtils.getGenericType(field);
            if (genericType != null && UniqueData.class.isAssignableFrom(genericType)) {
                Class<? extends UniqueData> dependencyClass = genericType.asSubclass(UniqueData.class);
                if (!uniqueDataMetadataMap.containsKey(dependencyClass)) {
                    extractMetadata(dependencyClass);
                }
            }
        }
    }

    public UniqueDataMetadata getMetadata(Class<? extends UniqueData> clazz) {
        UniqueDataMetadata metadata = uniqueDataMetadataMap.get(clazz);
        Preconditions.checkNotNull(metadata, "UniqueData class %s has not been parsed yet", clazz.getName());
        return metadata;
    }

    @ApiStatus.Internal
    public void handleDelete(List<String> columnNames, String schema, String table, Object[] values) {
        uniqueDataMetadataMap.values().forEach(uniqueDataMetadata -> {
            if (!uniqueDataMetadata.schema().equals(schema) || !uniqueDataMetadata.table().equals(table)) {
                return;
            }

            ColumnValuePair[] idColumns = new ColumnValuePair[uniqueDataMetadata.idColumns().size()];
            for (ColumnMetadata idColumn : uniqueDataMetadata.idColumns()) {
                boolean found = false;
                for (int i = 0; i < columnNames.size(); i++) {
                    if (idColumn.name().equals(columnNames.get(i))) {
                        idColumns[uniqueDataMetadata.idColumns().indexOf(idColumn)] = new ColumnValuePair(idColumn.name(), values[i]);
                        found = true;
                        break;
                    }
                }
                Preconditions.checkArgument(found, "Not all ID columnsInReferringTable were provided for UniqueData class %s. Required: %s, Provided: %s", uniqueDataMetadata.clazz().getName(), uniqueDataMetadata.idColumns(), Arrays.toString(values));
            }

            UniqueData instance = uniqueDataInstanceCache.getOrDefault(uniqueDataMetadata.clazz(), Collections.emptyMap()).get(new ColumnValuePairs(idColumns));
            if (instance == null) {
                return;
            }
            instance.markDeleted();
        });
    }

    @ApiStatus.Internal
    public synchronized void updateIdColumns(List<String> columnNames, String schema, String table, String column, Object[] oldValues, Object[] newValues) {
        uniqueDataMetadataMap.values().forEach(uniqueDataMetadata -> {
            if (!uniqueDataMetadata.schema().equals(schema) || !uniqueDataMetadata.table().equals(table)) {
                return;
            }

            boolean idColumnWasUpdated = false;
            for (ColumnMetadata idColumn : uniqueDataMetadata.idColumns()) {
                if (idColumn.name().equals(column)) {
                    idColumnWasUpdated = true;
                    break;
                }
            }

            if (!idColumnWasUpdated) {
                return;
            }

            ColumnValuePair[] oldIdColumns = new ColumnValuePair[uniqueDataMetadata.idColumns().size()];
            ColumnValuePair[] newIdColumns = new ColumnValuePair[uniqueDataMetadata.idColumns().size()];
            for (ColumnMetadata idColumn : uniqueDataMetadata.idColumns()) {
                boolean found = false;
                for (int i = 0; i < columnNames.size(); i++) {
                    if (idColumn.name().equals(columnNames.get(i))) {
                        oldIdColumns[uniqueDataMetadata.idColumns().indexOf(idColumn)] = new ColumnValuePair(idColumn.name(), oldValues[i]);
                        newIdColumns[uniqueDataMetadata.idColumns().indexOf(idColumn)] = new ColumnValuePair(idColumn.name(), newValues[i]);
                        found = true;
                        break;
                    }
                }
                Preconditions.checkArgument(found, "Not all ID columnsInReferringTable were provided for UniqueData class %s. Required: %s, Provided: %s", uniqueDataMetadata.clazz().getName(), uniqueDataMetadata.idColumns(), Arrays.toString(oldValues));
            }
            if (Arrays.equals(oldIdColumns, newIdColumns)) {
                return; // no change to id columnsInReferringTable here
            }

            ColumnValuePairs oldIdCols = new ColumnValuePairs(oldIdColumns);
            Map<ColumnValuePairs, UniqueData> classCache = uniqueDataInstanceCache.get(uniqueDataMetadata.clazz());
            if (classCache == null) {
                return;
            }
            UniqueData instance = classCache.remove(oldIdCols);
            if (instance == null) {
                return;
            }
            ColumnValuePairs newIdCols = new ColumnValuePairs(newIdColumns);
            instance.setIdColumns(newIdCols);
            classCache.put(newIdCols, instance);
        });
    }

    public <T extends UniqueData> List<T> query(Class<T> clazz, String where, List<Object> values) {
        UniqueDataMetadata metadata = getMetadata(clazz);
        Preconditions.checkNotNull(metadata, "UniqueData class %s has not been parsed yet", clazz.getName());

        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        for (ColumnMetadata idColumn : metadata.idColumns()) {
            sb.append("\"").append(idColumn.name()).append("\", ");
        }
        sb.setLength(sb.length() - 2);
        sb.append(" FROM \"").append(metadata.schema()).append("\".\"").append(metadata.table()).append("\" ");
        sb.append(where);
        @Language("SQL") String sql = sb.toString();
        List<T> results = new ArrayList<>();
        try (ResultSet rs = dataAccessor.executeQuery(sql, values)) {
            while (rs.next()) {
                ColumnValuePair[] idColumns = new ColumnValuePair[metadata.idColumns().size()];
                for (int i = 0; i < metadata.idColumns().size(); i++) {
                    ColumnMetadata idColumn = metadata.idColumns().get(i);
                    Object value = rs.getObject(idColumn.name());
                    idColumns[i] = new ColumnValuePair(idColumn.name(), value);
                }
                T instance = getInstance(clazz, idColumns);
                if (instance != null) {
                    results.add(instance);
                }
            }
            return results;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends UniqueData> T getInstance(Class<T> clazz, ColumnValuePair... idColumnValues) {
        ColumnValuePairs idColumns = new ColumnValuePairs(idColumnValues);
        UniqueDataMetadata metadata = getMetadata(clazz);
        Preconditions.checkNotNull(metadata, "UniqueData class %s has not been parsed yet", clazz.getName());
        boolean hasAllIdColumns = true;
        for (ColumnMetadata idColumn : metadata.idColumns()) {
            boolean found = false;
            for (ColumnValuePair providedIdColumn : idColumns) {
                if (idColumn.name().equals(providedIdColumn.column())) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                hasAllIdColumns = false;
                break;
            }
        }

        for (ColumnValuePair providedIdColumn : idColumns) {
            Preconditions.checkNotNull(providedIdColumn.value(), "ID name value for name %s in UniqueData class %s cannot be null", providedIdColumn.column(), clazz.getName());
        }

        Preconditions.checkArgument(hasAllIdColumns, "Not all @IdColumn columnsInReferringTable were provided for UniqueData class %s. Required: %s, Provided: %s", clazz.getName(), metadata.idColumns(), idColumns);

        T instance;
        if (uniqueDataInstanceCache.containsKey(clazz) && uniqueDataInstanceCache.get(clazz).containsKey(idColumns)) {
            logger.trace("Cache hit for UniqueData class {} with ID columnsInReferringTable {}", clazz.getName(), idColumns);
            instance = (T) uniqueDataInstanceCache.get(clazz).get(idColumns);
            if (instance.isDeleted()) {
                return null;
            }
            return instance;
        }

        try {
            Constructor<T> constructor = clazz.getDeclaredConstructor();
            constructor.setAccessible(true);
            instance = constructor.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        instance.setDataManager(this);
        instance.setIdColumns(idColumns);

        String schema = metadata.schema();
        String table = metadata.table();

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT 1 FROM \"").append(schema).append("\".\"").append(table).append("\" WHERE ");
        for (ColumnValuePair columnValuePair : idColumns) {
            sqlBuilder.append("\"").append(columnValuePair.column()).append("\" = ? AND ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 5);
        @Language("SQL") String sql = sqlBuilder.toString();
        try (ResultSet rs = dataAccessor.executeQuery(sql, idColumns.stream().map(ColumnValuePair::value).toList())) {
            if (!rs.next()) {
                return null;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        PersistentValueImpl.delegate(instance);
        ReferenceImpl.delegate(instance);
        PersistentOneToManyCollectionImpl.delegate(instance);
        PersistentManyToManyCollectionImpl.delegate(instance);
        PersistentOneToManyValueCollectionImpl.delegate(instance);

        uniqueDataInstanceCache.computeIfAbsent(clazz, k -> new MapMaker().weakValues().makeMap())
                .put(idColumns, instance);

        logger.trace("Cache miss for UniqueData class {} with ID columnsInReferringTable {}. Created new instance.", clazz.getName(), idColumns);

        return instance;
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

    public void submitAsyncTask(ConnectionConsumer task) {
        taskQueue.submitTask(task);
    }


    public void submitAsyncTask(ConnectionJedisConsumer task) {
        taskQueue.submitTask(task);
    }

    public void insert(InsertContext insertContext, InsertMode insertMode) {
        //todo: when inserting validate all id and required values are present - this will be enforced by h2, but we should do it here for better logging/errors.
        Set<SQLTable> tables = new HashSet<>();
        insertContext.getEntries().forEach((simpleColumnMetadata, o) -> {
            SQLTable table = Objects.requireNonNull(sqlBuilder.getSchema(simpleColumnMetadata.schema())).getTable(simpleColumnMetadata.table());
            tables.add(table);
        });

        // handle foreign keys. for each foreign key, if we have a value for the local column, we need to set the value for the foreign column. this consists of linking ids mostly.
        for (SQLTable table : tables) {
            for (ForeignKey fKey : table.getForeignKeys()) {
                SQLSchema referencedSchema = Objects.requireNonNull(sqlBuilder.getSchema(fKey.getReferencedSchema()));
                SQLTable referencedTable = Objects.requireNonNull(referencedSchema.getTable(fKey.getReferencedTable()));
                for (Link link : fKey.getLinkingColumns()) {
                    String myColumnName = link.columnInReferringTable();
                    String otherColumnName = link.columnInReferencedTable();
                    SQLColumn otherColumn = Objects.requireNonNull(referencedTable.getColumn(otherColumnName));

                    // if its nullable and we don't have a value, skip it.
                    if (otherColumn.isNullable()) {
                        continue;
                    }

                    insertContext.set(fKey.getReferencedSchema(), fKey.getReferencedTable(), otherColumn.getName(), insertContext.getEntries().get(new SimpleColumnMetadata(fKey.getReferringSchema(), fKey.getReferringTable(), myColumnName, otherColumn.getType())), InsertStrategy.PREFER_EXISTING);
                }
            }
        }

        tables.clear(); // rebuild the referringTable set in case we added any new tables from foreign keys
        insertContext.getEntries().forEach((simpleColumnMetadata, o) -> {
            SQLTable table = Objects.requireNonNull(sqlBuilder.getSchema(simpleColumnMetadata.schema())).getTable(simpleColumnMetadata.table());
            tables.add(table);
        });

        // Build dependency graph: referringTable -> set of tables it depends on
        Map<String, Set<SQLTable>> dependencyGraph = new HashMap<>();
        for (SQLTable table : tables) {
            Set<SQLTable> dependsOn = new HashSet<>();
            for (ForeignKey fKey : table.getForeignKeys()) {
                SQLSchema referencedSchema = Objects.requireNonNull(sqlBuilder.getSchema(fKey.getReferencedSchema()));
                SQLTable referencedTable = Objects.requireNonNull(referencedSchema.getTable(fKey.getReferencedTable()));

                boolean addDependency = true;
                // if one of the linking columnsInReferringTable is not present in the insert context, we can't add the dependency
                for (Link link : fKey.getLinkingColumns()) {
                    Object value = insertContext.getEntries().entrySet().stream()
                            .filter(entry -> {
                                SimpleColumnMetadata key = entry.getKey();
                                return key.schema().equals(fKey.getReferencedSchema()) &&
                                        key.table().equals(fKey.getReferencedTable()) &&
                                        key.name().equals(link.columnInReferencedTable());
                            })
                            .findFirst()
                            .orElse(null);

                    if (value == null) {
                        addDependency = false;
                        break;
                    }
                }

                if (addDependency) {
                    dependsOn.add(referencedTable);
                }
            }
            if (!dependsOn.isEmpty()) {
                dependencyGraph.put(table.getName(), dependsOn);
            }
        }

        // DFS to detect cycles
        Set<SQLTable> visited = new HashSet<>();
        Set<SQLTable> stack = new HashSet<>();
        for (SQLTable table : tables) {
            if (hasCycle(table, dependencyGraph, visited, stack)) {
                throw new IllegalStateException(String.format("Cycle detected in foreign key dependencies involving referringTable %s.%s", table.getSchema().getName(), table.getName()));
            }
        }

        // Topological sort for insert order
        List<SQLTable> orderedTables = new ArrayList<>();
        visited.clear();
        for (SQLTable table : tables) {
            topoSort(table, dependencyGraph, visited, orderedTables);
        }

        List<SQlStatement> sqlStatements = new ArrayList<>();

        Map<String, List<SimpleColumnMetadata>> columnsToInsert = new HashMap<>();
        for (Map.Entry<SimpleColumnMetadata, Object> entry : insertContext.getEntries().entrySet()) {
            SimpleColumnMetadata column = entry.getKey();
            columnsToInsert.computeIfAbsent(column.table(), k -> new ArrayList<>())
                    .add(column);
        }

        for (SQLTable table : orderedTables) {
            String schemaName = table.getSchema().getName();
            String tableName = table.getName();
            List<SimpleColumnMetadata> columnsInTable = columnsToInsert.get(tableName);


            StringBuilder h2SqlBuilder = new StringBuilder("MERGE INTO \"");
            h2SqlBuilder.append(schemaName).append("\".\"").append(tableName).append("\" AS target USING (VALUES (");
            Map<SimpleColumnMetadata, InsertStrategy> conflicts = new HashMap<>();
            h2SqlBuilder.append("?, ".repeat(columnsInTable.size()));
            h2SqlBuilder.setLength(h2SqlBuilder.length() - 2);
            h2SqlBuilder.append(")) AS source (");
            for (SimpleColumnMetadata column : columnsInTable) {
                h2SqlBuilder.append("\"").append(column.name()).append("\", ");
                InsertStrategy strategy = insertContext.getInsertStrategies().get(column);
                if (strategy != null) {
                    conflicts.put(column, strategy);
                }
            }
            h2SqlBuilder.setLength(h2SqlBuilder.length() - 2);
            h2SqlBuilder.append(") ON ");
            for (ColumnMetadata idColumn : table.getIdColumns()) {
                h2SqlBuilder.append("target.\"").append(idColumn.name()).append("\" = source.\"").append(idColumn.name()).append("\" AND ");
            }
            h2SqlBuilder.setLength(h2SqlBuilder.length() - 5);
            h2SqlBuilder.append(" WHEN NOT MATCHED THEN INSERT (");
            for (SimpleColumnMetadata column : columnsInTable) {
                h2SqlBuilder.append("\"").append(column.name()).append("\", ");
            }
            h2SqlBuilder.setLength(h2SqlBuilder.length() - 2);
            h2SqlBuilder.append(") VALUES (");
            for (SimpleColumnMetadata column : columnsInTable) {
                h2SqlBuilder.append("source.\"").append(column.name()).append("\", ");
            }
            h2SqlBuilder.setLength(h2SqlBuilder.length() - 2);
            h2SqlBuilder.append(")");

            List<SimpleColumnMetadata> overwriteExisting = new ArrayList<>();
            for (Map.Entry<SimpleColumnMetadata, InsertStrategy> entry : conflicts.entrySet()) {
                if (entry.getValue() == InsertStrategy.OVERWRITE_EXISTING) {
                    overwriteExisting.add(entry.getKey());
                }
            }
            if (!overwriteExisting.isEmpty()) {
                h2SqlBuilder.append(" WHEN MATCHED THEN UPDATE SET ");
                for (SimpleColumnMetadata column : overwriteExisting) {
                    h2SqlBuilder.append("\"").append(column.name()).append("\" = source.\"").append(column.name()).append("\", ");
                }
                h2SqlBuilder.setLength(h2SqlBuilder.length() - 2);
            }

            StringBuilder pgSqlBuilder = new StringBuilder("INSERT INTO \"");
            pgSqlBuilder.append(schemaName).append("\".\"").append(tableName).append("\" (");
            for (SimpleColumnMetadata column : columnsInTable) {
                pgSqlBuilder.append("\"").append(column.name()).append("\", ");
            }
            pgSqlBuilder.setLength(pgSqlBuilder.length() - 2);
            pgSqlBuilder.append(") VALUES (");
            pgSqlBuilder.append("?, ".repeat(columnsInTable.size()));
            pgSqlBuilder.setLength(pgSqlBuilder.length() - 2);
            pgSqlBuilder.append(")");

            if (!conflicts.isEmpty()) {
                pgSqlBuilder.append(" ON CONFLICT (");
                for (ColumnMetadata idColumn : table.getIdColumns()) {
                    pgSqlBuilder.append("\"").append(idColumn.name()).append("\", ");
                }
                pgSqlBuilder.setLength(pgSqlBuilder.length() - 2);
                pgSqlBuilder.append(") DO ");
                if (!overwriteExisting.isEmpty()) {
                    pgSqlBuilder.append("UPDATE SET ");
                    for (SimpleColumnMetadata column : overwriteExisting) {
                        pgSqlBuilder.append("\"").append(column.name()).append("\" = EXCLUDED.\"").append(column.name()).append("\", ");
                    }
                    pgSqlBuilder.setLength(pgSqlBuilder.length() - 2);
                } else {
                    pgSqlBuilder.append("NOTHING");
                }
            }

            String h2Sql = h2SqlBuilder.toString();
            String pgSql = pgSqlBuilder.toString();
            List<Object> values = new ArrayList<>();
            for (SimpleColumnMetadata column : columnsInTable) {
                Object deserializedValue = insertContext.getEntries().get(column);
                Object serializedValue = serialize(deserializedValue);
                values.add(serializedValue);
            }
            sqlStatements.add(new SQlStatement(h2Sql, pgSql, values));
        }

        try {
            insertContext.markInserted();
            dataAccessor.insert(sqlStatements, insertMode);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> T get(String schema, String table, String column, ColumnValuePairs idColumns, List<Link> idColumnLinks, Class<T> dataType) {
        //todo: caffeine cache for these as well.
        StringBuilder sqlBuilder = new StringBuilder().append("SELECT \"").append(column).append("\" FROM \"").append(schema).append("\".\"").append(table).append("\" WHERE ");
        for (ColumnValuePair columnValuePair : idColumns) {
            String name = columnValuePair.column();
            for (Link link : idColumnLinks) {
                if (link.columnInReferringTable().equals(columnValuePair.column())) {
                    name = link.columnInReferencedTable();
                    break;
                }
            }
            sqlBuilder.append("\"").append(name).append("\" = ? AND ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 5);
        @Language("SQL") String sql = sqlBuilder.toString();
        try (ResultSet rs = dataAccessor.executeQuery(sql, idColumns.stream().map(ColumnValuePair::value).toList())) {
            Object serializedValue = null;
            if (rs.next()) {
                serializedValue = rs.getObject(column, getSerializedType(dataType));
            }
            //todo: do some type validation here, either on the serialized or un serialized type. this method will be exposed so we need to be careful
            return deserialize(dataType, serializedValue);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @ApiStatus.Internal
    public void set(String schema, String table, String column, ColumnValuePairs idColumns, List<Link> idColumnLinks, Object value, int delay) {
        StringBuilder sqlBuilder;
        if (idColumnLinks.isEmpty()) {
            sqlBuilder = new StringBuilder().append("UPDATE \"").append(schema).append("\".\"").append(table).append("\" SET \"").append(column).append("\" = ? WHERE ");
            for (ColumnValuePair columnValuePair : idColumns) {
                String name = columnValuePair.column();
                sqlBuilder.append("\"").append(name).append("\" = ? AND ");
            }
            sqlBuilder.setLength(sqlBuilder.length() - 5);
        } else { // we're dealing with a foreign key
            //todo: use update on conclifct bla bla bla for pg
            sqlBuilder = new StringBuilder().append("MERGE INTO \"").append(schema).append("\".\"").append(table).append("\" target USING (VALUES (?");
            sqlBuilder.append(", ?".repeat(idColumns.getPairs().length));
            sqlBuilder.append(")) AS source (\"").append(column).append("\"");
            for (ColumnValuePair columnValuePair : idColumns) {
                String name = columnValuePair.column();
                for (Link link : idColumnLinks) {
                    if (link.columnInReferringTable().equals(columnValuePair.column())) {
                        name = link.columnInReferencedTable();
                        break;
                    }
                }
                sqlBuilder.append(", \"").append(name).append("\"");
            }
            sqlBuilder.append(") ON ");
            for (ColumnValuePair columnValuePair : idColumns) {
                String name = columnValuePair.column();
                for (Link link : idColumnLinks) {
                    if (link.columnInReferringTable().equals(columnValuePair.column())) {
                        name = link.columnInReferencedTable();
                        break;
                    }
                }
                sqlBuilder.append("target.\"").append(name).append("\" = source.\"").append(name).append("\" AND ");
            }
            sqlBuilder.setLength(sqlBuilder.length() - 5);
            sqlBuilder.append(" WHEN MATCHED THEN UPDATE SET \"").append(column).append("\" = source.\"").append(column).append("\" WHEN NOT MATCHED THEN INSERT (\"").append(column).append("\"");
            for (ColumnValuePair columnValuePair : idColumns) {
                String name = columnValuePair.column();
                for (Link link : idColumnLinks) {
                    if (link.columnInReferringTable().equals(columnValuePair.column())) {
                        name = link.columnInReferencedTable();
                        break;
                    }
                }
                sqlBuilder.append(", \"").append(name).append("\"");
            }
            sqlBuilder.append(") VALUES (source.\"").append(column).append("\"");
            for (ColumnValuePair columnValuePair : idColumns) {
                String name = columnValuePair.column();
                for (Link link : idColumnLinks) {
                    if (link.columnInReferringTable().equals(columnValuePair.column())) {
                        name = link.columnInReferencedTable();
                        break;
                    }
                }
                sqlBuilder.append(", source.\"").append(name).append("\"");
            }
            sqlBuilder.append(")");
        }
        @Language("SQL") String h2Sql = sqlBuilder.toString();

        sqlBuilder.setLength(0);
        sqlBuilder.append("UPDATE \"").append(schema).append("\".\"").append(table).append("\" SET \"").append(column).append("\" = ? WHERE ");
        for (ColumnValuePair columnValuePair : idColumns) {
            String name = columnValuePair.column();
            for (Link link : idColumnLinks) {
                if (link.columnInReferringTable().equals(columnValuePair.column())) {
                    name = link.columnInReferencedTable();
                    break;
                }
            }
            sqlBuilder.append("\"").append(name).append("\" = ? AND ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 5);
        @Language("SQL") String pgSql = sqlBuilder.toString();

        List<Object> values = new ArrayList<>(1 + idColumns.getPairs().length);
        values.add(serialize(value));
        for (ColumnValuePair columnValuePair : idColumns) {
            values.add(columnValuePair.value());
        }
        try {
            dataAccessor.executeUpdate(SQLTransaction.Statement.of(h2Sql, pgSql), values, delay);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean hasCycle(SQLTable table, Map<String, Set<SQLTable>> dependencyGraph, Set<SQLTable> visited, Set<SQLTable> stack) {
        if (stack.contains(table)) {
            return true;
        }
        if (visited.contains(table)) {
            return false;
        }
        visited.add(table);
        stack.add(table);
        for (SQLTable dep : dependencyGraph.getOrDefault(table.getName(), Collections.emptySet())) {
            if (hasCycle(dep, dependencyGraph, visited, stack)) {
                return true;
            }
        }
        stack.remove(table);
        return false;
    }

    private void topoSort(SQLTable table, Map<String, Set<SQLTable>> dependencyGraph, Set<SQLTable> visited, List<SQLTable> ordered) {
        if (visited.contains(table)) {
            return;
        }
        visited.add(table);
        for (SQLTable dep : dependencyGraph.getOrDefault(table.getName(), Collections.emptySet())) {
            topoSort(dep, dependencyGraph, visited, ordered);
        }
        ordered.add(table);
    }

    public void registerValueSerializer(ValueSerializer<?, ?> serializer) {
        if (Primitives.isPrimitive(serializer.getDeserializedType())) {
            throw new IllegalArgumentException("Cannot register a serializer for a primitive type");
        }

        if (!Primitives.isPrimitive(serializer.getSerializedType())) {
            throw new IllegalArgumentException("Cannot register a ValueSerializer that serializes to a non-primitive type");
        }

        for (ValueSerializer<?, ?> s : valueSerializers) {
            if (s.getDeserializedType().isAssignableFrom(serializer.getDeserializedType())) {
                throw new IllegalArgumentException("A serializer for " + serializer.getDeserializedType() + " is already registered! (" + s.getClass() + ")");
            }
        }

        valueSerializers.add(serializer);
    }

    private ValueSerializer<?, ?> getValueSerializer(Class<?> deserializedType) {
        for (ValueSerializer<?, ?> s : valueSerializers) {
            if (s.getDeserializedType().isAssignableFrom(deserializedType)) {
                return s;
            }
        }

        throw new IllegalStateException("No ValueSerializer registered for type " + deserializedType.getName());
    }

    public <T> T deserialize(Class<T> clazz, Object serialized) {
        if (serialized == null || Primitives.isPrimitive(clazz)) {
            return (T) serialized;
        }
        return (T) deserialize(getValueSerializer(clazz), serialized);
    }

    public <T> T serialize(Object deserialized) {
        if (deserialized == null || Primitives.isPrimitive(deserialized.getClass())) {
            return (T) deserialized;
        }
        return (T) serialize(getValueSerializer(deserialized.getClass()), deserialized);
    }

    private <D, S> D deserialize(ValueSerializer<D, S> serializer, Object serialized) {
        return serializer.deserialize(serializer.getSerializedType().cast(serialized));
    }

    private <D, S> S serialize(ValueSerializer<D, S> serializer, Object deserialized) {
        return serializer.serialize(serializer.getDeserializedType().cast(deserialized));
    }

    public Class<?> getSerializedType(Class<?> clazz) {
        if (Primitives.isPrimitive(clazz)) {
            return clazz;
        }
        ValueSerializer<?, ?> serializer = getValueSerializer(clazz);
        return serializer.getSerializedType();
    }

//    /**
//     * For internal use only. A dummy instance has no DataManager, no id columnsInReferringTable, and is marked as deleted.
//     *
//     * @param clazz The UniqueData class to create a dummy instance of.
//     * @param <T>   The type of UniqueData.
//     */
//    @ApiStatus.Internal
//    public <T extends UniqueData> T createDummyInstance(Class<T> clazz) {
//        T instance;
//        try {
//            Constructor<T> constructor = clazz.getDeclaredConstructor();
//            constructor.setAccessible(true);
//            instance = constructor.newInstance();
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//
//        instance.markDeleted();
//        return instance;
//    }
}
