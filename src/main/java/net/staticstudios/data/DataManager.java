package net.staticstudios.data;

import com.google.common.base.Preconditions;
import com.google.common.collect.MapMaker;
import net.staticstudios.data.impl.data.PersistentValueImpl;
import net.staticstudios.data.impl.data.ReferenceImpl;
import net.staticstudios.data.impl.h2.H2DataAccessor;
import net.staticstudios.data.impl.pg.PostgresListener;
import net.staticstudios.data.insert.InsertContext;
import net.staticstudios.data.parse.*;
import net.staticstudios.data.util.*;
import net.staticstudios.data.util.TaskQueue;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Blocking;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
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
    private final ConcurrentHashMap<Class<? extends UniqueData>, Map<ColumnValuePairs, UniqueData>> uniqueDataInstanceCache = new ConcurrentHashMap<>(); //todo: weak reference map
    private final ConcurrentHashMap<String, Map<Class<? extends UniqueData>, List<ValueUpdateHandlerWrapper<?, ?>>>> updateHandlers = new ConcurrentHashMap<>();
    private final PostgresListener postgresListener;
    //todo: use the class as a key since we will need to pass the instance as a param on each update
    private final Set<PersistentValueMetadata> registeredUpdateHandlersForColumns = Collections.synchronizedSet(new HashSet<>());

    public DataManager(DataSourceConfig dataSourceConfig) {
        this(dataSourceConfig, true);
    }

    public DataManager(DataSourceConfig dataSourceConfig, boolean setGlobal) {
        if (setGlobal) {
            if (DataManager.useGlobal == false) {
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

        //todo: when we parse UniqueData objects we should build an internal map, and then when we are done auto create the sql if the tables dont exist
        //todo: this will be extremely useful for building the internal cache tables

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

    //todo: when a row is updated, provide the entire row to this method (so we can grab id cols). this is the responsibility of the data accessor impl
    @ApiStatus.Internal
    public void callUpdateHandlers(List<String> columnNames, String schema, String table, String column, Object[] oldSerializedValues, Object[] newSerializedValues) {
        logger.trace("Calling update handlers for {}/{}/{} with old values {} and new values {}", schema, table, column, Arrays.toString(oldSerializedValues), Arrays.toString(newSerializedValues));
        //todo: submit to somewhere for where to run these, configured during setup. default to thread utils
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
                    throw new IllegalArgumentException("Not all ID columns were provided for UniqueData class " + holderClass.getName() + ". Required: " + metadata.idColumns() + ", Provided: " + columnNames);
                }
            }
            UniqueData instance = get(holderClass, idColumns);
            for (ValueUpdateHandlerWrapper<?, ?> wrapper : handlers) {
                Class<?> dataType = wrapper.getDataType();
                Object deserializedOldValue = oldSerializedValues[columnIndex]; //todo: these
                Object deserializedNewValue = newSerializedValues[columnIndex];

                wrapper.unsafeHandle(instance, deserializedOldValue, deserializedNewValue);
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

        //todo: the sql builder needs to be altered to spit out the sql for the just walked class
        //todo: then we need to create those tables in the cache and source, and then finally load all of that data into the cache
        //todo: also, stare listening to events before we start grabbing data, queue them, and then process them after the initial load is done
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
        UniqueDataMetadata metadata = new UniqueDataMetadata(clazz, ValueUtils.parseValue(dataAnnotation.schema()), ValueUtils.parseValue(dataAnnotation.table()), idColumns);
        uniqueDataMetadataMap.put(clazz, metadata);

        for (Field field : ReflectionUtils.getFields(clazz, Relation.class)) {
            Class<? extends UniqueData> dependencyClass = Objects.requireNonNull(ReflectionUtils.getGenericType(field)).asSubclass(UniqueData.class);
            if (!uniqueDataMetadataMap.containsKey(dependencyClass)) {
                extractMetadata(dependencyClass);
            }
        }
    }

    public UniqueDataMetadata getMetadata(Class<? extends UniqueData> clazz) {
        UniqueDataMetadata metadata = uniqueDataMetadataMap.get(clazz);
        Preconditions.checkNotNull(metadata, "UniqueData class %s has not been parsed yet", clazz.getName());
        return metadata;
    }

    @ApiStatus.Internal
    public void delete(List<String> columnNames, String schema, String table, Object[] values) {
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
                Preconditions.checkArgument(found, "Not all ID columns were provided for UniqueData class %s. Required: %s, Provided: %s", uniqueDataMetadata.clazz().getName(), uniqueDataMetadata.idColumns(), Arrays.toString(values));
            }

            UniqueData instance = uniqueDataInstanceCache.getOrDefault(uniqueDataMetadata.clazz(), Collections.emptyMap()).get(new ColumnValuePairs(idColumns));
            if (instance == null) {
                return;
            }
            instance.markDeleted();
        });
    }

    @SuppressWarnings("unchecked")
    public <T extends UniqueData> T get(Class<T> clazz, ColumnValuePair... idColumnValues) {
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

        Preconditions.checkArgument(hasAllIdColumns, "Not all @IdColumn columns were provided for UniqueData class %s. Required: %s, Provided: %s", clazz.getName(), metadata.idColumns(), idColumns);

        T instance;
        if (uniqueDataInstanceCache.containsKey(clazz) && uniqueDataInstanceCache.get(clazz).containsKey(idColumns)) {
            logger.trace("Cache hit for UniqueData class {} with ID columns {}", clazz.getName(), idColumns);
            instance = (T) uniqueDataInstanceCache.get(clazz).get(idColumns);
            if (instance.isDeleted()) {
                return null;
            }
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

        Data dataAnnotation = clazz.getAnnotation(Data.class);
        Preconditions.checkNotNull(dataAnnotation, "UniqueData class %s is missing @Data annotation", clazz.getName());
        String schema = ValueUtils.parseValue(dataAnnotation.schema());
        String table = ValueUtils.parseValue(dataAnnotation.table());
        PersistentValueImpl.delegate(schema, table, instance);
        ReferenceImpl.delegate(instance);

        uniqueDataInstanceCache.computeIfAbsent(clazz, k -> new MapMaker().weakValues().makeMap())
                .put(idColumns, instance);

        logger.trace("Cache miss for UniqueData class {} with ID columns {}. Created new instance.", clazz.getName(), idColumns);

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
                SQLSchema otherSchema = Objects.requireNonNull(sqlBuilder.getSchema(fKey.getSchema()));
                SQLTable otherTable = Objects.requireNonNull(otherSchema.getTable(fKey.getTable()));
                for (Map.Entry<String, String> link : fKey.getLinkingColumns().entrySet()) {
                    String myColumnName = link.getKey();
                    String otherColumnName = link.getValue();
                    SQLColumn otherColumn = Objects.requireNonNull(otherTable.getColumn(otherColumnName));
                    insertContext.set(fKey.getSchema(), fKey.getTable(), otherColumn.getName(), insertContext.getEntries().get(new SimpleColumnMetadata(table.getSchema().getName(), table.getName(), myColumnName, otherColumn.getType())));
                }
            }
        }

        tables.clear(); // rebuild the table set in case we added any new tables from foreign keys
        insertContext.getEntries().forEach((simpleColumnMetadata, o) -> {
            SQLTable table = Objects.requireNonNull(sqlBuilder.getSchema(simpleColumnMetadata.schema())).getTable(simpleColumnMetadata.table());
            tables.add(table);
        });

        //sort tables based on foreign key dependencies. tables who are depended on should come first

        // Build dependency graph: table -> set of tables it depends on
        Map<SQLTable, Set<SQLTable>> dependencyGraph = new HashMap<>();
        for (SQLTable table : tables) {
            Set<SQLTable> dependsOn = new HashSet<>();
            for (ForeignKey fKey : table.getForeignKeys()) {
                SQLSchema otherSchema = Objects.requireNonNull(sqlBuilder.getSchema(fKey.getSchema()));
                SQLTable otherTable = Objects.requireNonNull(otherSchema.getTable(fKey.getTable()));
                dependsOn.add(otherTable);
            }
            dependencyGraph.put(table, dependsOn);
        }

        // DFS to detect cycles
        Set<SQLTable> visited = new HashSet<>();
        Set<SQLTable> stack = new HashSet<>();
        for (SQLTable table : dependencyGraph.keySet()) {
            if (hasCycle(table, dependencyGraph, visited, stack)) {
                throw new IllegalStateException(String.format("Cycle detected in foreign key dependencies involving table %s.%s", table.getSchema().getName(), table.getName()));
            }
        }

        // Topological sort for insert order
        List<SQLTable> orderedTables = new ArrayList<>();
        visited.clear();
        for (SQLTable table : dependencyGraph.keySet()) {
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


            StringBuilder sqlBuilder = new StringBuilder("INSERT INTO \"");
            sqlBuilder.append(schemaName).append("\".\"").append(tableName).append("\" (");
            for (SimpleColumnMetadata column : columnsInTable) {
                sqlBuilder.append("\"").append(column.name()).append("\", ");
            }
            sqlBuilder.setLength(sqlBuilder.length() - 2);
            sqlBuilder.append(") VALUES (");
            sqlBuilder.append("?, ".repeat(columnsInTable.size()));
            sqlBuilder.setLength(sqlBuilder.length() - 2);
            sqlBuilder.append(")");

            String sql = sqlBuilder.toString();
            List<Object> values = new ArrayList<>();
            for (SimpleColumnMetadata column : columnsInTable) {
                Object deserializedValue = insertContext.getEntries().get(column);
                Object serializedValue = deserializedValue; //todo: serialization
                values.add(serializedValue);
            }
            sqlStatements.add(new SQlStatement(sql, values));
        }

        try {
            insertContext.markInserted();
            dataAccessor.insert(sqlStatements, insertMode);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean hasCycle(SQLTable table, Map<SQLTable, Set<SQLTable>> dependencyGraph, Set<SQLTable> visited, Set<SQLTable> stack) {
        if (stack.contains(table)) {
            return true;
        }
        if (visited.contains(table)) {
            return false;
        }
        visited.add(table);
        stack.add(table);
        for (SQLTable dep : dependencyGraph.get(table)) {
            if (hasCycle(dep, dependencyGraph, visited, stack)) {
                return true;
            }
        }
        stack.remove(table);
        return false;
    }

    private void topoSort(SQLTable table, Map<SQLTable, Set<SQLTable>> dependencyGraph, Set<SQLTable> visited, List<SQLTable> ordered) {
        if (visited.contains(table)) {
            return;
        }
        visited.add(table);
        for (SQLTable dep : dependencyGraph.get(table)) {
            topoSort(dep, dependencyGraph, visited, ordered);
        }
        ordered.add(table);
    }
}
