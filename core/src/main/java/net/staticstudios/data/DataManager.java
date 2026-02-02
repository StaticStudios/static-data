package net.staticstudios.data;

import com.google.common.base.Preconditions;
import com.google.common.collect.MapMaker;
import net.staticstudios.data.impl.DataAccessor;
import net.staticstudios.data.impl.data.*;
import net.staticstudios.data.impl.h2.H2DataAccessor;
import net.staticstudios.data.impl.pg.PostgresListener;
import net.staticstudios.data.impl.redis.RedisListener;
import net.staticstudios.data.insert.BatchInsert;
import net.staticstudios.data.insert.InsertContext;
import net.staticstudios.data.insert.PostInsertAction;
import net.staticstudios.data.parse.DDLStatement;
import net.staticstudios.data.parse.SQLBuilder;
import net.staticstudios.data.parse.SQLColumn;
import net.staticstudios.data.parse.SQLTable;
import net.staticstudios.data.primative.Primitives;
import net.staticstudios.data.util.*;
import net.staticstudios.data.util.TaskQueue;
import net.staticstudios.data.utils.Link;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Blocking;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

@ApiStatus.Internal
public class DataManager {
    private static Boolean useGlobal = null;
    private static DataManager instance;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final String applicationName;
    private final DataAccessor dataAccessor;
    private final SQLBuilder sqlBuilder;
    private final TaskQueue taskQueue;
    private final Map<String, UniqueDataMetadata> uniqueDataMetadataMap = new ConcurrentHashMap<>();
    private final Map<String, Map<ColumnValuePairs, UniqueData>> uniqueDataInstanceCache = new ConcurrentHashMap<>();
    private final Map<String, Map<String, List<ValueUpdateHandlerWrapper<?, ?>>>> persistentValueUpdateHandlers = new ConcurrentHashMap<>();
    private final Map<String, Map<String, List<CachedValueUpdateHandlerWrapper<?, ?>>>> cachedValueUpdateHandlers = new ConcurrentHashMap<>();
    private final Map<String, Map<String, List<CollectionChangeHandlerWrapper<?, ?>>>> collectionChangeHandlers = new ConcurrentHashMap<>();
    private final Map<String, Map<String, List<ReferenceUpdateHandlerWrapper<?, ?>>>> referenceUpdateHandlers = new ConcurrentHashMap<>();
    private final PostgresListener postgresListener;
    private final RedisListener redisListener;
    private final Set<PersistentValueMetadata> registeredUpdateHandlersForColumns = ConcurrentHashMap.newKeySet();
    private final Set<CachedValueMetadata> registeredUpdateHandlersForRedis = ConcurrentHashMap.newKeySet();
    private final Set<PersistentCollectionMetadata> registeredChangeHandlersForCollection = ConcurrentHashMap.newKeySet();
    private final Set<ReferenceMetadata> registeredUpdateHandlersForReference = ConcurrentHashMap.newKeySet();

    private final List<ValueSerializer<?, ?>> valueSerializers = new CopyOnWriteArrayList<>();
    private final Consumer<Runnable> updateHandlerExecutor;

    private boolean finishedLoading = false;
    //todo: custom types are serialized and deserialized all the time currently, we should have a cache for these. caffeine with time based eviction sounds good.

    public DataManager(StaticDataConfig config, boolean setGlobal) {
        DataSourceConfig dataSourceConfig = new DataSourceConfig(
                config.postgresHost(),
                config.postgresPort(),
                config.postgresDatabase(),
                config.postgresUsername(),
                config.postgresPassword(),
                config.redisHost(),
                config.redisPort()
        );
        this.updateHandlerExecutor = config.updateHandlerExecutor();

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
        this.taskQueue = new TaskQueue(dataSourceConfig, applicationName);
        redisListener = new RedisListener(dataSourceConfig, this.taskQueue);
        sqlBuilder = new SQLBuilder(this);
        dataAccessor = new H2DataAccessor(this, postgresListener, redisListener, taskQueue);

        //todo: when we reconnect to postgres, refresh the internal cache from the source
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

    public void addUpdateHandler(String schema, String table, String column, ValueUpdateHandlerWrapper<?, ?> handler) {
        String key = schema + "." + table + "." + column;
        persistentValueUpdateHandlers.computeIfAbsent(key, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(handler.getHolderClass().getName(), k -> new CopyOnWriteArrayList<>())
                .add(handler);
    }

    private void addRedisUpdateHandler(String partialKey, CachedValueUpdateHandlerWrapper<?, ?> handler) {
        cachedValueUpdateHandlers.computeIfAbsent(partialKey, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(handler.getHolderClass().getName(), k -> new CopyOnWriteArrayList<>())
                .add(handler);
    }

    public void callPersistentValueUpdateHandlers(List<String> columnNames, String schema, String table, String column, Object[] oldSerializedValues, Object[] newSerializedValues) {
        logger.trace("Calling update handlers for {}.{}.{} with old values {} and new values {}", schema, table, column, Arrays.toString(oldSerializedValues), Arrays.toString(newSerializedValues));
        Map<String, List<ValueUpdateHandlerWrapper<?, ?>>> handlersForColumn = persistentValueUpdateHandlers.get(schema + "." + table + "." + column);
        if (handlersForColumn == null) {
            return;
        }

        int columnIndex = columnNames.indexOf(column);
        Preconditions.checkArgument(columnIndex != -1, "Column %s not found in provided name names %s", column, columnNames);

        for (Map.Entry<String, List<ValueUpdateHandlerWrapper<?, ?>>> entry : handlersForColumn.entrySet()) {
            String holderClassName = entry.getKey();
            Class<? extends UniqueData> holderClass = ClassUtils.forName(holderClassName);
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
                submitUpdateHandler(() -> wrapper.unsafeHandle(instance, deserializedOldValue, deserializedNewValue));
            }
        }
    }

    public void callCachedValueUpdateHandlers(String partialKey, List<String> encodedIdNames, List<String> encodedIdValues, @Nullable String oldValue, @Nullable String newValue) {
        if (Objects.equals(oldValue, newValue)) {
            return;
        }
        logger.trace("Calling Redis update handlers for {} with old value {} and new value {}", partialKey, oldValue, newValue);
        Map<String, List<CachedValueUpdateHandlerWrapper<?, ?>>> handlersForKey = cachedValueUpdateHandlers.get(partialKey);
        if (handlersForKey == null) {
            return;
        }
        for (Map.Entry<String, List<CachedValueUpdateHandlerWrapper<?, ?>>> entry : handlersForKey.entrySet()) {
            String holderClassName = entry.getKey();
            Class<? extends UniqueData> holderClass = ClassUtils.forName(holderClassName);
            UniqueDataMetadata metadata = getMetadata(holderClass);
            List<CachedValueUpdateHandlerWrapper<?, ?>> handlers = entry.getValue();
            ColumnValuePair[] idColumns = new ColumnValuePair[encodedIdValues.size()];
            for (int i = 0; i < encodedIdNames.size(); i++) {
                Class<?> valueType = null;
                for (ColumnMetadata idColumn : metadata.idColumns()) {
                    if (idColumn.name().equals(encodedIdNames.get(i))) {
                        valueType = idColumn.type();
                        break;
                    }
                }
                Preconditions.checkNotNull(valueType, "Could not find ID column %s for UniqueData class %s", encodedIdNames.get(i), holderClass.getName());
                Object decodedValue = Primitives.decode(getSerializedType(valueType), encodedIdValues.get(i));
                Object deserializedValue = deserialize(valueType, decodedValue);
                idColumns[i] = new ColumnValuePair(encodedIdNames.get(i), deserializedValue);
            }
            UniqueData instance = getInstance(holderClass, idColumns);
            for (CachedValueUpdateHandlerWrapper<?, ?> wrapper : handlers) {
                Class<?> serializedType = getSerializedType(wrapper.getDataType());
                Object deserializedOldValue;
                Object deserializedNewValue;

                if (oldValue == null) {
                    deserializedOldValue = wrapper.getFallback();
                } else {
                    Object decodedOldValue = Primitives.decode(serializedType, oldValue);
                    deserializedOldValue = deserialize(wrapper.getDataType(), decodedOldValue);
                }

                if (newValue == null) {
                    deserializedNewValue = wrapper.getFallback();
                } else {
                    Object decodedNewValue = Primitives.decode(serializedType, newValue);
                    deserializedNewValue = deserialize(wrapper.getDataType(), decodedNewValue);
                }
                submitUpdateHandler(() -> wrapper.unsafeHandle(instance, deserializedOldValue, deserializedNewValue));
            }
        }
    }

    /**
     * Called when an entry is deleted from the database, but a remove handler will want this snapshot of the data later, when update handlers are called.
     */
    public @Nullable UniqueData createSnapshotForCollectionRemoveHandlers(List<String> columnNames, String schema, String table, Object[] oldSerializedValues) {
        Map<String, List<CollectionChangeHandlerWrapper<?, ?>>> handlersForTable = collectionChangeHandlers.get(schema + "." + table);
        if (handlersForTable == null) {
            return null;
        }

        for (Map.Entry<String, List<CollectionChangeHandlerWrapper<?, ?>>> entry : handlersForTable.entrySet()) {
            List<CollectionChangeHandlerWrapper<?, ?>> handlers = entry.getValue();
            for (CollectionChangeHandlerWrapper<?, ?> wrapper : handlers) {
                PersistentCollectionMetadata collectionMetadata = wrapper.getCollectionMetadata();
                Preconditions.checkNotNull(collectionMetadata, "Collection metadata not set for collection change handler");
                if (wrapper.getType() != CollectionChangeHandlerWrapper.Type.REMOVE) {
                    continue;
                }

                UniqueDataMetadata referencedMetadata;

                if (collectionMetadata instanceof PersistentOneToManyCollectionMetadata oneToManyCollectionMetadata) {
                    referencedMetadata = getMetadata(oneToManyCollectionMetadata.getReferencedType());
                } else if (collectionMetadata instanceof PersistentManyToManyCollectionMetadata manyToManyCollectionMetadata) {
                    referencedMetadata = getMetadata(manyToManyCollectionMetadata.getReferencedType());

                    if (!Objects.equals(schema, referencedMetadata.schema()) || !Objects.equals(table, referencedMetadata.table())) {
                        continue; //don't need a snapshot if it wasn't the referenced object being deleted
                    }
                } else {
                    continue;
                }

                ColumnValuePair[] idColumns = new ColumnValuePair[referencedMetadata.idColumns().size()];
                for (ColumnMetadata idColumn : referencedMetadata.idColumns()) {
                    Object serializedValue = oldSerializedValues[columnNames.indexOf(idColumn.name())];
                    Object deserializedValue = deserialize(idColumn.type(), serializedValue);
                    idColumns[referencedMetadata.idColumns().indexOf(idColumn)] = new ColumnValuePair(idColumn.name(), deserializedValue);
                }

                return createSnapshot(referencedMetadata.clazz(), new ColumnValuePairs(idColumns));
            }
        }

        return null;
    }

    public void callCollectionChangeHandlers(List<String> columnNames, String schema, String table, List<String> changedColumns, Object[] oldSerializedValues, Object[] newSerializedValues, TriggerCause cause, @Nullable UniqueData snapshot) {
        logger.trace("Calling collection change handlers for {}.{} on changed columns {} with old values {} and new values {}", schema, table, changedColumns, Arrays.toString(oldSerializedValues), Arrays.toString(newSerializedValues));
        Map<String, List<CollectionChangeHandlerWrapper<?, ?>>> handlersForTable = collectionChangeHandlers.get(schema + "." + table);
        if (handlersForTable == null) {
            return;
        }

        for (Map.Entry<String, List<CollectionChangeHandlerWrapper<?, ?>>> entry : handlersForTable.entrySet()) {
            List<CollectionChangeHandlerWrapper<?, ?>> handlers = entry.getValue();
            for (CollectionChangeHandlerWrapper<?, ?> wrapper : handlers) {
                PersistentCollectionMetadata collectionMetadata = wrapper.getCollectionMetadata();
                Preconditions.checkNotNull(collectionMetadata, "Collection metadata not set for collection change handler");
                switch (collectionMetadata) {
                    case PersistentOneToManyCollectionMetadata oneToManyCollectionMetadata ->
                            handleOneToManyCollectionChange(wrapper, oneToManyCollectionMetadata, columnNames, oldSerializedValues, newSerializedValues, cause, snapshot);
                    case PersistentOneToManyValueCollectionMetadata oneToManyValueCollectionMetadata ->
                            handleOneToManyValuedCollectionChange(wrapper, oneToManyValueCollectionMetadata, columnNames, oldSerializedValues, newSerializedValues);
                    case PersistentManyToManyCollectionMetadata manyToManyCollectionMetadata ->
                            handleManyToManyCollectionChange(wrapper, manyToManyCollectionMetadata, schema, table, columnNames, oldSerializedValues, newSerializedValues, cause, snapshot);
                    default ->
                            throw new IllegalStateException("Unknown collection metadata type: " + collectionMetadata.getClass().getName());
                }
            }
        }
    }

    private void handleOneToManyCollectionChange(CollectionChangeHandlerWrapper<?, ?> handler, PersistentOneToManyCollectionMetadata metadata, List<String> columnNames, Object[] oldSerializedValues, Object[] newSerializedValues, TriggerCause cause, @Nullable UniqueData snapshot) {
        List<Link> links = metadata.getLinks();
        Object[] oldLinkValues = new Object[links.size()];
        Object[] newLinkValues = new Object[links.size()];
        boolean differenceFound = false;
        for (int i = 0; i < links.size(); i++) {
            Link link = links.get(i);
            int columnIndex = columnNames.indexOf(link.columnInReferencedTable());
            Preconditions.checkArgument(columnIndex != -1, "Column %s not found in provided name names %s", link.columnInReferencedTable(), columnNames);
            oldLinkValues[i] = oldSerializedValues[columnIndex];
            newLinkValues[i] = newSerializedValues[columnIndex];
            if (!Objects.equals(oldLinkValues[i], newLinkValues[i])) {
                differenceFound = true;
            }
        }

        if (!differenceFound) {
            return;
        }

        UniqueDataMetadata referencedMetadata = getMetadata(metadata.getReferencedType());
        List<Object> newValues = new ArrayList<>();
        for (ColumnMetadata idColumn : referencedMetadata.idColumns()) {
            int columnIndex = columnNames.indexOf(idColumn.name());
            Object newDeserializedValue = deserialize(idColumn.type(), newSerializedValues[columnIndex]);
            newValues.add(newDeserializedValue);
        }

        UniqueData instance = getInstanceForCollectionChangeHandler(metadata.getHolderClass(), links, columnNames, oldSerializedValues, newSerializedValues, handler.getType() == CollectionChangeHandlerWrapper.Type.ADD);
        if (instance == null) {
            return;
        }
        ColumnValuePair[] idColumns = new ColumnValuePair[referencedMetadata.idColumns().size()];

        if (handler.getType() == CollectionChangeHandlerWrapper.Type.REMOVE) {
            UniqueData oldInstance;
            if (cause == TriggerCause.DELETE) {
                //the handler probably cares about the data that was removed, so we create a snapshot since the actual data is no longer available
                oldInstance = snapshot;
            } else {
                for (ColumnMetadata idColumn : referencedMetadata.idColumns()) {
                    Object serializedValue = newValues.get(columnNames.indexOf(idColumn.name()));
                    Object deserializedValue = deserialize(idColumn.type(), serializedValue);
                    idColumns[referencedMetadata.idColumns().indexOf(idColumn)] = new ColumnValuePair(idColumn.name(), deserializedValue);
                }

                oldInstance = getInstance(referencedMetadata.clazz(), idColumns);
            }
            if (oldInstance != null) {
                submitUpdateHandler(() -> handler.unsafeHandle(instance, oldInstance));
            }
        }

        if (handler.getType() == CollectionChangeHandlerWrapper.Type.ADD) {
            for (ColumnMetadata idColumn : referencedMetadata.idColumns()) {
                Object serializedValue = newValues.get(columnNames.indexOf(idColumn.name()));
                Object deserializedValue = deserialize(idColumn.type(), serializedValue);
                idColumns[referencedMetadata.idColumns().indexOf(idColumn)] = new ColumnValuePair(idColumn.name(), deserializedValue);
            }
            UniqueData newInstance = getInstance(referencedMetadata.clazz(), idColumns);
            if (newInstance != null) {
                submitUpdateHandler(() -> handler.unsafeHandle(instance, newInstance));
            }
        }
    }

    private void handleOneToManyValuedCollectionChange(CollectionChangeHandlerWrapper<?, ?> handler, PersistentOneToManyValueCollectionMetadata metadata, List<String> columnNames, Object[] oldSerializedValues, Object[] newSerializedValues) {
        List<Link> links = metadata.getLinks();
        Object[] oldLinkValues = new Object[links.size()];
        Object[] newLinkValues = new Object[links.size()];
        boolean differenceFound = false;
        for (int i = 0; i < links.size(); i++) {
            Link link = links.get(i);
            int columnIndex = columnNames.indexOf(link.columnInReferencedTable());
            Preconditions.checkArgument(columnIndex != -1, "Column %s not found in provided name names %s", link.columnInReferencedTable(), columnNames);
            oldLinkValues[i] = oldSerializedValues[columnIndex];
            newLinkValues[i] = newSerializedValues[columnIndex];
            if (!Objects.equals(oldLinkValues[i], newLinkValues[i])) {
                differenceFound = true;
            }
        }

        if (!differenceFound) {
            return;
        }

        UniqueData instance = getInstanceForCollectionChangeHandler(metadata.getHolderClass(), links, columnNames, oldSerializedValues, newSerializedValues, handler.getType() == CollectionChangeHandlerWrapper.Type.ADD);

        if (instance == null) {
            return;
        }

        int columnIndex = columnNames.indexOf(metadata.getDataColumn());
        if (handler.getType() == CollectionChangeHandlerWrapper.Type.REMOVE) {
            Object oldSerializedValue = oldSerializedValues[columnIndex];
            Object deserializedOldValue = deserialize(metadata.getDataType(), oldSerializedValue);
            submitUpdateHandler(() -> handler.unsafeHandle(instance, deserializedOldValue));
        }

        if (handler.getType() == CollectionChangeHandlerWrapper.Type.ADD) {
            Object newSerializedValue = newSerializedValues[columnIndex];
            Object deserializedNewValue = deserialize(metadata.getDataType(), newSerializedValue);
            submitUpdateHandler(() -> handler.unsafeHandle(instance, deserializedNewValue));
        }
    }

    private void handleManyToManyCollectionChange(CollectionChangeHandlerWrapper<?, ?> handler, PersistentManyToManyCollectionMetadata metadata, String schema, String table, List<String> columnNames, Object[] oldSerializedValues, Object[] newSerializedValues, TriggerCause cause, @Nullable UniqueData snapshot) {
        List<Link> links = metadata.getJoinTableToReferencedTableLinks(this);
        Object[] oldLinkValues = new Object[links.size()];
        Object[] newLinkValues = new Object[links.size()];
        boolean differenceFound = false;
        for (int i = 0; i < links.size(); i++) {
            Link link = links.get(i);
            int columnIndex = columnNames.indexOf(link.columnInReferringTable());
            Preconditions.checkArgument(columnIndex != -1, "Column %s not found in provided name names %s", link.columnInReferringTable(), columnNames);
            oldLinkValues[i] = oldSerializedValues[columnIndex];
            newLinkValues[i] = newSerializedValues[columnIndex];
            if (!Objects.equals(oldLinkValues[i], newLinkValues[i])) {
                differenceFound = true;
            }
        }

        if (!differenceFound) {
            return;
        }

        UniqueDataMetadata referencedMetadata = getMetadata(metadata.getReferencedType());
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT ");
        for (ColumnMetadata idColumn : referencedMetadata.idColumns()) {
            sqlBuilder.append("\"").append(idColumn.name()).append("\", ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 2);
        sqlBuilder.append(" FROM \"").append(referencedMetadata.schema()).append("\".\"").append(referencedMetadata.table()).append("\" WHERE ");
        List<Object> oldValues = new ArrayList<>();
        List<Object> newValues = new ArrayList<>();
        SQLTable referencedTable = Objects.requireNonNull(this.sqlBuilder.getSchema(referencedMetadata.schema())).getTable(referencedMetadata.table());
        Preconditions.checkNotNull(referencedTable, "Referenced table %s.%s not found", referencedMetadata.schema(), referencedMetadata.table());
        for (Link link : metadata.getJoinTableToReferencedTableLinks(this)) {
            if (!oldValues.isEmpty()) {
                sqlBuilder.append(" AND ");
            }
            String columnInJoinTable = link.columnInReferringTable();
            String columnInReferencedTable = link.columnInReferencedTable();
            sqlBuilder.append("\"").append(columnInReferencedTable).append("\" = ? ");
            Class<?> columnType = null;
            for (SQLColumn column : referencedTable.getColumns()) {
                if (column.getName().equals(columnInReferencedTable)) {
                    columnType = column.getType();
                    break;
                }
            }
            Preconditions.checkNotNull(columnType, "Could not find column %s in referenced table %s.%s", columnInReferencedTable, referencedMetadata.schema(), referencedMetadata.table());
            int columnIndex = columnNames.indexOf(columnInJoinTable);
            Object oldDeserializedValue = deserialize(columnType, oldSerializedValues[columnIndex]);
            oldValues.add(oldDeserializedValue);
            Object newDeserializedValue = deserialize(columnType, newSerializedValues[columnIndex]);
            newValues.add(newDeserializedValue);
        }

        UniqueData instance = getInstanceForCollectionChangeHandler(metadata.getHolderClass(),
                metadata.getJoinTableToDataTableLinks(this).stream().map(link -> new Link(link.columnInReferringTable(), link.columnInReferencedTable())).toList(), //reverse since the method expects the referenced table to be the join table
                columnNames, oldSerializedValues, newSerializedValues, handler.getType() == CollectionChangeHandlerWrapper.Type.ADD);
        if (instance == null) {
            return;
        }

        ColumnValuePair[] idColumns = new ColumnValuePair[referencedMetadata.idColumns().size()];
        if (handler.getType() == CollectionChangeHandlerWrapper.Type.REMOVE) {
            UniqueData oldInstance = null;
            if (cause == TriggerCause.DELETE && Objects.equals(schema, referencedMetadata.schema()) && Objects.equals(table, referencedMetadata.table())) {
                //the handler probably cares about the data that was removed, so we create a snapshot since the actual data is no longer available
                oldInstance = snapshot;
            } else {
                try (ResultSet rs = dataAccessor.executeQuery(sqlBuilder.toString(), oldValues)) {
                    if (rs.next()) {
                        for (ColumnMetadata idColumn : referencedMetadata.idColumns()) {
                            Object serializedValue = rs.getObject(idColumn.name());
                            Object deserializedValue = deserialize(idColumn.type(), serializedValue);
                            idColumns[referencedMetadata.idColumns().indexOf(idColumn)] = new ColumnValuePair(idColumn.name(), deserializedValue);
                        }
                        oldInstance = getInstance(referencedMetadata.clazz(), idColumns);
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            if (oldInstance != null) {
                UniqueData finalOldInstance = oldInstance;
                submitUpdateHandler(() -> handler.unsafeHandle(instance, finalOldInstance));
            }
        }

        if (handler.getType() == CollectionChangeHandlerWrapper.Type.ADD) {
            try (ResultSet rs = dataAccessor.executeQuery(sqlBuilder.toString(), newValues)) {
                if (rs.next()) {
                    for (ColumnMetadata idColumn : referencedMetadata.idColumns()) {
                        Object serializedValue = rs.getObject(idColumn.name());
                        Object deserializedValue = deserialize(idColumn.type(), serializedValue);
                        idColumns[referencedMetadata.idColumns().indexOf(idColumn)] = new ColumnValuePair(idColumn.name(), deserializedValue);
                    }
                    UniqueData newInstance = getInstance(referencedMetadata.clazz(), idColumns);
                    if (newInstance != null) {
                        submitUpdateHandler(() -> handler.unsafeHandle(instance, newInstance));
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private UniqueData getInstanceForCollectionChangeHandler(Class<? extends UniqueData> holderClass, List<Link> links, List<String> columnNames, Object[] oldSerializedValues, Object[] newSerializedValues, boolean useNewValues) {
        UniqueData instance = null;
        UniqueDataMetadata uniqueDataMetadata = getMetadata(holderClass);
        SQLTable uniqueDataTable = Objects.requireNonNull(this.sqlBuilder.getSchema(uniqueDataMetadata.schema())).getTable(uniqueDataMetadata.table());
        Preconditions.checkNotNull(uniqueDataTable, "Table %s.%s not found", uniqueDataMetadata.schema(), uniqueDataMetadata.table());

        StringBuilder instanceSqlBuilder = new StringBuilder();
        instanceSqlBuilder.append("SELECT ");
        for (ColumnMetadata idColumn : uniqueDataMetadata.idColumns()) {
            instanceSqlBuilder.append("\"").append(idColumn.name()).append("\", ");
        }
        instanceSqlBuilder.setLength(instanceSqlBuilder.length() - 2);
        instanceSqlBuilder.append(" FROM \"").append(uniqueDataMetadata.schema()).append("\".\"").append(uniqueDataMetadata.table()).append("\" WHERE ");
        List<Object> instanceValues = new ArrayList<>();
        for (Link link : links) {
            int columnIndex = columnNames.indexOf(link.columnInReferencedTable());
            Preconditions.checkArgument(columnIndex != -1, "Column %s not found in provided name names %s", link.columnInReferencedTable(), columnNames);
            if (!instanceValues.isEmpty()) {
                instanceSqlBuilder.append(" AND ");
            }
            instanceSqlBuilder.append("\"").append(link.columnInReferringTable()).append("\" = ? ");
            Class<?> valueType = null;
            for (SQLColumn column : uniqueDataTable.getColumns()) {
                if (column.getName().equals(link.columnInReferringTable())) {
                    valueType = column.getType();
                    break;
                }
            }
            Preconditions.checkNotNull(valueType, "Could not find column %s in holder UniqueData class %s", link.columnInReferringTable(), uniqueDataMetadata.clazz().getName());
            Object deserializedValue = useNewValues
                    ? deserialize(valueType, newSerializedValues[columnNames.indexOf(link.columnInReferencedTable())])
                    : deserialize(valueType, oldSerializedValues[columnNames.indexOf(link.columnInReferencedTable())]);
            instanceValues.add(deserializedValue);
        }
        try (ResultSet rs = dataAccessor.executeQuery(instanceSqlBuilder.toString(), instanceValues)) {
            if (rs.next()) {
                ColumnValuePair[] idColumns = new ColumnValuePair[uniqueDataMetadata.idColumns().size()];
                for (ColumnMetadata idColumn : uniqueDataMetadata.idColumns()) {
                    Object serializedValue = rs.getObject(idColumn.name());
                    Object deserializedValue = deserialize(idColumn.type(), serializedValue);
                    idColumns[uniqueDataMetadata.idColumns().indexOf(idColumn)] = new ColumnValuePair(idColumn.name(), deserializedValue);
                }
                instance = getInstance(uniqueDataMetadata.clazz(), idColumns);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return instance;
    }

    private UniqueData getInstanceForReferenceUpdateHandler(Class<? extends UniqueData> holderClass, List<String> columnNames, Object[] newSerializedValues) {
        UniqueDataMetadata uniqueDataMetadata = getMetadata(holderClass);
        ColumnValuePair[] idColumns = new ColumnValuePair[uniqueDataMetadata.idColumns().size()];
        for (ColumnMetadata idColumn : uniqueDataMetadata.idColumns()) {
            int columnIndex = columnNames.indexOf(idColumn.name());
            Object serializedValue = newSerializedValues[columnIndex];
            Object deserializedValue = deserialize(idColumn.type(), serializedValue);
            idColumns[uniqueDataMetadata.idColumns().indexOf(idColumn)] = new ColumnValuePair(idColumn.name(), deserializedValue);
        }
        return getInstance(uniqueDataMetadata.clazz(), idColumns);
    }

    public void callReferenceUpdateHandlers(List<String> columnNames, String schema, String table, List<String> changedColumns, Object[] oldSerializedValues, Object[] newSerializedValues) {
        logger.trace("Calling reference update handlers for {}.{} on changed columns {} with old values {} and new values {}", schema, table, changedColumns, Arrays.toString(oldSerializedValues), Arrays.toString(newSerializedValues));
        Map<String, List<ReferenceUpdateHandlerWrapper<?, ?>>> handlersForTable = referenceUpdateHandlers.get(schema + "." + table);
        if (handlersForTable == null) {
            return;
        }

        for (Map.Entry<String, List<ReferenceUpdateHandlerWrapper<?, ?>>> entry : handlersForTable.entrySet()) {
            List<ReferenceUpdateHandlerWrapper<?, ?>> handlers = entry.getValue();
            for (ReferenceUpdateHandlerWrapper<?, ?> wrapper : handlers) {
                ReferenceMetadata metadata = wrapper.getReferenceMetadata();
                List<Link> links = metadata.links();
                Object[] oldLinkValues = new Object[links.size()];
                Object[] newLinkValues = new Object[links.size()];
                boolean differenceFound = false;
                for (int i = 0; i < links.size(); i++) {
                    Link link = links.get(i);
                    int columnIndex = columnNames.indexOf(link.columnInReferringTable());
                    Preconditions.checkArgument(columnIndex != -1, "Column %s not found in provided name names %s", link.columnInReferringTable(), columnNames);
                    oldLinkValues[i] = oldSerializedValues[columnIndex];
                    newLinkValues[i] = newSerializedValues[columnIndex];
                    if (!Objects.equals(oldLinkValues[i], newLinkValues[i])) {
                        differenceFound = true;
                    }
                }

                if (!differenceFound) {
                    continue;
                }

                UniqueDataMetadata referencedMetadata = getMetadata(metadata.referencedClass());
                SQLTable referencedTable = Objects.requireNonNull(this.sqlBuilder.getSchema(referencedMetadata.schema())).getTable(referencedMetadata.table());
                Preconditions.checkNotNull(referencedTable, "Referenced table %s.%s not found", referencedMetadata.schema(), referencedMetadata.table());

                StringBuilder sqlBuilder = new StringBuilder();
                sqlBuilder.append("SELECT ");
                for (ColumnMetadata idColumn : referencedMetadata.idColumns()) {
                    sqlBuilder.append("\"").append(idColumn.name()).append("\", ");
                }
                sqlBuilder.setLength(sqlBuilder.length() - 2);
                sqlBuilder.append(" FROM \"").append(referencedMetadata.schema()).append("\".\"").append(referencedMetadata.table()).append("\" WHERE ");
                List<Object> oldValues = new ArrayList<>();
                List<Object> newValues = new ArrayList<>();
                for (Link link : metadata.links()) {
                    if (!oldValues.isEmpty()) {
                        sqlBuilder.append(" AND ");
                    }
                    sqlBuilder.append("\"").append(link.columnInReferencedTable()).append("\" = ? ");
                    Class<?> columnType = null;
                    for (SQLColumn column : referencedTable.getColumns()) {
                        if (column.getName().equals(link.columnInReferencedTable())) {
                            columnType = column.getType();
                            break;
                        }
                    }
                    Preconditions.checkNotNull(columnType, "Could not find column %s in referenced table %s.%s", link.columnInReferencedTable(), referencedMetadata.schema(), referencedMetadata.table());
                    int columnIndex = columnNames.indexOf(link.columnInReferringTable());
                    Object oldDeserializedValue = deserialize(columnType, oldSerializedValues[columnIndex]);
                    oldValues.add(oldDeserializedValue);
                    Object newDeserializedValue = deserialize(columnType, newSerializedValues[columnIndex]);
                    newValues.add(newDeserializedValue);
                }

                UniqueData instance = getInstanceForReferenceUpdateHandler(metadata.holderClass(), columnNames, newSerializedValues);
                if (instance == null) {
                    continue;
                }

                UniqueData oldInstance = null;
                UniqueData newInstance = null;
                try (ResultSet rs = dataAccessor.executeQuery(sqlBuilder.toString(), oldValues)) {
                    if (rs.next()) {
                        ColumnValuePair[] idColumns = new ColumnValuePair[referencedMetadata.idColumns().size()];
                        for (ColumnMetadata idColumn : referencedMetadata.idColumns()) {
                            Object serializedValue = rs.getObject(idColumn.name());
                            Object deserializedValue = deserialize(idColumn.type(), serializedValue);
                            idColumns[referencedMetadata.idColumns().indexOf(idColumn)] = new ColumnValuePair(idColumn.name(), deserializedValue);
                        }
                        oldInstance = getInstance(referencedMetadata.clazz(), idColumns);
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }

                try (ResultSet rs = dataAccessor.executeQuery(sqlBuilder.toString(), newValues)) {
                    if (rs.next()) {
                        ColumnValuePair[] idColumns = new ColumnValuePair[referencedMetadata.idColumns().size()];
                        for (ColumnMetadata idColumn : referencedMetadata.idColumns()) {
                            Object serializedValue = rs.getObject(idColumn.name());
                            Object deserializedValue = deserialize(idColumn.type(), serializedValue);
                            idColumns[referencedMetadata.idColumns().indexOf(idColumn)] = new ColumnValuePair(idColumn.name(), deserializedValue);
                        }
                        newInstance = getInstance(referencedMetadata.clazz(), idColumns);
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }

                UniqueData finalOldInstance = oldInstance;
                UniqueData finalNewInstance = newInstance;
                submitUpdateHandler(() -> wrapper.unsafeHandle(instance, finalOldInstance, finalNewInstance));
            }
        }
    }

    private void submitUpdateHandler(Runnable runnable) {
        updateHandlerExecutor.accept(runnable);
    }

    public void registerPersistentValueUpdateHandlers(PersistentValueMetadata metadata, Collection<ValueUpdateHandlerWrapper<?, ?>> handlers) {
        if (registeredUpdateHandlersForColumns.add(metadata)) {
            for (ValueUpdateHandlerWrapper<?, ?> handler : handlers) {
                addUpdateHandler(metadata.getSchema(), metadata.getTable(), metadata.getColumn(), handler);
            }
        }
    }

    public void registerCachedValueUpdateHandlers(CachedValueMetadata metadata, Collection<CachedValueUpdateHandlerWrapper<?, ?>> handlers) {
        if (registeredUpdateHandlersForRedis.add(metadata)) {
            UniqueDataMetadata holderMetadata = getMetadata(metadata.holderClass());
            String partialKey = RedisUtils.buildPartialRedisKey(metadata.holderSchema(), metadata.holderTable(), metadata.identifier(), holderMetadata.idColumns());
            for (CachedValueUpdateHandlerWrapper<?, ?> handler : handlers) {
                addRedisUpdateHandler(partialKey, handler);
            }
        }
    }

    public void registerCollectionChangeHandlers(PersistentCollectionMetadata metadata, Collection<CollectionChangeHandlerWrapper<?, ?>> handlers) {
        if (registeredChangeHandlersForCollection.add(metadata)) {
            handlers.forEach(h -> h.setCollectionMetadata(metadata));
            String key = switch (metadata) {
                case PersistentOneToManyCollectionMetadata oneToManyCollectionMetadata -> {
                    UniqueDataMetadata referencedMetadata = getMetadata(oneToManyCollectionMetadata.getReferencedType());
                    yield referencedMetadata.schema() + "." + referencedMetadata.table();
                }
                case PersistentOneToManyValueCollectionMetadata oneToManyValueCollectionMetadata ->
                        oneToManyValueCollectionMetadata.getDataSchema() + "." + oneToManyValueCollectionMetadata.getDataTable();
                case PersistentManyToManyCollectionMetadata manyToManyCollectionMetadata ->
                        manyToManyCollectionMetadata.getJoinTableSchema(this) + "." + manyToManyCollectionMetadata.getJoinTableName(this);
                default ->
                        throw new IllegalStateException("Unknown PersistentCollectionMetadata type: " + metadata.getClass().getName());
            };
            collectionChangeHandlers.computeIfAbsent(key, k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(metadata.getHolderClass().getName(), k -> new CopyOnWriteArrayList<>())
                    .addAll(handlers);
        }
    }

    public void registerReferenceUpdateHandlers(ReferenceMetadata metadata, Collection<ReferenceUpdateHandlerWrapper<?, ?>> handlers) {
        if (registeredUpdateHandlersForReference.add(metadata)) {
            handlers.forEach(h -> h.setReferenceMetadata(metadata));
            UniqueDataMetadata holderMetadata = getMetadata(metadata.holderClass());
            String key = holderMetadata.schema() + "." + holderMetadata.table();
            for (ReferenceUpdateHandlerWrapper<?, ?> handler : handlers) {
                referenceUpdateHandlers.computeIfAbsent(key, k -> new ConcurrentHashMap<>())
                        .computeIfAbsent(holderMetadata.clazz().getName(), k -> new CopyOnWriteArrayList<>())
                        .add(handler);
            }
        }
    }

    public List<ValueUpdateHandlerWrapper<?, ?>> getUpdateHandlers(String schema, String table, String column, Class<? extends UniqueData> holderClass) {
        String key = schema + "." + table + "." + column;
        if (persistentValueUpdateHandlers.containsKey(key) && persistentValueUpdateHandlers.get(key).containsKey(holderClass.getName())) {
            return persistentValueUpdateHandlers.get(key).get(holderClass.getName());
        }
        return Collections.emptyList();
    }

    public final void finishLoading() {
        Preconditions.checkState(!finishedLoading, "finishLoading() has already been called");

        finishedLoading = true;
        dataAccessor.resync();
    }

    @SafeVarargs
    public final void load(Class<? extends UniqueData>... classes) {
        Preconditions.checkState(!finishedLoading, "Cannot call load(...) after finishLoading() has been called");

        List<UniqueDataMetadata> extracted = new ArrayList<>();
        for (Class<? extends UniqueData> clazz : classes) {
            extracted.addAll(extractMetadata(clazz));
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

        List<String> partialRedisKeys = new ArrayList<>();
        for (UniqueDataMetadata metadata : extracted) {
            for (CachedValueMetadata cachedValueMetadata : metadata.cachedValueMetadata().values()) {
                partialRedisKeys.add(RedisUtils.buildPartialRedisKey(cachedValueMetadata.holderSchema(), cachedValueMetadata.holderTable(), cachedValueMetadata.identifier(), metadata.idColumns()));
            }
        }
        dataAccessor.discoverRedisKeys(partialRedisKeys);
    }

    public List<UniqueDataMetadata> extractMetadata(Class<? extends UniqueData> clazz) {
        Data dataAnnotation = clazz.getAnnotation(Data.class);
        List<UniqueDataMetadata> extracted = new ArrayList<>();
        extractMetadata(clazz, dataAnnotation, extracted);
        return extracted;
    }

    public void extractMetadata(Class<? extends UniqueData> clazz, Data fallbackDataAnnotation, List<UniqueDataMetadata> extracted) {
        logger.debug("Extracting metadata for UniqueData class {}", clazz.getName());
        Preconditions.checkArgument(!uniqueDataMetadataMap.containsKey(clazz.getName()), "UniqueData class %s has already been parsed", clazz.getName());
        Data dataAnnotation = clazz.getAnnotation(Data.class);
        if (dataAnnotation == null) {
            dataAnnotation = fallbackDataAnnotation;
        }
        Preconditions.checkNotNull(dataAnnotation, "UniqueData class %s is missing @Data annotation", clazz.getName());

        UniqueDataMetadata metadata = null;

        if (!Modifier.isAbstract(clazz.getModifiers())) {
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
            persistentCollectionMetadataMap.putAll(PersistentOneToManyCollectionImpl.extractMetadata(this, clazz));
            persistentCollectionMetadataMap.putAll(PersistentManyToManyCollectionImpl.extractMetadata(clazz));
            persistentCollectionMetadataMap.putAll(PersistentOneToManyValueCollectionImpl.extractMetadata(clazz, schema));
            metadata = new UniqueDataMetadata(
                    clazz,
                    schema,
                    table,
                    idColumns,
                    CachedValueImpl.extractMetadata(schema, table, clazz),
                    PersistentValueImpl.extractMetadata(schema, table, clazz),
                    ReferenceImpl.extractMetadata(clazz),
                    persistentCollectionMetadataMap
            );
            uniqueDataMetadataMap.put(clazz.getName(), metadata);
            extracted.add(metadata);
        }

        for (Field field : ReflectionUtils.getFields(clazz, Relation.class)) {
            Class<?> genericType = ReflectionUtils.getGenericType(field);
            if (genericType != null && !Modifier.isAbstract(genericType.getModifiers()) && UniqueData.class.isAssignableFrom(genericType)) {
                Class<? extends UniqueData> dependencyClass = genericType.asSubclass(UniqueData.class);
                if (!uniqueDataMetadataMap.containsKey(dependencyClass.getName())) {
                    extractMetadata(dependencyClass, null, extracted);
                }
            }
        }

        Class<?> superClass = clazz.getSuperclass();
        if (superClass != null && UniqueData.class.isAssignableFrom(superClass) && superClass != UniqueData.class) {
            Class<? extends UniqueData> superUniqueDataClass = superClass.asSubclass(UniqueData.class);
            if (!uniqueDataMetadataMap.containsKey(superUniqueDataClass.getName())) {
                extractMetadata(superUniqueDataClass, dataAnnotation, extracted);
            }
        }
    }

    public UniqueDataMetadata getMetadata(Class<? extends UniqueData> clazz) {
        UniqueDataMetadata metadata = uniqueDataMetadataMap.get(clazz.getName());
        Preconditions.checkNotNull(metadata, "UniqueData class %s has not been parsed yet", clazz.getName());
        return metadata;
    }

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

            UniqueData instance = uniqueDataInstanceCache.getOrDefault(uniqueDataMetadata.clazz().getName(), Collections.emptyMap()).get(new ColumnValuePairs(idColumns));
            if (instance == null) {
                return;
            }
            instance.markDeleted();
        });
    }

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
            Map<ColumnValuePairs, UniqueData> classCache = uniqueDataInstanceCache.get(uniqueDataMetadata.clazz().getName());
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

    public <T extends UniqueData> T getInstance(Class<T> clazz, ColumnValuePair... idColumnValues) {
        return getInstance(clazz, new ColumnValuePairs(idColumnValues));
    }

    @SuppressWarnings("unchecked")
    public <T extends UniqueData> T getInstance(Class<T> clazz, @NotNull ColumnValuePairs idColumns) {
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
            Preconditions.checkNotNull(providedIdColumn.value(), "ID column value for column %s in UniqueData class %s cannot be null", providedIdColumn.column(), clazz.getName());
        }

        Preconditions.checkArgument(hasAllIdColumns, "Not all @IdColumn columnsInReferringTable were provided for UniqueData class %s. Required: %s, Provided: %s", clazz.getName(), metadata.idColumns(), idColumns);

        T instance;
        if (uniqueDataInstanceCache.containsKey(clazz.getName()) && uniqueDataInstanceCache.get(clazz.getName()).containsKey(idColumns)) {
            logger.trace("Cache hit for UniqueData class {} with ID columnsInReferringTable {}", clazz.getName(), idColumns);
            instance = (T) uniqueDataInstanceCache.get(clazz.getName()).get(idColumns);
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

        instance.setDataManager(this, false);
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
        CachedValueImpl.delegate(instance);
        ReferenceImpl.delegate(instance);
        PersistentOneToManyCollectionImpl.delegate(instance);
        PersistentManyToManyCollectionImpl.delegate(instance);
        PersistentOneToManyValueCollectionImpl.delegate(instance);

        uniqueDataInstanceCache.computeIfAbsent(clazz.getName(), k -> new MapMaker().weakValues().makeMap())
                .put(idColumns, instance);

        logger.trace("Cache miss for UniqueData class {} with ID columnsInReferringTable {}. Created new instance.", clazz.getName(), idColumns);

        return instance;
    }

    /**
     * Creates a snapshot of the given UniqueData instance.
     * The snapshot instance will have the same ID columns as the original instance,
     * but it's values will be read-only and represent the state of the data at the time of snapshot creation.
     *
     * @param instance the UniqueData instance to create a snapshot of
     * @param <T>      the type of UniqueData
     * @return a snapshot UniqueData instance
     */
    @SuppressWarnings("unchecked")
    public <T extends UniqueData> T createSnapshot(T instance) {
        return createSnapshot((Class<T>) instance.getClass(), instance.getIdColumns());
    }

    private <T extends UniqueData> T createSnapshot(Class<T> clazz, ColumnValuePairs idColumns) {
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
            Preconditions.checkNotNull(providedIdColumn.value(), "ID column value for column %s in UniqueData class %s cannot be null", providedIdColumn.column(), clazz.getName());
        }

        Preconditions.checkArgument(hasAllIdColumns, "Not all @IdColumn columnsInReferringTable were provided for UniqueData class %s. Required: %s, Provided: %s", clazz.getName(), metadata.idColumns(), idColumns);
        T instance;
        try {
            Constructor<T> constructor = clazz.getDeclaredConstructor();
            constructor.setAccessible(true);
            instance = constructor.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        instance.setDataManager(this, true);
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

        ReadOnlyPersistentValue.delegate(instance);
        ReadOnlyCachedValue.delegate(instance);
        ReadOnlyReference.delegate(instance);
        ReadOnlyValuedCollection.delegate(instance);
        ReadOnlyReferenceCollection.delegate(instance);

        logger.trace("Created snapshot for UniqueData class {} with ID columnsInReferringTable {}", clazz.getName(), idColumns);

        return instance;
    }

    public BatchInsert createBatchInsert() {
        return new BatchInsert(this);
    }

    public void insert(InsertContext context, InsertMode insertMode) {
        BatchInsert batch = createBatchInsert();

        batch.add(context);
        insert(batch, insertMode);
    }

    public void insert(BatchInsert batch, InsertMode insertMode) {

        List<InsertStatement> insertStatements = new LinkedList<>();
        for (InsertContext context : batch.getInsertContexts()) {
            insertStatements.addAll(generateInsertStatements(context));
        }

        for (InsertStatement insertStatement : List.copyOf(insertStatements)) {
            insertStatement.calculateRequiredDependencies();
            insertStatement.satisfyDependencies(insertStatements);
        }

        InsertStatement.checkForCycles(insertStatements);

        insertStatements = InsertStatement.sort(insertStatements);

        List<SQlStatement> statements = new ArrayList<>();
        for (InsertStatement insertStatement : insertStatements) {
            statements.add(insertStatement.asStatement());
        }

        for (PostInsertAction action : batch.getPostInsertActions()) {
            statements.addAll(action.getStatements());
        }

        try {
            dataAccessor.insert(statements, insertMode);

            for (InsertContext context : batch.getInsertContexts()) {
                context.markInserted();
                context.runPostInsertActions();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private List<InsertStatement> generateInsertStatements(InsertContext insertContext) {
        List<InsertStatement> insertStatements = new LinkedList<>();

        Map<SQLTable, List<SimpleColumnMetadata>> tableColumnsMap = new HashMap<>();
        insertContext.getEntries().forEach((simpleColumnMetadata, o) -> {
            SQLTable table = Objects.requireNonNull(sqlBuilder.getSchema(simpleColumnMetadata.schema())).getTable(simpleColumnMetadata.table());
            tableColumnsMap.computeIfAbsent(table, k -> new LinkedList<>())
                    .add(simpleColumnMetadata);
        });

        for (SQLTable table : tableColumnsMap.keySet()) {
            List<ColumnValuePair> idColumns = new ArrayList<>();
            Map<SimpleColumnMetadata, Object> otherColumnValues = new HashMap<>();
            List<SimpleColumnMetadata> columns = tableColumnsMap.get(table);
            for (SimpleColumnMetadata column : columns) {
                Object value = insertContext.getEntries().get(column);

                if (table.getIdColumns().stream().anyMatch(c -> c.name().equals(column.name()))) {
                    idColumns.add(new ColumnValuePair(column.name(), value));
                } else {
                    otherColumnValues.put(column, value);
                }
            }
            InsertStatement statement = new InsertStatement(this, table, new ColumnValuePairs(idColumns.toArray(new ColumnValuePair[0])));

            otherColumnValues.forEach((column, value) -> {
                InsertStrategy strategy = insertContext.getInsertStrategy(column);
                statement.set(column.name(), strategy, value);
            });

            insertStatements.add(statement);
        }

        return insertStatements;
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

    public void set(String schema, String table, String column, ColumnValuePairs idColumns, List<Link> idColumnLinks, Object value, int delay) {
        StringBuilder sqlBuilder;

        @Language("SQL") String h2Sql;
        @Language("SQL") String pgSql;

        if (idColumnLinks.isEmpty()) {
            sqlBuilder = new StringBuilder().append("UPDATE \"").append(schema).append("\".\"").append(table).append("\" SET \"").append(column).append("\" = ? WHERE ");
            for (ColumnValuePair columnValuePair : idColumns) {
                String name = columnValuePair.column();
                sqlBuilder.append("\"").append(name).append("\" = ? AND ");
            }
            sqlBuilder.setLength(sqlBuilder.length() - 5);
            pgSql = h2Sql = sqlBuilder.toString();
        } else { // we're dealing with a foreign key
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

            h2Sql = sqlBuilder.toString();
            sqlBuilder.setLength(0);

            sqlBuilder.append("INSERT INTO \"").append(schema).append("\".\"").append(table).append("\" (\"").append(column).append("\"");
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
            sqlBuilder.append(") VALUES (?");
            for (ColumnValuePair columnValuePair : idColumns) {
                sqlBuilder.append(", ?");
            }
            sqlBuilder.append(") ON CONFLICT (");
            for (ColumnValuePair columnValuePair : idColumns) {
                String name = columnValuePair.column();
                for (Link link : idColumnLinks) {
                    if (link.columnInReferringTable().equals(columnValuePair.column())) {
                        name = link.columnInReferencedTable();
                        break;
                    }
                }
                sqlBuilder.append("\"").append(name).append("\", ");
            }
            sqlBuilder.setLength(sqlBuilder.length() - 2);
            sqlBuilder.append(") DO UPDATE SET \"").append(column).append("\" = EXCLUDED.\"").append(column).append("\"");
            pgSql = sqlBuilder.toString();
        }

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


    public <T> @Nullable T getRedis(String holderSchema, String holderTable, String identifier, ColumnValuePairs icColumns, Class<T> type) {
        String key = RedisUtils.buildRedisKey(holderSchema, holderTable, identifier, icColumns);
        String encoded = dataAccessor.getRedisValue(key);
        if (encoded == null) {
            return null;
        }
        Object serialized = Primitives.decode(getSerializedType(type), encoded);
        return deserialize(type, serialized);
    }

    public void setRedis(String holderSchema, String holderTable, String identifier, ColumnValuePairs icColumns, int expireAfterSeconds, @Nullable Object value) {
        String key = RedisUtils.buildRedisKey(holderSchema, holderTable, identifier, icColumns);
        Object serialized = serialize(value);
        String encoded = Primitives.encode(serialized);
        dataAccessor.setRedisValue(key, encoded, expireAfterSeconds);
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
        return null;
    }

    public <T> T deserialize(Class<T> clazz, Object serialized) {
        if (serialized == null || Primitives.isPrimitive(clazz)) {
            return (T) serialized;
        }

        ValueSerializer<?, ?> valueSerializer = getValueSerializer(clazz);
        if (valueSerializer != null) {
            return (T) deserialize(valueSerializer, serialized);
        }

        if (clazz.isEnum()) {
            return (T) Enum.valueOf((Class<Enum>) clazz, serialized.toString());
        }

        throw new IllegalStateException("No suitable deserializer found for type " + clazz.getName());
    }

    public <T> T serialize(Object deserialized) {
        if (deserialized == null || Primitives.isPrimitive(deserialized.getClass())) {
            return (T) deserialized;
        }

        ValueSerializer<?, ?> valueSerializer = getValueSerializer(deserialized.getClass());
        if (valueSerializer != null) {
            return (T) serialize(valueSerializer, deserialized);
        }

        if (deserialized instanceof Enum) {
            return (T) ((Enum<?>) deserialized).name();
        }

        throw new IllegalStateException("No suitable deserializer found for type " + deserialized.getClass().getName());

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
        if (serializer != null) {
            return serializer.getSerializedType();
        }

        if (clazz.isEnum()) {
            return String.class;
        }

        throw new IllegalStateException("No suitable serializer found for type " + clazz.getName());
    }

    public <T> T copy(T value, Class<T> dataType) {
        if (Primitives.isPrimitive(dataType)) {
            return Primitives.copy(value, dataType);
        }
        return deserialize(dataType, serialize(value));
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

    /**
     * Block the calling thread until all previously enqueued tasks have been completed
     */
    @Blocking
    public void flushTaskQueue() {
        //This will add a task to the queue and block until it's done
        taskQueue.submitTask(connection -> {
            //Ignore
        }).join();
    }
}
