package net.staticstudios.data;

import com.google.common.base.Preconditions;
import net.staticstudios.data.impl.sqlite.SQLiteDataAccessor;
import net.staticstudios.data.parse.Column;
import net.staticstudios.data.parse.Data;
import net.staticstudios.data.parse.SQLBuilder;
import net.staticstudios.data.parse.UniqueDataMetadata;
import net.staticstudios.data.util.*;
import net.staticstudios.data.util.TaskQueue;
import org.jetbrains.annotations.Blocking;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DataManager {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final DataAccessor dataAccessor;
    private final SQLBuilder sqlBuilder;
    private final TaskQueue taskQueue;
    private final ConcurrentHashMap<Class<? extends UniqueData>, UniqueDataMetadata> uniqueDataMetadataMap = new ConcurrentHashMap<>();

    public DataManager(DataSourceConfig dataSourceConfig) {
        String applicationName = "static_data_manager_v3-" + UUID.randomUUID();
        sqlBuilder = new SQLBuilder();
        dataAccessor = new SQLiteDataAccessor(sqlBuilder);
        this.taskQueue = new TaskQueue(dataSourceConfig, applicationName);

        //todo: when we parse UniqueData objects we should build an internal map, and then when we are done auto create the sql if the tables dont exist
        //todo: this will be extremely useful for building the internal cache tables
    }

    public DataAccessor getDataAccessor() {
        return dataAccessor;
    }

    public SQLBuilder getSQLBuilder() {
        return sqlBuilder;
    }

    @SafeVarargs
    public final void load(Class<? extends UniqueData>... classes) {
        for (Class<? extends UniqueData> clazz : classes) {
            extractMetadata(clazz);
        }

        //todo: create source tables if not exists
        //todo tell cache accessor to create its cache tables
    }

    public void extractMetadata(Class<? extends UniqueData> clazz) {
        Preconditions.checkArgument(!uniqueDataMetadataMap.containsKey(clazz), "UniqueData class %s has already been parsed", clazz.getName());
        Data dataAnnotation = clazz.getAnnotation(Data.class);
        Preconditions.checkNotNull(dataAnnotation, "UniqueData class %s is missing @Data annotation", clazz.getName());

        sqlBuilder.parse(clazz);
        UniqueDataMetadata metadata = new UniqueDataMetadata(ValueUtils.parseValue(dataAnnotation.schema()), ValueUtils.parseValue(dataAnnotation.table()), ValueUtils.parseValue(dataAnnotation.idColumn()));
        uniqueDataMetadataMap.put(clazz, metadata);
    }

    public UniqueDataMetadata getMetadata(Class<? extends UniqueData> clazz) {
        UniqueDataMetadata metadata = uniqueDataMetadataMap.get(clazz);
        Preconditions.checkNotNull(metadata, "UniqueData class %s has not been parsed yet", clazz.getName());
        return metadata;
    }

    public void init(UniqueData uniqueData) {
        Data dataAnnotation = uniqueData.getClass().getAnnotation(Data.class);
        Preconditions.checkNotNull(dataAnnotation, "UniqueData class %s is missing @Data annotation", uniqueData.getClass().getName());
        for (FieldInstancePair<@Nullable PersistentValue> pair : ReflectionUtils.getFieldInstancePairs(uniqueData, PersistentValue.class)) {
            Column columnAnnotation = pair.field().getAnnotation(Column.class);
            Preconditions.checkNotNull(columnAnnotation, "PersistentValue field %s is missing @Column annotation", pair.field().getName());

            String columnName = ValueUtils.parseValue(columnAnnotation.value());
            String tableName = ValueUtils.parseValue(columnAnnotation.table().isEmpty() ? dataAnnotation.table() : columnAnnotation.table());
            String schemaName = ValueUtils.parseValue(columnAnnotation.schema().isEmpty() ? dataAnnotation.schema() : columnAnnotation.schema());

            //todo: the primary key gets a bit more complicated when we are dealing with a foreign key. this needs to be handled, and a new ForeignKey created which properly maps my id column to the foreign key column.
            // for the time being, all id columns are just "id"

            PersistentValue<?> newPv = dataAccessor.createPersistentValue(
                    uniqueData,
                    ReflectionUtils.getGenericType(pair.field()),
                    schemaName,
                    tableName,
                    columnName
            );

            logger.debug("Initialized PersistentValue field {}.{} -> {}.{}.{}", uniqueData.getClass().getSimpleName(), pair.field().getName(), schemaName, tableName, columnName);

            if (pair.instance() instanceof PersistentValue.ProxyPersistentValue<?> proxyPv) {
                proxyPv.setDelegate(newPv);
            } else {
                pair.field().setAccessible(true);
                try {
                    pair.field().set(uniqueData, newPv);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
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

    public void insert(UniqueData uniqueData, boolean async) {
        ConnectionConsumer task = (connection) -> {
            boolean autoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            insertIntoSource(connection, uniqueData);
            connection.commit();
            connection.setAutoCommit(autoCommit);
        };

        if (async) {
            submitAsyncTask(task);
        } else {
            submitBlockingTask(task);
        }

        try {
            dataAccessor.insertIntoCache(uniqueData);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void insertIntoSource(Connection connection, UniqueData uniqueData) throws SQLException { //todo: async handling and such
        UniqueDataMetadata metadata = uniqueData.getMetadata();

        Map<String, List<PrimaryKey.ColumnValuePair>> tables = new HashMap<>();
        tables.computeIfAbsent(metadata.schema() + "." + metadata.table(), k -> new ArrayList<>()).add(new PrimaryKey.ColumnValuePair(metadata.idColumn(), uniqueData.getId()));
        for (PersistentValue<?> pv : ReflectionUtils.getFieldInstances(uniqueData, PersistentValue.class)) {
            tables.computeIfAbsent(pv.getSchema() + "." + pv.getTable(), k -> new ArrayList<>()).add(new PrimaryKey.ColumnValuePair(pv.getColumn(), pv.get()));
        }

        for (Map.Entry<String, List<PrimaryKey.ColumnValuePair>> entry : tables.entrySet()) {
            String fullTableName = entry.getKey();
            List<PrimaryKey.ColumnValuePair> columnValuePairs = entry.getValue();

            StringBuilder sqlBuilder = new StringBuilder("INSERT INTO " + fullTableName + " (");
            for (PrimaryKey.ColumnValuePair pair : columnValuePairs) {
                sqlBuilder.append(pair.column()).append(", ");
            }
            sqlBuilder.setLength(sqlBuilder.length() - 2);
            sqlBuilder.append(") VALUES (");
            sqlBuilder.append("?, ".repeat(columnValuePairs.size()));
            sqlBuilder.setLength(sqlBuilder.length() - 2);
            sqlBuilder.append(")");

            //todo: on conflict : use insertion strategy for this

            String sql = sqlBuilder.toString();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            for (int i = 0; i < columnValuePairs.size(); i++) {
                PrimaryKey.ColumnValuePair pair = columnValuePairs.get(i);
                preparedStatement.setObject(i + 1, pair.value());
            }
            preparedStatement.executeUpdate();
        }
    }
}
