package net.staticstudios.data.impl.h2;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.impossibl.postgres.api.jdbc.PGConnection;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.InsertMode;
import net.staticstudios.data.impl.DataAccessor;
import net.staticstudios.data.impl.h2.trigger.H2UpdateHandlerTrigger;
import net.staticstudios.data.impl.pg.PostgresListener;
import net.staticstudios.data.impl.redis.RedisEncodedValue;
import net.staticstudios.data.impl.redis.RedisEvent;
import net.staticstudios.data.impl.redis.RedisListener;
import net.staticstudios.data.parse.DDLStatement;
import net.staticstudios.data.parse.SQLColumn;
import net.staticstudios.data.parse.SQLSchema;
import net.staticstudios.data.parse.SQLTable;
import net.staticstudios.data.primative.Primitives;
import net.staticstudios.data.util.*;
import net.staticstudios.data.util.TaskQueue;
import net.staticstudios.utils.Pair;
import net.staticstudios.utils.ShutdownStage;
import net.staticstudios.utils.ThreadUtils;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * This data accessor uses a write-behind caching strategy with an in-memory H2 database to optimize read and write operations.
 */
public class H2DataAccessor implements DataAccessor {
    private static final Logger logger = LoggerFactory.getLogger(H2DataAccessor.class);
    @Language("SQL")
    private static final String SET_REFERENTIAL_INTEGRITY_FALSE = "SET REFERENTIAL_INTEGRITY FALSE";
    @Language("SQL")
    private static final String SET_REFERENTIAL_INTEGRITY_TRUE = "SET REFERENTIAL_INTEGRITY TRUE";
    private static final Gson GSON = new Gson();
    private final TaskQueue taskQueue;
    private final String jdbcUrl;
    private final ThreadLocal<Connection> threadConnection = new ThreadLocal<>();
    private final ThreadLocal<Map<String, PreparedStatement>> threadPreparedStatementCache = new ThreadLocal<>();
    private final Set<SchemaTable> knownTables = new HashSet<>();
    private final DataManager dataManager;
    private final PostgresListener postgresListener;
    private final Map<List<Pair<String, String>>, Runnable> delayedTasks = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(thread -> {
        Thread t = new Thread(thread);
        t.setName(H2DataAccessor.class.getSimpleName() + "-ScheduledExecutor");
        return t;
    });
    private final RedisListener redisListener;
    private final Map<String, String> redisCache = new ConcurrentHashMap<>();
    private final Set<String> knownRedisPartialKeys = ConcurrentHashMap.newKeySet();

    private final ThreadLocal<LinkedList<Runnable>> commitCallbacks = ThreadLocal.withInitial(LinkedList::new);
    private final ThreadLocal<LinkedList<Runnable>> rollbackCallbacks = ThreadLocal.withInitial(LinkedList::new);

    public H2DataAccessor(DataManager dataManager, PostgresListener postgresListener, RedisListener redisListener, TaskQueue taskQueue) {
        try {
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load H2 Driver", e);
        }

        this.taskQueue = taskQueue;
        this.postgresListener = postgresListener;
        this.redisListener = redisListener;
        this.jdbcUrl = "jdbc:h2:mem:static-data-cache;DB_CLOSE_DELAY=-1;LOCK_MODE=3;CACHE_SIZE=65536;QUERY_CACHE_SIZE=1024;CACHE_TYPE=SOFT_LRU";
        this.dataManager = dataManager;

        postgresListener.addHandler(notification -> {
            try {
                SQLSchema sqlSchema = dataManager.getSQLBuilder().getSchema(notification.getSchema());
                if (sqlSchema == null) {
                    return; // we don't care about this schema
                }
                SQLTable sqlTable = sqlSchema.getTable(notification.getTable());
                if (sqlTable == null) {
                    return; // we don't care about this table
                }
                switch (notification.getOperation()) {
                    case UPDATE -> {
                        List<Object> values = new ArrayList<>();
                        StringBuilder sb = new StringBuilder("UPDATE \"").append(notification.getSchema()).append("\".\"").append(notification.getTable()).append("\" SET ");
                        Pair<String, String>[] changedValues = new Pair[notification.getData().newDataValueMap().size()];

                        int index = 0;
                        for (Map.Entry<String, String> entry : notification.getData().newDataValueMap().entrySet()) {
                            String column = entry.getKey();
                            String newValue = entry.getValue();
                            String oldValue = notification.getData().oldDataValueMap().get(column);
                            if (!Objects.equals(newValue, oldValue)) {
                                changedValues[index++] = Pair.of(column, newValue);
                            }
                        }

                        if (index == 0) {
                            return; // nothing changed
                        }

                        for (Pair<String, String> changed : changedValues) {
                            if (changed == null) break;
                            String column = changed.first();
                            String encoded = changed.second();
                            SQLColumn sqlColumn = sqlTable.getColumn(column);
                            if (sqlColumn == null) {
                                return; // we don't care about this column
                            }
                            Object decoded = encoded == null ? null : Primitives.decodePrimitive(sqlColumn.getType(), encoded);
                            values.add(decoded);
                            sb.append("\"").append(column).append("\" = ?, ");
                        }
                        sb.setLength(sb.length() - 2);
                        sb.append(" WHERE ");
                        for (ColumnMetadata idColumnMetadata : sqlTable.getIdColumns()) {
                            String idColumn = idColumnMetadata.name();
                            sb.append("\"").append(idColumn).append("\" = ? AND ");
                            SQLColumn sqlColumn = sqlTable.getColumn(idColumn);
                            Preconditions.checkNotNull(sqlColumn, "Column %s.%s.%s not found".formatted(notification.getSchema(), notification.getTable(), idColumn));
                            String encoded = notification.getData().oldDataValueMap().get(idColumn);
                            Preconditions.checkNotNull(encoded, "ID Column %s.%s.%s not found in notification".formatted(notification.getSchema(), notification.getTable(), idColumn));
                            Object decoded = Primitives.decodePrimitive(sqlColumn.getType(), encoded);
                            values.add(decoded);
                        }
                        sb.setLength(sb.length() - 5);
                        String sql = sb.toString();

                        Connection connection = getConnection();
                        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                            for (int i = 0; i < values.size(); i++) {
                                preparedStatement.setObject(i + 1, values.get(i));
                            }
                            logger.debug("[H2] [HANDLE POSTGRES UPDATE] {}", sql);
                            preparedStatement.executeUpdate();
                            if (!connection.getAutoCommit()) {
                                connection.commit();
                            }
                        }
                    }
                    case INSERT -> {
                        List<Object> values = new ArrayList<>();
                        StringBuilder sb = new StringBuilder("INSERT INTO \"").append(notification.getSchema()).append("\".\"").append(notification.getTable()).append("\" (");

                        for (Map.Entry<String, String> entry : notification.getData().newDataValueMap().entrySet()) {
                            String column = entry.getKey();
                            String encoded = entry.getValue();
                            SQLColumn sqlColumn = sqlTable.getColumn(column);
                            if (sqlColumn == null) {
                                return; // we don't care about this column
                            }
                            Object decoded = encoded == null ? null : Primitives.decodePrimitive(sqlColumn.getType(), encoded);
                            values.add(decoded);
                            sb.append("\"").append(column).append("\", ");
                        }
                        sb.setLength(sb.length() - 2);
                        sb.append(") VALUES (");
                        sb.append("?, ".repeat(values.size()));
                        sb.setLength(sb.length() - 2);
                        sb.append(")");
                        String sql = sb.toString();

                        Connection connection = getConnection();
                        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                            for (int i = 0; i < values.size(); i++) {
                                preparedStatement.setObject(i + 1, values.get(i));
                            }
                            logger.debug("[H2] [HANDLE POSTGRES INSERT] {}", sql);
                            preparedStatement.executeUpdate();
                            if (!connection.getAutoCommit()) {
                                connection.commit();
                            }
                        }
                    }
                    case DELETE -> {
                        List<Object> values = new ArrayList<>();
                        StringBuilder sb = new StringBuilder("DELETE FROM \"").append(notification.getSchema()).append("\".\"").append(notification.getTable()).append("\" WHERE ");
                        for (ColumnMetadata idColumnMetadata : sqlTable.getIdColumns()) {
                            String idColumn = idColumnMetadata.name();
                            sb.append("\"").append(idColumn).append("\" = ? AND ");
                            SQLColumn sqlColumn = sqlTable.getColumn(idColumn);
                            Preconditions.checkNotNull(sqlColumn, "Column %s.%s.%s not found".formatted(notification.getSchema(), notification.getTable(), idColumn));
                            String encoded = notification.getData().oldDataValueMap().get(idColumn);
                            Preconditions.checkNotNull(encoded, "ID Column %s.%s.%s not found in notification".formatted(notification.getSchema(), notification.getTable(), idColumn));
                            Object decoded = Primitives.decodePrimitive(sqlColumn.getType(), encoded);
                            values.add(decoded);
                        }
                        sb.setLength(sb.length() - 5);
                        String sql = sb.toString();

                        Connection connection = getConnection();
                        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                            for (int i = 0; i < values.size(); i++) {
                                preparedStatement.setObject(i + 1, values.get(i));
                            }
                            logger.debug("[H2] [HANDLE POSTGRES DELETE] {}", sql);
                            preparedStatement.executeUpdate();
                            if (!connection.getAutoCommit()) {
                                connection.commit();
                            }
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Error handling notification from postgres", e);
            }
        });

        ThreadUtils.onShutdownRunSync(ShutdownStage.FINAL, () -> {
            // wipe the db on shutdown. This is especially useful for unit tests.
            try (Connection connection = DriverManager.getConnection(jdbcUrl)) {
                try (Statement statement = connection.createStatement()) {
                    String resetDb = "DROP ALL OBJECTS DELETE FILES".toString(); // call toString so that IDEs dont freak out about invalid SQL
                    statement.execute(resetDb);
                }
            } catch (SQLException e) {
                logger.error("Failed to shutdown H2 database", e);
            }
        });


        ThreadUtils.onShutdownRunSync(ShutdownStage.EARLY, () -> {
            List<Runnable> tasks = scheduledExecutorService.shutdownNow();

            logger.info("Shutting down {}, running {} enqueued tasks", H2DataAccessor.class.getSimpleName(), tasks.size());
            for (Runnable task : tasks) {
                task.run();
            }
        });
    }

    public synchronized void sync(List<SchemaTable> schemaTables, List<String> redisPartialKeys) throws SQLException {
        taskQueue.submitTask((realDbConnection, jedis) -> {
            if (!schemaTables.isEmpty()) {
                Connection h2Connection = getConnection();
                boolean autoCommit = h2Connection.getAutoCommit();
                try (
                        Statement h2Statement = h2Connection.createStatement()
                ) {
                    h2Connection.setAutoCommit(false);
                    logger.trace("[H2] {}", SET_REFERENTIAL_INTEGRITY_FALSE);
                    h2Statement.execute(SET_REFERENTIAL_INTEGRITY_FALSE);

                    for (SchemaTable schemaTable : schemaTables) {
                        String schema = schemaTable.schema();
                        String table = schemaTable.table();
                        Path tmpFile = Paths.get(System.getProperty("java.io.tmpdir"), UUID.randomUUID() + "_" + schema + "_" + table + ".csv");
                        String absolutePath = tmpFile.toAbsolutePath().toString();

                        List<String> columns = getColumnsInTable(schema, table);

                        StringBuilder sqlBuilder = new StringBuilder("COPY (SELECT ");
                        for (String column : columns) {
                            sqlBuilder.append("\"").append(column).append("\", ");
                        }
                        sqlBuilder.setLength(sqlBuilder.length() - 2);
                        sqlBuilder.append(" FROM \"").append(schema).append("\".\"").append(table).append("\") TO STDOUT WITH CSV HEADER");
                        @Language("SQL") String copySql = sqlBuilder.toString();
                        @Language("SQL") String truncateSql = "TRUNCATE TABLE \"" + schema + "\".\"" + table + "\"";
                        StringBuilder insertSqlBuilder = new StringBuilder("INSERT INTO \"").append(schema).append("\".\"").append(table).append("\" (");
                        for (String column : columns) {
                            insertSqlBuilder.append("\"").append(column).append("\", ");
                        }
                        insertSqlBuilder.setLength(insertSqlBuilder.length() - 2);
                        insertSqlBuilder.append(") SELECT * FROM CSVREAD('").append(absolutePath).append("')");
                        String insertSql = insertSqlBuilder.toString();
                        PGConnection pgConnection = realDbConnection.unwrap(PGConnection.class);
                        FileOutputStream fileOutputStream;
                        try {
                            fileOutputStream = new FileOutputStream(absolutePath);
                        } catch (FileNotFoundException e) {
                            throw new RuntimeException(e);
                        }
                        logger.debug("[DB] {}", copySql);
                        pgConnection.copyTo(copySql, fileOutputStream);
                        logger.trace("[H2] {}", truncateSql);
                        h2Statement.execute(truncateSql);
                        logger.trace("[H2] {}", insertSql);
                        h2Statement.execute(insertSql);
                    }
                    logger.trace("[H2] {}", SET_REFERENTIAL_INTEGRITY_TRUE);
                    h2Statement.execute(SET_REFERENTIAL_INTEGRITY_TRUE);
                } finally {
                    if (autoCommit) {
                        h2Connection.setAutoCommit(true);
                    } else {
                        h2Connection.commit();
                    }
                }
            }
            for (String partialKey : redisPartialKeys) {
                String cursor = ScanParams.SCAN_POINTER_START;
                ScanParams scanParams = new ScanParams().match(partialKey).count(1000);

                do {
                    ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                    cursor = scanResult.getCursor();

                    for (String key : scanResult.getResult()) {
                        redisCache.put(key, decodeRedis(jedis.get(key)).value());
                    }
                } while (!cursor.equals(ScanParams.SCAN_POINTER_START));

                redisListener.listen(partialKey, this::handleRedisEvent);
            }
        }).join();

        //todo: start listening to changes from pg
        // then log them
        // load appropriate data into h2
        // process logs
        // then continue as normal
    }

    private Connection getConnection() throws SQLException {
        Connection connection = threadConnection.get();
        if (connection == null) {
            connection = DriverManager.getConnection(jdbcUrl);
            connection.setAutoCommit(false);
            threadConnection.set(connection = new H2ProxyConnection(connection, this));

            ThreadUtils.onShutdownRunSync(ShutdownStage.CLEANUP, () -> {
                Connection _connection = threadConnection.get();
                if (_connection == null) return;
                try {
                    _connection.close();
                } catch (SQLException e) {
                    logger.error("Failed to close H2 connection", e);
                } finally {
                    threadConnection.remove();
                }
            });
        }
        return connection;
    }

    public PreparedStatement prepareStatement(@Language("SQL") String sql) throws SQLException {
        Connection connection = getConnection();
        Map<String, PreparedStatement> preparedStatementCache = threadPreparedStatementCache.get();
        if (preparedStatementCache == null) {
            preparedStatementCache = new HashMap<>();
            threadPreparedStatementCache.set(preparedStatementCache);
        }

        PreparedStatement preparedStatement = preparedStatementCache.get(sql);
        if (preparedStatement == null) {
            preparedStatement = connection.prepareStatement(sql);
            preparedStatementCache.put(sql, preparedStatement);
        }

        return preparedStatement;
    }

    @Override
    public void insert(List<SQlStatement> sqlStatements, InsertMode insertMode) throws SQLException {
        Connection connection = getConnection();
        boolean autoCommit = connection.getAutoCommit();
        try {
            connection.setAutoCommit(false);

            for (SQlStatement sqlStatement : sqlStatements) {
                try (PreparedStatement preparedStatement = connection.prepareStatement(sqlStatement.getH2Sql())) {
                    int i = 1;
                    for (Object value : sqlStatement.getValues()) {
                        preparedStatement.setObject(i++, value);
                    }
                    logger.trace("[H2] {}", sqlStatement.getH2Sql());
                    preparedStatement.executeUpdate();
                }
            }

            CompletableFuture<Void> future = taskQueue.submitTask(realConnection -> {
                boolean realAutoCommit = realConnection.getAutoCommit();
                realConnection.setAutoCommit(false);
                try {
                    for (SQlStatement statement : sqlStatements) {
                        try (PreparedStatement preparedStatement = realConnection.prepareStatement(statement.getPgSql())) {
                            List<Object> values = statement.getValues();
                            for (int i = 0; i < values.size(); i++) {
                                Object value = values.get(i);
                                preparedStatement.setObject(i + 1, value);
                            }
                            logger.debug("[DB] {}", statement.getPgSql());
                            preparedStatement.executeUpdate();
                        }
                    }
                } finally {
                    if (realAutoCommit) {
                        realConnection.setAutoCommit(true);
                    } else {
                        realConnection.commit();
                    }
                }
            });

            if (insertMode == InsertMode.SYNC) {
                try {
                    future.join();
                } catch (CompletionException e) {
                    connection.rollback();
                    logger.error("Error updating the real db", e.getCause());
                }
            }
        } finally {
            if (autoCommit) {
                connection.setAutoCommit(true);
            } else {
                connection.commit();
            }
        }
    }

    @Override
    public ResultSet executeQuery(@Language("SQL") String sql, List<Object> values) throws SQLException {
        PreparedStatement cachePreparedStatement = prepareStatement(sql);
        for (int i = 0; i < values.size(); i++) {
            cachePreparedStatement.setObject(i + 1, values.get(i));
        }
        logger.trace("[H2] {}", sql);
        return cachePreparedStatement.executeQuery();
    }

    @Override
    public void executeTransaction(SQLTransaction transaction, int delay) throws SQLException {
        Connection connection = getConnection();
        boolean autoCommit = connection.getAutoCommit();
        try {
            connection.setAutoCommit(false);
            for (SQLTransaction.Operation operation : transaction.getOperations()) {
                SQLTransaction.Statement sqlStatement = operation.getStatement();
                String h2Sql = sqlStatement.getH2Sql();
                PreparedStatement cachePreparedStatement = prepareStatement(h2Sql);
                int i = 0;
                List<Object> values = operation.getValuesSupplier().get();
                for (Object value : values) {
                    cachePreparedStatement.setObject(++i, value);
                }
                logger.trace("[H2] {}", h2Sql);
                Consumer<ResultSet> resultHandler = operation.getResultHandler();
                if (resultHandler == null) {
                    cachePreparedStatement.executeUpdate();
                } else {
                    try (ResultSet rs = cachePreparedStatement.executeQuery()) {
                        resultHandler.accept(rs);
                    }
                }
            }
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        } finally {
            if (autoCommit) {
                connection.setAutoCommit(true);
            }
        }

        runDatabaseTask(transaction, delay);
    }

    @Override
    public void runDDL(DDLStatement ddl) {
        taskQueue.submitTask(connection -> {
            if (!ddl.postgresqlStatement().isEmpty()) {

                logger.debug("[DB] {}", ddl.postgresqlStatement());
                try {
                    connection.createStatement().execute(ddl.postgresqlStatement());
                } catch (Exception e) {
                    logger.error("Error executing DDL on real database: {}", ddl.postgresqlStatement(), e);
                    throw e;
                }
            }
            if (ddl.h2Statement().isEmpty()) return;
            try (Statement statement = getConnection().createStatement()) {
                logger.trace("[H2] {}", ddl.h2Statement());
                statement.execute(ddl.h2Statement());
            } catch (SQLException e) {
                logger.error("Error executing DDL on H2 database: {}", ddl.h2Statement(), e);
                throw e;
            }
        }).join();

    }

    @Override
    public void postDDL() throws SQLException {
        updateKnownTables();
    }

    @Override
    public @Nullable String getRedisValue(String key) {
        return redisCache.get(key);
    }

    @Override
    public void setRedisValue(String key, String value, int expirationSeconds) {
        String prev;
        if (value == null) {
            prev = redisCache.remove(key);
            taskQueue.submitTask((connection, jedis) -> {
                jedis.del(key);
            });
        } else {
            prev = redisCache.put(key, value);
            taskQueue.submitTask((connection, jedis) -> {
                if (expirationSeconds > 0) {
                    jedis.setex(key, expirationSeconds, encodeRedis(value));
                } else {
                    jedis.set(key, encodeRedis(value));
                }
            });
        }

        RedisUtils.DeconstructedKey deconstructedKey = RedisUtils.deconstruct(key);
        dataManager.callCachedValueUpdateHandlers(deconstructedKey.partialKey(), deconstructedKey.encodedIdNames(), deconstructedKey.encodedIdValues(), prev, value);
    }

    @Override
    public void discoverRedisKeys(List<String> partialRedisKeys) {
        knownRedisPartialKeys.addAll(partialRedisKeys);
    }

    @Override
    public synchronized void resync() {
        //todo: i would ideally like to support periodic resyncing of data. even if this means we pause everything until then. not exactly sure how this would look tho.

        //todo: when we resync we should clear the task queue and steal the connection
        //todo: if possible, id like to pause everything else until we are done syncing
        try {
            sync(new ArrayList<>(knownTables), new ArrayList<>(knownRedisPartialKeys));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private synchronized void updateKnownTables() throws SQLException {
        Set<SchemaTable> currentTables = new HashSet<>();
        Connection connection = getConnection();
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(
                     "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'SYSTEM_LOBS') AND TABLE_TYPE='BASE TABLE'")) {
            while (rs.next()) {
                String schema = rs.getString("TABLE_SCHEMA");
                String table = rs.getString("TABLE_NAME");
                SchemaTable schemaTable = new SchemaTable(schema, table);
                currentTables.add(schemaTable);

                if (!knownTables.contains(schemaTable)) {
                    logger.debug("Discovered new referringTable {}.{}", schema, table);
                    UUID randomId = UUID.randomUUID();
                    @Language("SQL") String sql = "CREATE TRIGGER IF NOT EXISTS \"insert_update_trg_%s_%s\" AFTER INSERT, UPDATE ON \"%s\".\"%s\" FOR EACH ROW CALL '%s'";

                    try (Statement createTrigger = connection.createStatement()) {
                        String formatted = sql.formatted(table, randomId.toString().replace('-', '_'), schema, table, H2UpdateHandlerTrigger.class.getName());
                        logger.trace("[H2] {}", formatted);
                        H2UpdateHandlerTrigger.registerDataManager(randomId, dataManager);
                        createTrigger.execute(formatted);
                    }

                    sql = "CREATE TRIGGER IF NOT EXISTS \"delete_trg_%s_%s\" BEFORE DELETE ON \"%s\".\"%s\" FOR EACH ROW CALL '%s'";

                    try (Statement createTrigger = connection.createStatement()) {
                        String formatted = sql.formatted(table, randomId.toString().replace('-', '_'), schema, table, H2UpdateHandlerTrigger.class.getName());
                        logger.trace("[H2] {}", formatted);
                        H2UpdateHandlerTrigger.registerDataManager(randomId, dataManager);
                        createTrigger.execute(formatted);
                    }

                    taskQueue.submitTask(realDbConnection -> postgresListener.ensureTableHasTrigger(realDbConnection, schema, table)).join();
                }
            }
        }
        knownTables.clear();
        knownTables.addAll(currentTables);
    }

    private List<String> getColumnsInTable(String schema, String table) throws SQLException {
        List<String> columns = new ArrayList<>();
        try (PreparedStatement ps = getConnection().prepareStatement(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION"
        )) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    columns.add(rs.getString("COLUMN_NAME"));
                }
            }
        }
        return columns;
    }

    private void runDatabaseTask(SQLTransaction transaction, int delay) {
        List<Pair<String, String>> key = new ArrayList<>(transaction.getOperations().size());
        for (SQLTransaction.Operation operation : transaction.getOperations()) {
            SQLTransaction.Statement sqlStatement = operation.getStatement();
            key.add(Pair.of(sqlStatement.getH2Sql(), sqlStatement.getPgSql()));
        }

        Runnable runnable = () -> taskQueue.submitTask(connection -> {
            boolean autoCommit = connection.getAutoCommit();
            try {
                connection.setAutoCommit(false);
                for (SQLTransaction.Operation operation : transaction.getOperations()) {
                    if (operation.getResultHandler() != null) {
                        // we don't support queries on the real db
                        continue;
                    }
                    SQLTransaction.Statement statement = operation.getStatement();
                    PreparedStatement realPreparedStatement = connection.prepareStatement(statement.getPgSql());
                    int i = 0;
                    List<Object> values = operation.getValuesSupplier().get();
                    for (Object value : values) {
                        realPreparedStatement.setObject(++i, value);
                    }
                    logger.debug("[DB] {}", statement.getPgSql());
                    realPreparedStatement.executeUpdate();
                }
                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                //todo: ideally this should trigger an error which causes us to resync the h2 db
                logger.error("Error updating the real db", e);
            } finally {
                if (autoCommit) {
                    connection.setAutoCommit(true);
                }
            }
        });

        if (delay <= 0) {
            runnable.run();
            return;
        }
        if (delayedTasks.put(key, runnable) == null) {
            scheduledExecutorService.schedule(() -> {
                Runnable removed = delayedTasks.remove(key);
                if (removed != null) {
                    removed.run();
                }
            }, delay, TimeUnit.MILLISECONDS);
        }
    }

    private void handleRedisEvent(RedisEvent event, String key, @Nullable String value) {
        RedisEncodedValue redisEncoded = value == null ? null : decodeRedis(value);
        String redisValue = redisEncoded == null ? null : redisEncoded.value();

        if (redisEncoded != null && Objects.equals(redisEncoded.staticDataAppName(), dataManager.getApplicationName())) {
            return; // ignore events from ourselves
        }

        if (event == RedisEvent.SET) {
            String entry = redisCache.get(key);
            if (entry != null && Objects.equals(entry, redisValue)) {
                return;
            }
            redisCache.put(key, redisValue);
            RedisUtils.DeconstructedKey deconstructedKey = RedisUtils.deconstruct(key);
            dataManager.callCachedValueUpdateHandlers(deconstructedKey.partialKey(), deconstructedKey.encodedIdNames(), deconstructedKey.encodedIdValues(), entry, redisValue);
        } else if (event == RedisEvent.DEL || event == RedisEvent.EXPIRED) {
            String entry = redisCache.remove(key);
            if (entry != null) {
                RedisUtils.DeconstructedKey deconstructedKey = RedisUtils.deconstruct(key);
                dataManager.callCachedValueUpdateHandlers(deconstructedKey.partialKey(), deconstructedKey.encodedIdNames(), deconstructedKey.encodedIdValues(), entry, null);
            }
        }
    }

    public void onCommit(Runnable callback) {
        commitCallbacks.get().add(callback);
    }

    public void onRollback(Runnable callback) {
        rollbackCallbacks.get().add(callback);
    }

    protected void handleCommit() {
        List<Runnable> callbacks = commitCallbacks.get();
        commitCallbacks.set(new LinkedList<>());
        rollbackCallbacks.get().clear();
        if (callbacks != null) {
            for (Runnable callback : callbacks) {
                try {
                    callback.run();
                } catch (Exception e) {
                    logger.error("Error executing commit callback", e);
                }
            }
        }
    }

    protected void handleRollback() {
        List<Runnable> callbacks = rollbackCallbacks.get();
        rollbackCallbacks.set(new LinkedList<>());
        commitCallbacks.get().clear();
        if (callbacks != null) {
            for (Runnable callback : callbacks) {
                try {
                    callback.run();
                } catch (Exception e) {
                    logger.error("Error executing rollback callback", e);
                }
            }
        }
    }

    private String encodeRedis(Object value) {
        return GSON.toJson(new RedisEncodedValue(dataManager.getApplicationName(), value.toString()));
    }

    private RedisEncodedValue decodeRedis(String encoded) {
        return GSON.fromJson(encoded, RedisEncodedValue.class);
    }
}
