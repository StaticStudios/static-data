package net.staticstudios.data.impl.h2;

import com.google.common.base.Preconditions;
import com.impossibl.postgres.api.jdbc.PGConnection;
import net.staticstudios.data.DataAccessor;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.InsertMode;
import net.staticstudios.data.impl.pg.PostgresListener;
import net.staticstudios.data.parse.DDLStatement;
import net.staticstudios.data.parse.SQLColumn;
import net.staticstudios.data.parse.SQLSchema;
import net.staticstudios.data.parse.SQLTable;
import net.staticstudios.data.primative.Primitives;
import net.staticstudios.data.util.ColumnMetadata;
import net.staticstudios.data.util.SQlStatement;
import net.staticstudios.data.util.SchemaTable;
import net.staticstudios.data.util.TaskQueue;
import net.staticstudios.utils.Pair;
import net.staticstudios.utils.ShutdownStage;
import net.staticstudios.utils.ThreadUtils;
import org.intellij.lang.annotations.Language;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * This data accessor uses a write-behind caching strategy with an in-memory H2 database to optimize read and write operations.
 */
public class H2DataAccessor implements DataAccessor {
    private static final Logger logger = LoggerFactory.getLogger(H2DataAccessor.class);
    @Language("SQL")
    private static final String SET_REFERENTIAL_INTEGRITY_FALSE = "SET REFERENTIAL_INTEGRITY FALSE";
    @Language("SQL")
    private static final String SET_REFERENTIAL_INTEGRITY_TRUE = "SET REFERENTIAL_INTEGRITY TRUE";
    private final TaskQueue taskQueue;
    private final String jdbcUrl;
    private final ThreadLocal<Connection> threadConnection = new ThreadLocal<>();
    private final ThreadLocal<Map<String, PreparedStatement>> threadPreparedStatementCache = new ThreadLocal<>();
    private final Set<String> knownTables = new HashSet<>();
    private final DataManager dataManager;
    private final PostgresListener postgresListener;

    public H2DataAccessor(DataManager dataManager, PostgresListener postgresListener, TaskQueue taskQueue) {
        this.taskQueue = taskQueue;
        this.postgresListener = postgresListener;
        this.jdbcUrl = "jdbc:h2:mem:static-data-cache;DB_CLOSE_DELAY=-1;LOCK_MODE=0;CACHE_SIZE=65536";
        this.dataManager = dataManager;

        postgresListener.addHandler(notification -> {
            try {
                SQLSchema sqlSchema = dataManager.getSQLBuilder().getSchema(notification.getSchema());
                Preconditions.checkNotNull(sqlSchema, "Schema %s not found".formatted(notification.getSchema()));
                SQLTable sqlTable = sqlSchema.getTable(notification.getTable());
                Preconditions.checkNotNull(sqlTable, "Table %s.%s not found".formatted(notification.getSchema(), notification.getTable()));
                switch (notification.getOperation()) {
                    case UPDATE -> { //todo: when we update an id column, we have to update references to uniquedata objects, and the map. do this logic in the H2Trigger.
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

                        for (Pair<String, String> changed : changedValues) {
                            if (changed == null) break;
                            String column = changed.first();
                            String encoded = changed.second();
                            SQLColumn sqlColumn = sqlTable.getColumn(column);
                            Preconditions.checkNotNull(sqlColumn, "Column %s.%s.%s not found".formatted(notification.getSchema(), notification.getTable(), column));
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
                            Preconditions.checkNotNull(sqlColumn, "Column %s.%s.%s not found".formatted(notification.getSchema(), notification.getTable(), column));
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
    }

    public synchronized void sync(List<SchemaTable> schemaTables) throws SQLException {
        //todo: when we resync we should clear the task queue and steal the connection
        //todo: if possible, id like to pause everything else until we are done syncing
        dataManager.submitBlockingTask(realDbConnection -> {
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
        });

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
            threadConnection.set(connection);

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
                try (PreparedStatement preparedStatement = connection.prepareStatement(sqlStatement.getSql())) {
                    int i = 1;
                    for (Object value : sqlStatement.getValues()) {
                        preparedStatement.setObject(i++, value);
                    }
                    logger.debug("[H2] {}", sqlStatement.getSql());
                    preparedStatement.executeUpdate();
                }
            }

            CompletableFuture<Void> future = taskQueue.submitTask(realConnection -> {
                boolean realAutoCommit = realConnection.getAutoCommit();
                realConnection.setAutoCommit(false);
                try {
                    for (SQlStatement statement : sqlStatements) {
                        try (PreparedStatement preparedStatement = realConnection.prepareStatement(statement.getSql())) {
                            List<Object> values = statement.getValues();
                            for (int i = 0; i < values.size(); i++) {
                                preparedStatement.setObject(i + 1, values.get(i));
                            }
                            logger.debug("[DB] {}", statement.getSql());
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
        logger.debug("[H2] {}", sql);
        return cachePreparedStatement.executeQuery();
    }

    @Override
    public void executeUpdate(@Language("SQL") String sql, List<Object> values) throws SQLException {
        PreparedStatement cachePreparedStatement = prepareStatement(sql);
        for (int i = 0; i < values.size(); i++) {
            cachePreparedStatement.setObject(i + 1, values.get(i));
        }
        logger.debug("[H2] {}", sql);
        cachePreparedStatement.executeUpdate();

        taskQueue.submitTask(connection -> {
            PreparedStatement realPreparedStatement = connection.prepareStatement(sql);
            for (int i = 0; i < values.size(); i++) {
                realPreparedStatement.setObject(i + 1, values.get(i));
            }
            logger.debug("[DB] {}}", sql);
            realPreparedStatement.executeUpdate();
        });
    }

    @Override
    public void runDDL(DDLStatement ddl) {
        taskQueue.submitTask(connection -> {
            logger.debug("[DB] {}", ddl.postgresqlStatement());
            connection.createStatement().execute(ddl.postgresqlStatement());
            try (Statement statement = getConnection().createStatement()) {
                logger.trace("[H2] {}", ddl.h2Statement());
                statement.execute(ddl.h2Statement());
            }
        }).join();

    }

    @Override
    public void postDDL() throws SQLException {
        updateKnownTables();
    }

    private synchronized void updateKnownTables() throws SQLException {
        Set<String> currentTables = new HashSet<>();
        Connection connection = getConnection();
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(
                     "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'SYSTEM_LOBS') AND TABLE_TYPE='BASE TABLE'")) {
            List<SchemaTable> toSync = new ArrayList<>();
            while (rs.next()) {
                String schema = rs.getString("TABLE_SCHEMA");
                String table = rs.getString("TABLE_NAME");
                currentTables.add(schema + "." + table);

                if (!knownTables.contains(schema + "." + table)) {
                    logger.trace("Discovered new table {}.{}", schema, table);
                    UUID randomId = UUID.randomUUID();
                    @Language("SQL") String sql = "CREATE TRIGGER IF NOT EXISTS \"trg_%s_%s\" AFTER INSERT, UPDATE, DELETE ON \"%s\".\"%s\" FOR EACH ROW CALL '%s'";

                    try (Statement createTrigger = connection.createStatement()) {
                        String formatted = sql.formatted(table, randomId.toString().replace('-', '_'), schema, table, H2Trigger.class.getName());
                        logger.trace("[H2] {}", formatted);
                        H2Trigger.registerDataManager(randomId, dataManager);
                        createTrigger.execute(formatted);
                    }

                    dataManager.submitBlockingTask(realDbConnection -> postgresListener.ensureTableHasTrigger(realDbConnection, schema, table));
                    toSync.add(new SchemaTable(schema, table));
                }
            }
            sync(toSync);
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
}


//todo: maintain a buffer of what to send the the real db, and then collapse similar prepared statements into one so we can batch them. have a configurable interval to flush, but by default this will be 0ms
