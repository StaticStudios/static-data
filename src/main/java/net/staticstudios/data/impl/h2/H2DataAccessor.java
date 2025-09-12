package net.staticstudios.data.impl.h2;

import com.impossibl.postgres.api.jdbc.PGConnection;
import net.staticstudios.data.DataAccessor;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.impl.pg.PostgresListener;
import net.staticstudios.data.insert.InsertContext;
import net.staticstudios.data.insert.InsertMode;
import net.staticstudios.data.util.ColumnMetadata;
import net.staticstudios.data.util.SQlStatement;
import net.staticstudios.data.util.TaskQueue;
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
            switch (notification.getOperation()) { //todo: update our cache
                case UPDATE -> {
                }
                case INSERT -> {
                }
                case DELETE -> {
                }
            }
        });
    }

    public synchronized void sync(String schema, String table) throws SQLException {
        //todo: when we resync we should clear the task queue and steal the connection
        //todo: if possible, id like to pause everything else until we are done syncing
        dataManager.submitBlockingTask(realDbConnection -> {
            Path tmpFile = Paths.get(System.getProperty("java.io.tmpdir"), schema + "_" + table + ".csv");
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
            Connection h2Connection = getConnection();
            boolean autoCommit = h2Connection.getAutoCommit();
            try (
                    Statement h2Statement = h2Connection.createStatement()
            ) {
                h2Connection.setAutoCommit(false);
                logger.trace("[H2] {}", truncateSql);
                h2Statement.execute(truncateSql);
                logger.trace("[H2] {}", insertSql);
                h2Statement.execute(insertSql);
            } finally {
                if (autoCommit) {
                    h2Connection.setAutoCommit(true);
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
    public void insert(InsertContext insertContext, InsertMode insertMode) throws SQLException {
        Connection connection = getConnection();
        boolean autoCommit = connection.getAutoCommit();
        try {
            connection.setAutoCommit(false);
            List<SQlStatement> sqlStatements = new ArrayList<>();
            Map<String, Map<String, List<ColumnMetadata>>> columnsByTable = new HashMap<>();
            for (Map.Entry<ColumnMetadata, Object> entry : insertContext.getEntries().entrySet()) {
                ColumnMetadata column = entry.getKey();
                columnsByTable.computeIfAbsent(column.schema(), k -> new HashMap<>())
                        .computeIfAbsent(column.table(), k -> new ArrayList<>())
                        .add(column);
            }

            for (Map.Entry<String, Map<String, List<ColumnMetadata>>> schemaEntry : columnsByTable.entrySet()) {
                String schema = schemaEntry.getKey();
                for (Map.Entry<String, List<ColumnMetadata>> tableEntry : schemaEntry.getValue().entrySet()) {
                    String table = tableEntry.getKey();
                    List<ColumnMetadata> columns = tableEntry.getValue();

                    StringBuilder sqlBuilder = new StringBuilder("INSERT INTO \"");
                    sqlBuilder.append(schema).append("\".\"").append(table).append("\" (");
                    for (ColumnMetadata column : columns) {
                        sqlBuilder.append("\"").append(column.name()).append("\", ");
                    }
                    sqlBuilder.setLength(sqlBuilder.length() - 2);
                    sqlBuilder.append(") VALUES (");
                    sqlBuilder.append("?, ".repeat(columns.size()));
                    sqlBuilder.setLength(sqlBuilder.length() - 2);
                    sqlBuilder.append(")");

                    String sql = sqlBuilder.toString();
                    List<Object> values = new ArrayList<>();
                    try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                        for (int i = 0; i < columns.size(); i++) {
                            ColumnMetadata column = columns.get(i);
                            Object value = insertContext.getEntries().get(column);
                            preparedStatement.setObject(i + 1, value);
                            values.add(value);
                        }
                        logger.debug("[H2] {}", sql);
                        sqlStatements.add(new SQlStatement(sql, values));
                        preparedStatement.executeUpdate();
                    }
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
                    }
                }
            });

            if (insertMode == InsertMode.SYNC) {
                try {
                    future.join();
                } catch (CompletionException e) {
                    connection.rollback();
                }
            }
        } finally {
            if (autoCommit) {
                connection.setAutoCommit(true);
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
    public void runDDL(String sql) {
        taskQueue.submitTask(connection -> {
            logger.debug("[DB] {}", sql);
            connection.createStatement().execute(sql);
            try (Statement statement = getConnection().createStatement()) {
                logger.trace("[H2] {}", sql);
                statement.execute(sql);
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
            while (rs.next()) {
                String schema = rs.getString("TABLE_SCHEMA");
                String table = rs.getString("TABLE_NAME");
                currentTables.add(schema + "." + table);

                if (!knownTables.contains(schema + "." + table)) {
                    logger.trace("Discovered new table {}.{}", schema, table);
                    UUID randomId = UUID.randomUUID();
                    @Language("SQL") String sql = "CREATE TRIGGER IF NOT EXISTS \"trg_%s_%s\" AFTER INSERT, UPDATE, DELETE ON \"%s\".\"%s\" FOR EACH ROW CALL '%s'";

                    try (Statement createTrigger = connection.createStatement()) {
                        H2Trigger.registerDataManager(randomId, dataManager);
                        createTrigger.execute(sql.formatted(table, randomId.toString().replace('-', '_'), schema, table, H2Trigger.class.getName()));
                    }

                    dataManager.submitBlockingTask(realDbConnection -> postgresListener.ensureTableHasTrigger(realDbConnection, schema, table));
                    sync(schema, table);
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
}


//todo: maintain a buffer of what to send the the real db, and then collapse similar prepared statements into one so we can batch them. have a configurable interval to flush, but by default this will be 0ms

