package net.staticstudios.data.impl.sqlite;

import net.staticstudios.data.DataAccessor;
import net.staticstudios.data.PersistentValue;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.parse.SQLBuilder;
import net.staticstudios.data.parse.UniqueDataMetadata;
import net.staticstudios.data.util.PrimaryKey;
import net.staticstudios.data.util.ReflectionUtils;
import org.intellij.lang.annotations.Language;
import org.sqlite.SQLiteConfig;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SQLiteDataAccessor implements DataAccessor { //todo: we need data transformers since sqlite only supports a few types natively
    private final SQLBuilder sqlBuilder;
    private final String jdbcUrl;
    private final SQLiteConfig config = new SQLiteConfig();
    private final ThreadLocal<Connection> threadConnection = new ThreadLocal<>();
    private final ThreadLocal<Map<String, PreparedStatement>> threadPreparedStatementCache = new ThreadLocal<>();

    public SQLiteDataAccessor(SQLBuilder sqlBuilder) {
        this.sqlBuilder = sqlBuilder;
        String filePath = "static-data-cache.db";

        File cacheFile = new File(filePath + ".db");
        if (cacheFile.exists()) {
            cacheFile.delete();
        }
        config.setSharedCache(true);
        config.setJournalMode(SQLiteConfig.JournalMode.OFF);
        config.setSynchronous(SQLiteConfig.SynchronousMode.OFF);
        config.setTempStore(SQLiteConfig.TempStore.MEMORY);
        this.jdbcUrl = "jdbc:sqlite:" + filePath;

        //todo: using thread utils delete the file on exit
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

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
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
    public <T> PersistentValue<T> createPersistentValue(PrimaryKey primaryKey, Class<T> dataType, String schema, String table, String dataColumn) {
        return new SQLitePersistentValue<>(this, primaryKey, dataType, schema, table, dataColumn);
    }

    //todo: set & get should be here instead of in the pv impl

    public void insert(Connection connection, UniqueData uniqueData) throws SQLException {
        insertIntoCache(uniqueData);
    }

    @Override
    public void insertIntoCache(UniqueData uniqueData) throws SQLException {
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

            @Language("SQL") String sql = sqlBuilder.toString();
            PreparedStatement preparedStatement = prepareStatement(sql);
            for (int i = 0; i < columnValuePairs.size(); i++) {
                PrimaryKey.ColumnValuePair pair = columnValuePairs.get(i);
                preparedStatement.setObject(i + 1, pair.value());
            }
            preparedStatement.executeUpdate();
        }
    }


//        StringBuilder sqlBuilder = new StringBuilder("INSERT INTO " +
}


//todo: maintain a buffer of what to send the the real db, and then collapse similar prepared statements into one so we can batch them.