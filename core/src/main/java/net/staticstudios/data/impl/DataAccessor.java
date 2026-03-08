package net.staticstudios.data.impl;

import net.staticstudios.data.InsertMode;
import net.staticstudios.data.StaticDataStatistics;
import net.staticstudios.data.parse.DDLStatement;
import net.staticstudios.data.util.SQLTransaction;
import net.staticstudios.data.util.SQlStatement;
import net.staticstudios.data.util.redis.RedisIdentifier;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

public interface DataAccessor {

    ResultSet executeQuery(@Language("SQL") String sql, List<Object> values) throws SQLException;

    default void executeUpdate(SQLTransaction.Statement statement, List<Object> values, int delay) throws SQLException {
        executeTransaction(new SQLTransaction().update(statement, values), delay);
    }

    void executeTransaction(SQLTransaction transaction, int delay) throws SQLException;

    void insert(List<SQlStatement> sqlStatements, InsertMode insertMode) throws SQLException;

    void runDDL(DDLStatement ddl) throws SQLException;

    void postDDL() throws SQLException;

    @Nullable String getRedisValue(RedisIdentifier identifier);

    void setRedisValue(RedisIdentifier identifier, String value, int expirationSeconds);

    void discoverRedisKeys(List<String> partialRedisKeys);

    void resync();

    Optional<StaticDataStatistics> getStatistics();
}
