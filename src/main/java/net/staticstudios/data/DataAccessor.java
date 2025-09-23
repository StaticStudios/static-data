package net.staticstudios.data;

import net.staticstudios.data.parse.DDLStatement;
import net.staticstudios.data.util.SQlStatement;
import org.intellij.lang.annotations.Language;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public interface DataAccessor {
//    PreparedStatement prepareStatement(@Language("SQL") String sql) throws SQLException;

//    <T> PersistentValue<T> createPersistentValue(PrimaryKey primaryKey, Class<T> dataType, String schema, String table, String dataColumn);

    ResultSet executeQuery(@Language("SQL") String sql, List<Object> values) throws SQLException;

    void executeUpdate(@Language("SQL") String sql, List<Object> values, int delay) throws SQLException;

    void insert(List<SQlStatement> sqlStatements, InsertMode insertMode) throws SQLException;

    void runDDL(DDLStatement ddl) throws SQLException;

    void postDDL() throws SQLException;
}
