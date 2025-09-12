package net.staticstudios.data;

import net.staticstudios.data.insert.InsertContext;
import org.intellij.lang.annotations.Language;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public interface DataAccessor {
//    PreparedStatement prepareStatement(@Language("SQL") String sql) throws SQLException;

//    <T> PersistentValue<T> createPersistentValue(PrimaryKey primaryKey, Class<T> dataType, String schema, String table, String dataColumn);

    ResultSet executeQuery(@Language("SQL") String sql, List<Object> values) throws SQLException;

    void executeUpdate(@Language("SQL") String sql, List<Object> values) throws SQLException;

    void insert(InsertContext insertContext, InsertMode insertMode) throws SQLException;

    void runDDL(@Language("SQL") String sql) throws SQLException;

    void postDDL() throws SQLException;
}
