package net.staticstudios.data;

import net.staticstudios.data.util.PrimaryKey;
import org.intellij.lang.annotations.Language;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface DataAccessor {
    PreparedStatement prepareStatement(@Language("SQL") String sql) throws SQLException;

    <T> PersistentValue<T> createPersistentValue(PrimaryKey primaryKey, Class<T> dataType, String schema, String table, String dataColumn);

    void insertIntoCache(UniqueData uniqueData) throws SQLException;
}
