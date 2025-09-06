package net.staticstudios.data.impl.sqlite;

import com.google.common.base.Supplier;
import net.staticstudios.data.PersistentValue;
import net.staticstudios.data.util.PrimaryKey;
import net.staticstudios.data.util.ValueUpdateHandler;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.Nullable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SQLitePersistentValue<T> implements PersistentValue<T> {
    private final SQLiteDataAccessor dataAccessor;
    private final PrimaryKey primaryKey;
    private final Class<T> dataType;
    private final String schema;
    private final String table;
    private final String dataColumn;

    //todo: all methods should update the real db
    public SQLitePersistentValue(SQLiteDataAccessor dataAccessor, PrimaryKey primaryKey, Class<T> dataType, String schema, String table, String dataColumn) {
        this.dataAccessor = dataAccessor;
        this.primaryKey = primaryKey;
        this.dataType = dataType;
        this.schema = schema;
        this.table = table;
        this.dataColumn = dataColumn;
    }

    @Override
    public String getSchema() {
        return schema;
    }

    @Override
    public String getTable() {
        return table;
    }

    @Override
    public String getColumn() {
        return dataColumn;
    }

    @Override
    public PersistentValue<T> onUpdate(ValueUpdateHandler<T> updateHandler) {
        return null;
    }

    @Override
    public PersistentValue<T> withDefault(@Nullable T defaultValue) {
        return null;
    }

    @Override
    public PersistentValue<T> withDefault(@Nullable Supplier<@Nullable T> defaultValueSupplier) {
        return null;
    }

    @Override
    public T get() {
        try {
            StringBuilder sqlBuilder = new StringBuilder("SELECT " + dataColumn + " FROM " + schema + "_" + table + " WHERE ");
            for (PrimaryKey.ColumnValuePair pair : primaryKey.getWhereClause()) {
                sqlBuilder.append(pair.column()).append(" = ? AND ");
            }

            sqlBuilder.setLength(sqlBuilder.length() - 5);
            @Language("SQL") String sql = sqlBuilder.toString();
            PreparedStatement preparedStatement = dataAccessor.prepareStatement(sql);

            for (int i = 0; i < primaryKey.getWhereClause().size(); i++) {
                PrimaryKey.ColumnValuePair pair = primaryKey.getWhereClause().get(i);
                preparedStatement.setObject(i + 1, pair.value());
            }

            ResultSet rs = preparedStatement.executeQuery();
            Object rawValue = null;
            if (rs.next()) {
                rawValue = rs.getObject(dataColumn);
            }
            if (rawValue == null) {
                return null;
            }
            //todo: deserialize
            return (T) rawValue;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void set(T value) {
        try {
            Object serializedValue = value; //todo: serialize
            StringBuilder sqlBuilder = new StringBuilder("INSERT INTO " + schema + "_" + table + " (");
            for (PrimaryKey.ColumnValuePair pair : primaryKey.getWhereClause()) {
                sqlBuilder.append(pair.column()).append(", ");
            }
            sqlBuilder.append(dataColumn).append(") VALUES (");
            sqlBuilder.append("?, ".repeat(primaryKey.getWhereClause().size()));
            sqlBuilder.append("?) ON CONFLICT(");
            for (PrimaryKey.ColumnValuePair pair : primaryKey.getWhereClause()) {
                sqlBuilder.append(pair.column()).append(", ");
            }
            sqlBuilder.setLength(sqlBuilder.length() - 2);
            sqlBuilder.append(") DO UPDATE SET ").append(dataColumn).append(" = excluded.").append(dataColumn);
            @Language("SQL") String sql = sqlBuilder.toString();
            PreparedStatement preparedStatement = dataAccessor.prepareStatement(sql);
            int index = 1;
            for (PrimaryKey.ColumnValuePair pair : primaryKey.getWhereClause()) {
                preparedStatement.setObject(index++, pair.value());
            }
            preparedStatement.setObject(index, serializedValue);

            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
