package net.staticstudios.data.impl.data;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import net.staticstudios.data.DataAccessor;
import net.staticstudios.data.PersistentValue;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.Column;
import net.staticstudios.data.ForeignColumn;
import net.staticstudios.data.IdColumn;
import net.staticstudios.data.util.*;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class PersistentValueImpl<T> implements PersistentValue<T> {
    private final DataAccessor dataAccessor;
    private final UniqueData holder;
    private final Class<T> dataType;
    private final String schema;
    private final String table;
    private final String column;
    //    private final Deque<ValueUpdateHandler<T>> updateHandlers = new ConcurrentLinkedDeque<>();
    private Map<String, String> idColumnLinks;
    private @Nullable Supplier<@Nullable T> defaultValueSupplier;

    private PersistentValueImpl(DataAccessor dataAccessor, UniqueData holder, Class<T> dataType, String schema, String table, String column, Map<String, String> idColumnLinks) {
        this.dataAccessor = dataAccessor;
        this.holder = holder;
        this.dataType = dataType;
        this.schema = schema;
        this.table = table;
        this.column = column;
        this.idColumnLinks = idColumnLinks;
    }

    public static <T> void createAndDelegate(ProxyPersistentValue<T> proxy, ColumnMetadata columnMetadata) {
        PersistentValueImpl<T> delegate = new PersistentValueImpl<>(
                proxy.getHolder().getDataManager().getDataAccessor(),
                proxy.getHolder(),
                proxy.getDataType(),
                columnMetadata.schema(),
                columnMetadata.table(),
                columnMetadata.name(),
                proxy.getIdColumnLinks()
        );

        proxy.setDelegate(columnMetadata, delegate);
    }

    public static <T> PersistentValueImpl<T> create(DataAccessor dataAccessor, UniqueData holder, Class<T> dataType, String schema, String table, String column, Map<String, String> idColumnLinks) {
        return new PersistentValueImpl<>(dataAccessor, holder, dataType, schema, table, column, idColumnLinks);
    }

    public static <T extends UniqueData> void delegate(String schema, String table, T instance) {
        for (FieldInstancePair<@Nullable PersistentValue> pair : ReflectionUtils.getFieldInstancePairs(instance, PersistentValue.class)) {
            IdColumn idColumn = pair.field().getAnnotation(IdColumn.class);
            Column columnAnnotation = pair.field().getAnnotation(Column.class);
            ForeignColumn foreignColumn = pair.field().getAnnotation(ForeignColumn.class);
            ColumnMetadata columnMetadata = null;
            Map<String, String> idColumnLinks = Collections.emptyMap();
            if (idColumn != null) {
                Preconditions.checkArgument(columnAnnotation == null, "PersistentValue field %s cannot be annotated with both @IdColumn and @Column", pair.field().getName());
                Preconditions.checkArgument(foreignColumn == null, "PersistentValue field %s cannot be annotated with both @IdColumn and @ForeignColumn", pair.field().getName());
                columnMetadata = new ColumnMetadata(ValueUtils.parseValue(idColumn.name()), ReflectionUtils.getGenericType(pair.field()), false, false, table, schema);
            } else if (columnAnnotation != null) {
                columnMetadata = new ColumnMetadata(ValueUtils.parseValue(columnAnnotation.name()), ReflectionUtils.getGenericType(pair.field()), columnAnnotation.nullable(), columnAnnotation.index(),
                        columnAnnotation.table().isEmpty() ? table : ValueUtils.parseValue(columnAnnotation.table()),
                        columnAnnotation.schema().isEmpty() ? schema : ValueUtils.parseValue(columnAnnotation.schema()));
            } else if (foreignColumn != null) {
                columnMetadata = new ColumnMetadata(ValueUtils.parseValue(foreignColumn.name()), ReflectionUtils.getGenericType(pair.field()), foreignColumn.nullable(), foreignColumn.index(),
                        foreignColumn.table().isEmpty() ? table : ValueUtils.parseValue(foreignColumn.table()),
                        foreignColumn.schema().isEmpty() ? schema : ValueUtils.parseValue(foreignColumn.schema()));
                idColumnLinks = new HashMap<>();
                List<String> links = StringUtils.parseCommaSeperatedList(foreignColumn.link());
                for (String link : links) {
                    String[] parts = link.split("=");
                    Preconditions.checkArgument(parts.length == 2, "ForeignColumn link must be in the format localColumn=foreignColumn, got: %s", link);
                    idColumnLinks.put(ValueUtils.parseValue(parts[0]), ValueUtils.parseValue(parts[1]));
                }
            }
            Preconditions.checkNotNull(columnMetadata, "PersistentValue field %s is missing @Column annotation", pair.field().getName());

            //todo: the primary key gets a bit more complicated when we are dealing with a foreign key. this needs to be handled, and a new ForeignKey created which properly maps my id column to the foreign key column.
            //todo: update: what???

            if (pair.instance() instanceof PersistentValue.ProxyPersistentValue<?> proxyPv) {
                PersistentValueImpl.createAndDelegate(proxyPv, columnMetadata);
            } else {
                pair.field().setAccessible(true);
                try {
                    pair.field().set(instance, PersistentValueImpl.create(instance.getDataManager().getDataAccessor(), instance, pair.field().getType(), columnMetadata.schema(), columnMetadata.table(), columnMetadata.name(), idColumnLinks));
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public UniqueData getHolder() {
        return holder;
    }

    @Override
    public Class<T> getDataType() {
        return dataType;
    }

//    @Override
//    public PersistentValue<T> onUpdate(ValueUpdateHandler<T> updateHandler) {
//        updateHandlers.add(updateHandler);
//        return this;
//    }


    @Override
    public <U extends UniqueData> PersistentValue<T> onUpdate(Class<U> holderClass, ValueUpdateHandler<U, T> updateHandler) {
        throw new UnsupportedOperationException("Dynamically adding update handlers is not supported");
    }

    @Override
    public PersistentValue<T> withDefault(@Nullable T defaultValue) {
        return withDefault(() -> defaultValue);
    }

    @Override
    public PersistentValue<T> withDefault(@Nullable Supplier<@Nullable T> defaultValueSupplier) {
        this.defaultValueSupplier = defaultValueSupplier;
        return this;
    }

    @Override
    public Map<String, String> getIdColumnLinks() {
        return idColumnLinks;
    }

    @Override
    public T get() {
        StringBuilder sqlBuilder = new StringBuilder().append("SELECT \"").append(column).append("\" FROM \"").append(schema).append("\".\"").append(table).append("\" WHERE ");
        for (ColumnValuePair columnValuePair : holder.getIdColumns()) {
            String name = idColumnLinks.getOrDefault(columnValuePair.column(), columnValuePair.column());
            sqlBuilder.append("\"").append(name).append("\" = ? AND ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 5);
        @Language("SQL") String sql = sqlBuilder.toString();
        try (ResultSet rs = dataAccessor.executeQuery(sql, holder.getIdColumns().stream().map(ColumnValuePair::value).toList())) {
            Object serializedValue = null;
            if (rs.next()) {
                serializedValue = rs.getObject(column);
            }
            if (serializedValue != null) {
                T deserialized = (T) serializedValue; //todo: this
                return deserialized;
            }
            if (defaultValueSupplier != null) {
                return defaultValueSupplier.get();
            }
            return null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void set(T value) {
        //todo: whenever we set an id column of something, we need to tell the datamanager to update any tracked instance of uniquedata with that id.
        T oldValue = get();
        StringBuilder sqlBuilder;
        if (idColumnLinks.isEmpty()) {
            sqlBuilder = new StringBuilder().append("UPDATE \"").append(schema).append("\".\"").append(table).append("\" SET \"").append(column).append("\" = ? WHERE ");
            for (ColumnValuePair columnValuePair : holder.getIdColumns()) {
                String name = idColumnLinks.getOrDefault(columnValuePair.column(), columnValuePair.column());
                sqlBuilder.append("\"").append(name).append("\" = ? AND ");
            }
            sqlBuilder.setLength(sqlBuilder.length() - 5);
        } else { // we're dealing with a foreign key
            sqlBuilder = new StringBuilder().append("MERGE INTO \"").append(schema).append("\".\"").append(table).append("\" target USING (VALUES (?");
            sqlBuilder.append(", ?".repeat(holder.getIdColumns().getPairs().length));
            sqlBuilder.append(")) AS source (\"").append(column).append("\"");
            for (ColumnValuePair columnValuePair : holder.getIdColumns()) {
                String name = idColumnLinks.getOrDefault(columnValuePair.column(), columnValuePair.column());
                sqlBuilder.append(", \"").append(name).append("\"");
            }
            sqlBuilder.append(") ON ");
            for (ColumnValuePair columnValuePair : holder.getIdColumns()) {
                String name = idColumnLinks.getOrDefault(columnValuePair.column(), columnValuePair.column());
                sqlBuilder.append("target.\"").append(name).append("\" = source.\"").append(name).append("\" AND ");
            }
            sqlBuilder.setLength(sqlBuilder.length() - 5);
            sqlBuilder.append(" WHEN MATCHED THEN UPDATE SET \"").append(column).append("\" = source.\"").append(column).append("\" WHEN NOT MATCHED THEN INSERT (\"").append(column).append("\"");
            for (ColumnValuePair columnValuePair : holder.getIdColumns()) {
                String name = idColumnLinks.getOrDefault(columnValuePair.column(), columnValuePair.column());
                sqlBuilder.append(", \"").append(name).append("\"");
            }
            sqlBuilder.append(") VALUES (source.\"").append(column).append("\"");
            for (ColumnValuePair columnValuePair : holder.getIdColumns()) {
                String name = idColumnLinks.getOrDefault(columnValuePair.column(), columnValuePair.column());
                sqlBuilder.append(", source.\"").append(name).append("\"");
            }
            sqlBuilder.append(")");
        }
        @Language("SQL") String sql = sqlBuilder.toString();
        List<Object> values = new ArrayList<>(1 + holder.getIdColumns().getPairs().length);
        values.add(value);
        for (ColumnValuePair columnValuePair : holder.getIdColumns()) {
            values.add(columnValuePair.value());
        }
        try {
            dataAccessor.executeUpdate(sql, values);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    //todo: support set with SetMode, or operationMode (SYNC vs ASYNC)
}
