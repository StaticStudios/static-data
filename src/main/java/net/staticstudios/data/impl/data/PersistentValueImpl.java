package net.staticstudios.data.impl.data;

import com.google.common.base.Preconditions;
import net.staticstudios.data.*;
import net.staticstudios.data.util.*;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PersistentValueImpl<T> implements PersistentValue<T> {
    private final DataAccessor dataAccessor;
    private final UniqueData holder;
    private final Class<T> dataType;
    private final String schema;
    private final String table;
    private final String column;
    private final Map<String, String> idColumnLinks;

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
                columnMetadata = new ColumnMetadata(
                        schema,
                        table,
                        ValueUtils.parseValue(idColumn.name()),
                        ReflectionUtils.getGenericType(pair.field()),
                        false,
                        false,
                        ""
                );
            } else if (columnAnnotation != null) {
                columnMetadata = new ColumnMetadata(
                        columnAnnotation.schema().isEmpty() ? schema : ValueUtils.parseValue(columnAnnotation.schema()),
                        columnAnnotation.table().isEmpty() ? table : ValueUtils.parseValue(columnAnnotation.table()),
                        ValueUtils.parseValue(columnAnnotation.name()),
                        ReflectionUtils.getGenericType(pair.field()),
                        columnAnnotation.nullable(),
                        columnAnnotation.index(),
                        columnAnnotation.defaultValue()
                );
            } else if (foreignColumn != null) {
                columnMetadata = new ColumnMetadata(
                        foreignColumn.schema().isEmpty() ? schema : ValueUtils.parseValue(foreignColumn.schema()),
                        foreignColumn.table().isEmpty() ? table : ValueUtils.parseValue(foreignColumn.table()),
                        ValueUtils.parseValue(foreignColumn.name()),
                        ReflectionUtils.getGenericType(pair.field()),
                        foreignColumn.nullable(),
                        foreignColumn.index(),
                        foreignColumn.defaultValue()
                );
                idColumnLinks = new HashMap<>();
                List<String> links = StringUtils.parseCommaSeperatedList(foreignColumn.link());
                for (String link : links) {
                    String[] parts = link.split("=");
                    Preconditions.checkArgument(parts.length == 2, "ForeignColumn link must be in the format localColumn=foreignColumn, got: %s", link);
                    idColumnLinks.put(ValueUtils.parseValue(parts[0]), ValueUtils.parseValue(parts[1]));
                }
            }
            Preconditions.checkNotNull(columnMetadata, "PersistentValue field %s is missing @Column annotation", pair.field().getName());

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

    @Override
    public <U extends UniqueData> PersistentValue<T> onUpdate(Class<U> holderClass, ValueUpdateHandler<U, T> updateHandler) {
        throw new UnsupportedOperationException("Dynamically adding update handlers is not supported");
    }

    @Override
    public Map<String, String> getIdColumnLinks() {
        return idColumnLinks;
    }

    @Override
    public T get() {
        Preconditions.checkArgument(!holder.isDeleted(), "Cannot get value from a deleted UniqueData instance");
        return holder.getDataManager().get(schema, table, column, holder.getIdColumns(), idColumnLinks, dataType);
    }

    @Override
    public void set(T value) {
        Preconditions.checkArgument(!holder.isDeleted(), "Cannot set value on a deleted UniqueData instance");
        holder.getDataManager().set(schema, table, column, holder.getIdColumns(), idColumnLinks, value);
    }

    //todo: support set with SetMode, or operationMode (SYNC vs ASYNC)
}
