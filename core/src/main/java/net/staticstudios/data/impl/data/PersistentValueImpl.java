package net.staticstudios.data.impl.data;

import com.google.common.base.Preconditions;
import net.staticstudios.data.*;
import net.staticstudios.data.parse.ForeignKey;
import net.staticstudios.data.util.*;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Field;
import java.util.*;

public class PersistentValueImpl<T> implements PersistentValue<T> {
    private final UniqueData holder;
    private final Class<T> dataType;
    private final PersistentValueMetadata metadata;

    private PersistentValueImpl(UniqueData holder, Class<T> dataType, PersistentValueMetadata metadata) {
        this.holder = holder;
        this.dataType = dataType;
        this.metadata = metadata;
    }

    public static <T> void createAndDelegate(ProxyPersistentValue<T> proxy, PersistentValueMetadata metadata) {
        PersistentValueImpl<T> delegate = new PersistentValueImpl<>(
                proxy.getHolder(),
                proxy.getDataType(),
                metadata
        );

        proxy.setDelegate(metadata, delegate);
    }

    public static <T> PersistentValueImpl<T> create(UniqueData holder, Class<T> dataType, PersistentValueMetadata metadata) {
        return new PersistentValueImpl<>(holder, dataType, metadata);
    }

    public static <T extends UniqueData> void delegate(T instance) {
        UniqueDataMetadata metadata = instance.getDataManager().getMetadata(instance.getClass());
        for (FieldInstancePair<@Nullable PersistentValue> pair : ReflectionUtils.getFieldInstancePairs(instance, PersistentValue.class)) {
            PersistentValueMetadata pvMetadata = metadata.persistentValueMetadata().get(pair.field());
            if (pair.instance() instanceof PersistentValue.ProxyPersistentValue<?> proxyPv) {
                PersistentValueImpl.createAndDelegate(proxyPv, pvMetadata);
            } else {
                pair.field().setAccessible(true);
                try {
                    pair.field().set(instance, PersistentValueImpl.create(instance, ReflectionUtils.getGenericType(pair.field()), pvMetadata));
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static <T extends UniqueData> Map<Field, PersistentValueMetadata> extractMetadata(String schema, String table, Class<T> clazz) {
        Map<Field, PersistentValueMetadata> metadataMap = new HashMap<>();
        for (Field field : ReflectionUtils.getFields(clazz, PersistentValue.class)) {
            metadataMap.put(field, extractMetadata(schema, table, clazz, field));
        }
        return metadataMap;
    }

    public static <T extends UniqueData> PersistentValueMetadata extractMetadata(String schema, String table, Class<T> clazz, Field field) {
        IdColumn idColumn = field.getAnnotation(IdColumn.class);
        Column columnAnnotation = field.getAnnotation(Column.class);
        ForeignColumn foreignColumn = field.getAnnotation(ForeignColumn.class);
        UpdateInterval updateIntervalAnnotation = field.getAnnotation(UpdateInterval.class);
        DefaultValue defaultValueAnnotation = field.getAnnotation(DefaultValue.class);
        String defaultValue = defaultValueAnnotation != null ? defaultValueAnnotation.value() : "";
        int updateInterval = updateIntervalAnnotation != null ? updateIntervalAnnotation.value() : 0;
        if (idColumn != null) {
            Preconditions.checkArgument(columnAnnotation == null, "PersistentValue field %s cannot be annotated with both @IdColumn and @Column", field.getName());
            Preconditions.checkArgument(foreignColumn == null, "PersistentValue field %s cannot be annotated with both @IdColumn and @ForeignColumn", field.getName());
            ColumnMetadata columnMetadata = new ColumnMetadata(
                    schema,
                    table,
                    ValueUtils.parseValue(idColumn.name()),
                    ReflectionUtils.getGenericType(field),
                    false,
                    false,
                    ""
            );
            return new PersistentValueMetadata(clazz, columnMetadata, updateInterval);
        }
        if (columnAnnotation != null) {
            ColumnMetadata columnMetadata = new ColumnMetadata(
                    columnAnnotation.schema().isEmpty() ? schema : ValueUtils.parseValue(columnAnnotation.schema()),
                    columnAnnotation.table().isEmpty() ? table : ValueUtils.parseValue(columnAnnotation.table()),
                    ValueUtils.parseValue(columnAnnotation.name()),
                    ReflectionUtils.getGenericType(field),
                    columnAnnotation.nullable(),
                    columnAnnotation.index(),
                    defaultValue
            );
            return new PersistentValueMetadata(clazz, columnMetadata, updateInterval);
        }
        if (foreignColumn != null) {
            ColumnMetadata columnMetadata = new ColumnMetadata(
                    foreignColumn.schema().isEmpty() ? schema : ValueUtils.parseValue(foreignColumn.schema()),
                    foreignColumn.table().isEmpty() ? table : ValueUtils.parseValue(foreignColumn.table()),
                    ValueUtils.parseValue(foreignColumn.name()),
                    ReflectionUtils.getGenericType(field),
                    foreignColumn.nullable(),
                    foreignColumn.index(),
                    defaultValue
            );
            List<ForeignKey.Link> idColumnLinks = new LinkedList<>();
            List<String> links = StringUtils.parseCommaSeperatedList(foreignColumn.link());
            for (String link : links) {
                String[] parts = link.split("=");
                Preconditions.checkArgument(parts.length == 2, "ForeignColumn link must be in the format localColumn=foreignColumn, got: %s", link);
                idColumnLinks.add(new ForeignKey.Link(ValueUtils.parseValue(parts[1]), ValueUtils.parseValue(parts[0])));
            }
            return new ForeignPersistentValueMetadata(clazz, columnMetadata, updateInterval, idColumnLinks);
        }

        throw new IllegalStateException("PersistentValue field %s is missing @Column annotation".formatted(field.getName()));
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
    public T get() {
        Preconditions.checkArgument(!holder.isDeleted(), "Cannot get value from a deleted UniqueData instance");
        return holder.getDataManager().get(metadata.getSchema(), metadata.getTable(), metadata.getColumn(), holder.getIdColumns(), getIdColumnLinks(), dataType);
    }

    @Override
    public void set(T value) {
        Preconditions.checkArgument(!holder.isDeleted(), "Cannot set value on a deleted UniqueData instance");
        holder.getDataManager().set(metadata.getSchema(), metadata.getTable(), metadata.getColumn(), holder.getIdColumns(), getIdColumnLinks(), value, metadata.getUpdateInterval());
    }

    private List<ForeignKey.Link> getIdColumnLinks() {
        if (metadata instanceof ForeignPersistentValueMetadata foreignMetadata) {
            return foreignMetadata.getLinks();
        }
        return Collections.emptyList();
    }

    @Override
    public String toString() {
        if (holder.isDeleted()) {
            return "[DELETED]";
        }
        return String.valueOf(get());
    }
}
