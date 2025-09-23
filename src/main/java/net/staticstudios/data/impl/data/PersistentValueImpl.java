package net.staticstudios.data.impl.data;

import com.google.common.base.Preconditions;
import net.staticstudios.data.*;
import net.staticstudios.data.parse.ForeignKey;
import net.staticstudios.data.util.*;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Field;
import java.util.*;

public class PersistentValueImpl<T> implements PersistentValue<T> {
    private final DataAccessor dataAccessor;
    private final UniqueData holder;
    private final Class<T> dataType;
    private final String schema;
    private final String table;
    private final String column;
    private final List<ForeignKey.Link> idColumnLinks;

    private PersistentValueImpl(DataAccessor dataAccessor, UniqueData holder, Class<T> dataType, String schema, String table, String column, List<ForeignKey.Link> idColumnLinks) {
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

    public static <T> PersistentValueImpl<T> create(DataAccessor dataAccessor, UniqueData holder, Class<T> dataType, String schema, String table, String column, List<ForeignKey.Link> idColumnLinks) {
        return new PersistentValueImpl<>(dataAccessor, holder, dataType, schema, table, column, idColumnLinks);
    }

    public static <T extends UniqueData> void delegate(T instance) {
        UniqueDataMetadata metadata = instance.getDataManager().getMetadata(instance.getClass());
        for (FieldInstancePair<@Nullable PersistentValue> pair : ReflectionUtils.getFieldInstancePairs(instance, PersistentValue.class)) {
            PersistentValueMetadata pvMetadata = metadata.persistentValueMetadata().get(pair.field());
            ColumnMetadata columnMetadata = pvMetadata.getColumnMetadata();
            if (pair.instance() instanceof PersistentValue.ProxyPersistentValue<?> proxyPv) {
                PersistentValueImpl.createAndDelegate(proxyPv, columnMetadata);
            } else {
                pair.field().setAccessible(true);
                try {
                    List<ForeignKey.Link> idColumnLinks = Collections.emptyList();
                    if (pvMetadata instanceof ForeignPersistentValueMetadata foreignPvMetadata) {
                        idColumnLinks = foreignPvMetadata.getLinks();
                    }
                    pair.field().set(instance, PersistentValueImpl.create(instance.getDataManager().getDataAccessor(), instance, ReflectionUtils.getGenericType(pair.field()), columnMetadata.schema(), columnMetadata.table(), columnMetadata.name(), idColumnLinks));
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static <T extends UniqueData> Map<Field, PersistentValueMetadata> extractMetadata(String schema, String table, Class<T> clazz) {
        Map<Field, PersistentValueMetadata> metadataMap = new HashMap<>();
        for (Field field : ReflectionUtils.getFields(clazz, PersistentValue.class)) {
            IdColumn idColumn = field.getAnnotation(IdColumn.class);
            Column columnAnnotation = field.getAnnotation(Column.class);
            ForeignColumn foreignColumn = field.getAnnotation(ForeignColumn.class);
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
                metadataMap.put(field, new PersistentValueMetadata(clazz, columnMetadata));
                continue;
            }
            if (columnAnnotation != null) {
                ColumnMetadata columnMetadata = new ColumnMetadata(
                        columnAnnotation.schema().isEmpty() ? schema : ValueUtils.parseValue(columnAnnotation.schema()),
                        columnAnnotation.table().isEmpty() ? table : ValueUtils.parseValue(columnAnnotation.table()),
                        ValueUtils.parseValue(columnAnnotation.name()),
                        ReflectionUtils.getGenericType(field),
                        columnAnnotation.nullable(),
                        columnAnnotation.index(),
                        columnAnnotation.defaultValue()
                );
                metadataMap.put(field, new PersistentValueMetadata(clazz, columnMetadata));
                continue;
            }
            if (foreignColumn != null) {
                ColumnMetadata columnMetadata = new ColumnMetadata(
                        foreignColumn.schema().isEmpty() ? schema : ValueUtils.parseValue(foreignColumn.schema()),
                        foreignColumn.table().isEmpty() ? table : ValueUtils.parseValue(foreignColumn.table()),
                        ValueUtils.parseValue(foreignColumn.name()),
                        ReflectionUtils.getGenericType(field),
                        foreignColumn.nullable(),
                        foreignColumn.index(),
                        foreignColumn.defaultValue()
                );
                List<ForeignKey.Link> idColumnLinks = new LinkedList<>();
                List<String> links = StringUtils.parseCommaSeperatedList(foreignColumn.link());
                for (String link : links) {
                    String[] parts = link.split("=");
                    Preconditions.checkArgument(parts.length == 2, "ForeignColumn link must be in the format localColumn=foreignColumn, got: %s", link);
                    idColumnLinks.add(new ForeignKey.Link(ValueUtils.parseValue(parts[1]), ValueUtils.parseValue(parts[0])));
                }
                metadataMap.put(field, new ForeignPersistentValueMetadata(clazz, columnMetadata, idColumnLinks));
                continue;
            }

            throw new IllegalStateException("PersistentValue field %s is missing @Column annotation".formatted(field.getName()));
        }
        return metadataMap;
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
    public List<ForeignKey.Link> getIdColumnLinks() {
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
        holder.getDataManager().set(schema, table, column, holder.getIdColumns(), idColumnLinks, value, 0); //todo: this delay can be used to throttle frequent updates. allow it to be configured. this also needs to be tested
    }
}
