package net.staticstudios.data.impl.data;

import com.google.common.base.Preconditions;
import net.staticstudios.data.*;
import net.staticstudios.data.parse.SQLBuilder;
import net.staticstudios.data.util.*;
import net.staticstudios.data.utils.Link;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class PersistentOneToManyValueCollectionImpl<T> implements PersistentCollection<T> {
    private final UniqueData holder;
    private final Class<T> type;
    private final String dataSchema;
    private final String dataTable;
    private final String dataColumn;
    private final List<Link> link;

    public PersistentOneToManyValueCollectionImpl(UniqueData holder, Class<T> type, String dataSchema, String dataTable, String dataColumn, List<Link> link) {
        this.holder = holder;
        this.type = type;
        this.dataSchema = dataSchema;
        this.dataTable = dataTable;
        this.dataColumn = dataColumn;
        this.link = link;
    }

    public static <T> void createAndDelegate(ProxyPersistentCollection<T> proxy, String dataSchema, String dataTable, String dataColumn, List<Link> link) {
        PersistentOneToManyValueCollectionImpl<T> delegate = new PersistentOneToManyValueCollectionImpl<>(
                proxy.getHolder(),
                proxy.getReferenceType(),
                dataSchema,
                dataTable,
                dataColumn,
                link
        );
        proxy.setDelegate(delegate);
    }

    public static <T> PersistentOneToManyValueCollectionImpl<T> create(UniqueData holder, Class<T> type, String dataSchema, String dataTable, String dataColumn, List<Link> link) {
        return new PersistentOneToManyValueCollectionImpl<>(holder, type, dataSchema, dataTable, dataColumn, link);
    }

    public static <T extends UniqueData> void delegate(T instance) {
        UniqueDataMetadata metadata = instance.getDataManager().getMetadata(instance.getClass());
        for (FieldInstancePair<@Nullable PersistentCollection> pair : ReflectionUtils.getFieldInstancePairs(instance, PersistentCollection.class)) {
            PersistentCollectionMetadata collectionMetadata = metadata.persistentCollectionMetadata().get(pair.field());
            if (!(collectionMetadata instanceof PersistentOneToManyValueCollectionMetadata oneToManyValueMetadata))
                continue;

            if (pair.instance() instanceof PersistentCollection.ProxyPersistentCollection<?> proxyCollection) {
                createAndDelegate((ProxyPersistentCollection<?>) proxyCollection,
                        oneToManyValueMetadata.getDataSchema(),
                        oneToManyValueMetadata.getDataTable(),
                        oneToManyValueMetadata.getDataColumn(),
                        oneToManyValueMetadata.getLinks()
                );
            } else {
                pair.field().setAccessible(true);
                try {
                    pair.field().set(instance, create(instance,
                            oneToManyValueMetadata.getDataType(),
                            oneToManyValueMetadata.getDataSchema(),
                            oneToManyValueMetadata.getDataTable(),
                            oneToManyValueMetadata.getDataColumn(),
                            oneToManyValueMetadata.getLinks()
                    ));
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static <T extends UniqueData> Map<Field, PersistentOneToManyValueCollectionMetadata> extractMetadata(Class<T> clazz, String holderSchema) {
        Map<Field, PersistentOneToManyValueCollectionMetadata> metadataMap = new HashMap<>();
        for (Field field : ReflectionUtils.getFields(clazz, PersistentCollection.class)) {
            OneToMany oneToManyAnnotation = field.getAnnotation(OneToMany.class);
            if (oneToManyAnnotation == null) continue;
            Class<?> genericType = ReflectionUtils.getGenericType(field);
            if (genericType == null || UniqueData.class.isAssignableFrom(genericType)) continue;
            String schema = oneToManyAnnotation.schema();
            if (schema.isEmpty()) {
                schema = holderSchema;
            }
            schema = ValueUtils.parseValue(schema);
            String table = ValueUtils.parseValue(oneToManyAnnotation.table());
            String column = ValueUtils.parseValue(oneToManyAnnotation.column());
            Preconditions.checkArgument(!schema.isEmpty(), "OneToMany PersistentCollection field %s in class %s must specify a schema name since the data type %s does not extend UniqueData", field.getName(), clazz.getName(), genericType.getName());
            Preconditions.checkArgument(!table.isEmpty(), "OneToMany PersistentCollection field %s in class %s must specify a table name since the data type %s does not extend UniqueData", field.getName(), clazz.getName(), genericType.getName());
            metadataMap.put(field, new PersistentOneToManyValueCollectionMetadata(genericType, schema, table, column, SQLBuilder.parseLinks(oneToManyAnnotation.link())));
        }

        return metadataMap;
    }


    @Override
    public UniqueData getHolder() {
        return holder;
    }

    @Override
    public int size() {
        return getValues().size();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(Object o) {
        return containsAll(Collections.singleton(o));
    }

    @Override
    public @NotNull Iterator<T> iterator() {
        return new IteratorImpl(getValues());
    }

    @Override
    public @NotNull Object @NotNull [] toArray() {
        return getValues().toArray();
    }

    @Override
    public @NotNull <T1> T1 @NotNull [] toArray(@NotNull T1 @NotNull [] a) {
        return getValues().toArray(a);
    }

    @Override
    public boolean add(T t) {
        return addAll(Collections.singleton(t));
    }

    @Override
    public boolean remove(Object o) {
        return removeAll(Collections.singleton(o));
    }

    @Override
    public boolean containsAll(@NotNull Collection<?> c) {
        for (Object o : c) {
            if (!type.isInstance(o)) {
                return false;
            }
        }
        return getValues().containsAll(c);
    }

    @Override
    public boolean addAll(@NotNull Collection<? extends T> c) {
        Preconditions.checkArgument(!holder.isDeleted(), "Cannot set entries on a deleted UniqueData instance");
        if (c.isEmpty()) {
            return false;
        }
        DataAccessor dataAccessor = holder.getDataManager().getDataAccessor();

        SQLTransaction.Statement selectDataIdsStatement = buildSelectDataIdsStatement();
        SQLTransaction.Statement insertStatement = buildInsertStatement();


        List<Object> holderLinkingValues = new ArrayList<>(link.size());
        List<Object> holderIdValues = holder.getIdColumns().stream().map(ColumnValuePair::value).toList();

        SQLTransaction transaction = new SQLTransaction();
        transaction.query(selectDataIdsStatement, () -> holderIdValues, rs -> {
            try {
                Preconditions.checkState(rs.next(), "Could not find holder row in database");
                for (Link entry : link) {
                    String dataColumn = entry.columnInReferringTable();
                    Object value = rs.getObject(dataColumn);
                    holderLinkingValues.add(value);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        for (T entry : c) {
            transaction.update(insertStatement, () -> {
                List<Object> values = new ArrayList<>(holderLinkingValues);
                values.add(entry);
                return values;
            });
        }
        try {
            dataAccessor.executeTransaction(transaction, 0);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return true;
    }

    @Override
    public boolean removeAll(@NotNull Collection<?> c) {
        List<T> toRemove = new ArrayList<>();
        for (Object o : c) {
            if (!type.isInstance(o)) {
                continue;
            }
            T data = type.cast(o);
            toRemove.add(data);
        }
        removeValues(toRemove);

        return !toRemove.isEmpty();
    }

    @Override
    public boolean retainAll(@NotNull Collection<?> c) {
        Set<T> currentValues = getValues();
        Set<T> valuesToRetain = new HashSet<>();
        for (Object o : c) {
            if (!type.isInstance(o)) {
                continue;
            }
            T data = type.cast(o);
            valuesToRetain.add(data);
        }

        List<T> valuesToRemove = new ArrayList<>();
        for (T value : currentValues) {
            boolean found = false;
            for (T retainValue : valuesToRetain) {
                if (Objects.equals(value, retainValue)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                valuesToRemove.add(value);
            }
        }

        if (!valuesToRemove.isEmpty()) {
            removeValues(valuesToRemove);
            return true;
        }

        return false;
    }

    @Override
    public void clear() {
        Preconditions.checkArgument(!holder.isDeleted(), "Cannot clear entries on a deleted UniqueData instance");
        DataAccessor dataAccessor = holder.getDataManager().getDataAccessor();

        SQLTransaction.Statement selectDataIdsStatement = buildSelectDataIdsStatement();
        SQLTransaction.Statement clearStatement = buildClearStatement();


        List<Object> holderLinkingValues = new ArrayList<>(link.size());
        List<Object> holderIdValues = holder.getIdColumns().stream().map(ColumnValuePair::value).toList();

        SQLTransaction transaction = new SQLTransaction();
        transaction.query(selectDataIdsStatement, () -> holderIdValues, rs -> {
            try {
                Preconditions.checkState(rs.next(), "Could not find holder row in database");
                for (Link entry : link) {
                    String dataColumn = entry.columnInReferringTable();
                    Object value = rs.getObject(dataColumn);
                    holderLinkingValues.add(value);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        transaction.update(clearStatement, () -> holderLinkingValues);
        try {
            dataAccessor.executeTransaction(transaction, 0);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void removeValues(Collection<T> values) {
        Preconditions.checkArgument(!holder.isDeleted(), "Cannot set entries on a deleted UniqueData instance");
        if (values.isEmpty()) {
            return;
        }

        DataAccessor dataAccessor = holder.getDataManager().getDataAccessor();

        SQLTransaction.Statement selectDataIdsStatement = buildSelectDataIdsStatement();
        SQLTransaction.Statement removeStatement = buildRemoveStatement();


        List<Object> holderLinkingValues = new ArrayList<>(link.size());
        List<Object> holderIdValues = holder.getIdColumns().stream().map(ColumnValuePair::value).toList();

        SQLTransaction transaction = new SQLTransaction();
        transaction.query(selectDataIdsStatement, () -> holderIdValues, rs -> {
            try {
                Preconditions.checkState(rs.next(), "Could not find holder row in database");
                for (Link entry : link) {
                    String dataColumn = entry.columnInReferringTable();
                    Object value = rs.getObject(dataColumn);
                    holderLinkingValues.add(value);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        for (T value : values) {
            transaction.update(removeStatement, () -> {
                List<Object> statementValues = new ArrayList<>(holderLinkingValues);
                statementValues.add(value);
                return statementValues;
            });
        }
        try {
            dataAccessor.executeTransaction(transaction, 0);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private SQLTransaction.Statement buildSelectDataIdsStatement() {
        UniqueDataMetadata holderMetadata = holder.getMetadata();

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT ");
        for (Link entry : link) {
            String dataColumn = entry.columnInReferringTable();
            sqlBuilder.append("\"").append(dataColumn).append("\", ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 2);
        sqlBuilder.append(" FROM \"").append(holderMetadata.schema()).append("\".\"").append(holderMetadata.table()).append("\" WHERE ");
        for (ColumnValuePair columnValuePair : holder.getIdColumns()) {
            sqlBuilder.append("\"").append(columnValuePair.column()).append("\" = ? AND ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 5);
        @Language("SQL") String sql = sqlBuilder.toString();
        return SQLTransaction.Statement.of(sql, sql);
    }

    private SQLTransaction.Statement buildInsertStatement() {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("INSERT INTO \"").append(dataSchema).append("\".\"").append(dataTable).append("\" (");
        for (Link entry : link) {
            String theirColumn = entry.columnInReferencedTable();
            sqlBuilder.append("\"").append(theirColumn).append("\", ");
        }
        sqlBuilder.append("\"").append(dataColumn).append("\"");
        sqlBuilder.append(") VALUES (");
        sqlBuilder.append("?, ".repeat(link.size() + 1));
        sqlBuilder.setLength(sqlBuilder.length() - 2);
        sqlBuilder.append(")");
        @Language("SQL") String sql = sqlBuilder.toString();
        return SQLTransaction.Statement.of(sql, sql);
    }

    private SQLTransaction.Statement buildRemoveStatement() {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("DELETE FROM \"").append(dataSchema).append("\".\"").append(dataTable).append("\" WHERE ");
        for (Link entry : link) {
            String theirColumn = entry.columnInReferencedTable();
            sqlBuilder.append("\"").append(theirColumn).append("\" = ? AND ");
        }
        sqlBuilder.append("\"").append(dataColumn).append("\" = ? LIMIT 1");
        @Language("SQL") String h2 = sqlBuilder.toString();

        sqlBuilder = new StringBuilder();
        sqlBuilder.append("WITH to_delete AS (")
                .append("SELECT ctid FROM \"").append(dataSchema).append("\".\"").append(dataTable).append("\" WHERE ");

        for (Link entry : link) {
            String theirColumn = entry.columnInReferencedTable();
            sqlBuilder.append("\"").append(theirColumn).append("\" = ? AND ");
        }

        sqlBuilder.append("\"").append(dataColumn).append("\" = ? ")
                .append("LIMIT 1")
                .append(") DELETE FROM \"").append(dataSchema).append("\".\"").append(dataTable)
                .append("\" WHERE ctid IN (SELECT ctid FROM to_delete)");

        @Language("SQL")
        String pg = sqlBuilder.toString();

        return SQLTransaction.Statement.of(h2, pg);
    }

    private SQLTransaction.Statement buildClearStatement() {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("DELETE FROM \"").append(dataSchema).append("\".\"").append(dataTable).append("\" WHERE ");
        for (Link entry : link) {
            String theirColumn = entry.columnInReferencedTable();
            sqlBuilder.append("\"").append(theirColumn).append("\" = ? AND ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 5);
        @Language("SQL") String sql = sqlBuilder.toString();
        return SQLTransaction.Statement.of(sql, sql);
    }

    private Set<T> getValues() {
        // note: we need the join since we support linking on non-id columnsInReferringTable
        Preconditions.checkArgument(!holder.isDeleted(), "Cannot get entries on a deleted UniqueData instance");
        Set<T> values = new HashSet<>();
        UniqueDataMetadata holderMetadata = holder.getMetadata();
        DataAccessor dataAccessor = holder.getDataManager().getDataAccessor();
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT ").append("\"").append(dataColumn).append("\", ");
        for (ColumnMetadata columnMetadata : holderMetadata.idColumns()) {
            sqlBuilder.append("_source.\"").append(columnMetadata.name()).append("\", ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 2);
        sqlBuilder.append(" FROM \"").append(dataSchema).append("\".\"").append(dataTable).append("\" ");
        sqlBuilder.append("INNER JOIN \"").append(holderMetadata.schema()).append("\".\"").append(holderMetadata.table()).append("\" AS _source ON ");
        for (Link entry : link) {
            String myColumn = entry.columnInReferringTable();
            String theirColumn = entry.columnInReferencedTable();
            sqlBuilder.append("\"").append(dataSchema).append("\".\"").append(dataTable).append("\".\"").append(theirColumn).append("\" = _source.\"").append(myColumn).append("\" AND ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 5);
        sqlBuilder.append(" WHERE ");

        for (Link entry : link) {
            String theirColumn = entry.columnInReferencedTable();
            sqlBuilder.append("\"").append(theirColumn).append("\" = _source.\"").append(entry.columnInReferringTable()).append("\" AND ");
        }
        for (ColumnValuePair columnValuePair : holder.getIdColumns()) {
            sqlBuilder.append("_source.\"").append(columnValuePair.column()).append("\" = ? AND ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 5);

        @Language("SQL") String sql = sqlBuilder.toString();
        try (ResultSet rs = dataAccessor.executeQuery(sql, holder.getIdColumns().stream().map(ColumnValuePair::value).toList())) {
            while (rs.next()) {
                Object value = rs.getObject(dataColumn);
                values.add(type.cast(value));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return values;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof PersistentOneToManyValueCollectionImpl<?> that)) return false;
        //due to the nature of how one-to-many collections work, this will == that only if the holder is the same. no need to compare contents.
        return Objects.equals(type, that.type) && Objects.equals(holder, that.holder);
    }

    @Override
    public int hashCode() {
        //due to the nature of how one-to-many collections work, this will == that only if the holder is the same. no need to compare contents.
        //for this reason, we can use just the type and holder for the hashcode.
        return Objects.hash(type, holder);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (T item : this) {
            sb.append(item).append(", ");
        }
        if (!isEmpty()) {
            sb.setLength(sb.length() - 2);
        }
        sb.append("]");
        return sb.toString();
    }

    class IteratorImpl implements Iterator<T> {
        private final List<T> values;
        private int index = 0;

        public IteratorImpl(Set<T> valuesSet) {
            this.values = new ArrayList<>(valuesSet);
        }

        @Override
        public boolean hasNext() {
            return index < values.size();
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return values.get(index++);
        }

        @Override
        public void remove() {
            Preconditions.checkState(index > 0, "next() has not been called yet");
            removeValues(Collections.singletonList(values.get(index - 1)));
            values.remove(--index);
        }
    }
}
