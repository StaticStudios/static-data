package net.staticstudios.data.impl.data;

import com.google.common.base.Preconditions;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.OneToMany;
import net.staticstudios.data.PersistentCollection;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.impl.DataAccessor;
import net.staticstudios.data.parse.SQLBuilder;
import net.staticstudios.data.util.*;
import net.staticstudios.data.utils.Link;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class PersistentOneToManyCollectionImpl<T extends UniqueData> implements PersistentCollection<T> {
    private final UniqueData holder;
    private final Class<T> type;
    private final List<Link> link;

    public PersistentOneToManyCollectionImpl(UniqueData holder, Class<T> type, List<Link> link) {
        this.holder = holder;
        this.type = type;
        this.link = link;
    }

    public static <T extends UniqueData> void createAndDelegate(PersistentCollection.ProxyPersistentCollection<T> proxy, List<Link> link, PersistentOneToManyCollectionMetadata metadata) {
        PersistentOneToManyCollectionImpl<T> delegate = new PersistentOneToManyCollectionImpl<>(
                proxy.getHolder(),
                proxy.getDataType(),
                link
        );
        proxy.setDelegate(metadata, delegate);
    }

    public static <T extends UniqueData> PersistentOneToManyCollectionImpl<T> create(UniqueData holder, Class<T> type, List<Link> link) {
        return new PersistentOneToManyCollectionImpl<>(holder, type, link);
    }

    public static <T extends UniqueData> void delegate(T instance) {
        UniqueDataMetadata metadata = instance.getDataManager().getMetadata(instance.getClass());
        for (FieldInstancePair<@Nullable PersistentCollection> pair : ReflectionUtils.getFieldInstancePairs(instance, PersistentCollection.class)) {
            PersistentCollectionMetadata collectionMetadata = metadata.persistentCollectionMetadata().get(pair.field());
            if (!(collectionMetadata instanceof PersistentOneToManyCollectionMetadata oneToManyMetadata)) continue;

            if (pair.instance() instanceof PersistentCollection.ProxyPersistentCollection<?> proxyCollection) {
                createAndDelegate((PersistentCollection.ProxyPersistentCollection<? extends UniqueData>) proxyCollection, oneToManyMetadata.getLinks(), oneToManyMetadata);
            } else {
                pair.field().setAccessible(true);
                try {
                    pair.field().set(instance, create(instance, oneToManyMetadata.getReferencedType(), oneToManyMetadata.getLinks()));
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static <T extends UniqueData> Map<Field, PersistentOneToManyCollectionMetadata> extractMetadata(DataManager dataManager, Class<T> clazz) {
        Map<Field, PersistentOneToManyCollectionMetadata> metadataMap = new HashMap<>();
        for (Field field : ReflectionUtils.getFields(clazz, PersistentCollection.class)) {
            OneToMany oneToManyAnnotation = field.getAnnotation(OneToMany.class);
            if (oneToManyAnnotation == null) continue;
            Class<?> genericType = ReflectionUtils.getGenericType(field);
            if (genericType == null || !UniqueData.class.isAssignableFrom(genericType)) continue;
            Class<? extends UniqueData> referencedClass = genericType.asSubclass(UniqueData.class);
            metadataMap.put(field, new PersistentOneToManyCollectionMetadata(dataManager, clazz, referencedClass, SQLBuilder.parseLinks(oneToManyAnnotation.link())));
        }

        return metadataMap;
    }

    @Override
    public <U extends UniqueData> PersistentCollection<T> onAdd(Class<U> holderClass, CollectionChangeHandler<U, T> addHandler) {
        throw new UnsupportedOperationException("Dynamically adding change handlers is not supported for PersistentCollections");
    }

    @Override
    public <U extends UniqueData> PersistentCollection<T> onRemove(Class<U> holderClass, CollectionChangeHandler<U, T> removeHandler) {
        throw new UnsupportedOperationException("Dynamically adding change handlers is not supported for PersistentCollections");
    }

    @Override
    public UniqueData getHolder() {
        return holder;
    }

    @Override
    public int size() {
        return getIds().size();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(Object o) {
        if (!type.isInstance(o)) {
            return false;
        }
        T data = type.cast(o);
        return getIds().contains(data.getIdColumns());
    }

    @Override
    public @NotNull Iterator<T> iterator() {
        return new IteratorImpl(getIds());
    }

    @Override
    public @NotNull Object @NotNull [] toArray() {
        Set<ColumnValuePairs> ids = getIds();
        Object[] array = new Object[ids.size()];
        int i = 0;
        for (ColumnValuePairs idColumns : ids) {
            T instance = holder.getDataManager().getInstance(type, idColumns);
            array[i++] = instance;
        }
        return array;
    }

    @SuppressWarnings("unchecked")
    @Override
    public @NotNull <T1> T1 @NotNull [] toArray(@NotNull T1 @NotNull [] a) {
        Set<ColumnValuePairs> ids = getIds();
        if (a.length < ids.size()) {
            a = (T1[]) Array.newInstance(a.getClass().getComponentType(), ids.size());
        }
        int i = 0;
        for (ColumnValuePairs idColumns : ids) {
            T instance = holder.getDataManager().getInstance(type, idColumns);
            T1 element = (T1) instance;
            a[i++] = element;
        }
        return a;
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
        Set<ColumnValuePairs> ids = getIds();
        for (Object o : c) {
            T data = type.cast(o);
            if (!ids.contains(data.getIdColumns())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean addAll(@NotNull Collection<? extends T> c) {
        Preconditions.checkArgument(!holder.isDeleted(), "Cannot set entries on a deleted UniqueData instance");
        if (c.isEmpty()) {
            return false;
        }
        DataAccessor dataAccessor = holder.getDataManager().getDataAccessor();

        SQLTransaction.Statement selectDataIdsStatement = buildSelectDataIdsStatement();
        SQLTransaction.Statement updateStatement = buildUpdateStatement();


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
            transaction.update(updateStatement, () -> {
                List<Object> values = new ArrayList<>(holderLinkingValues);
                for (ColumnValuePair idColumn : entry.getIdColumns()) {
                    values.add(idColumn.value());
                }
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
        List<ColumnValuePairs> ids = new ArrayList<>();
        for (Object o : c) {
            if (!type.isInstance(o)) {
                continue;
            }
            T data = type.cast(o);
            ColumnValuePairs thatIdColumns = data.getIdColumns();
            ids.add(thatIdColumns);
        }
        removeIds(ids);

        return !ids.isEmpty();
    }

    @Override
    public boolean retainAll(@NotNull Collection<?> c) {
        Set<ColumnValuePairs> currentIds = getIds();
        Set<ColumnValuePairs> idsToRetain = new HashSet<>();
        for (Object o : c) {
            if (!type.isInstance(o)) {
                continue;
            }
            T data = type.cast(o);
            ColumnValuePairs thatIdColumns = data.getIdColumns();
            idsToRetain.add(thatIdColumns);
        }

        List<ColumnValuePairs> idsToRemove = new ArrayList<>();
        for (ColumnValuePairs idColumns : currentIds) {
            if (!idsToRetain.contains(idColumns)) {
                idsToRemove.add(idColumns);
            }
        }

        if (!idsToRemove.isEmpty()) {
            removeIds(idsToRemove);
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

    private void removeIds(List<ColumnValuePairs> ids) {
        Preconditions.checkArgument(!holder.isDeleted(), "Cannot set entries on a deleted UniqueData instance");
        if (ids.isEmpty()) {
            return;
        }

        DataAccessor dataAccessor = holder.getDataManager().getDataAccessor();

        SQLTransaction.Statement selectDataIdsStatement = buildSelectDataIdsStatement();
        SQLTransaction.Statement updateStatement = buildUpdateStatement();


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

        for (ColumnValuePairs idColumns : ids) {
            transaction.update(updateStatement, () -> {
                List<Object> values = new ArrayList<>();
                for (Object holderLinkingValue : holderLinkingValues) { //set them to null
                    values.add(null);
                }
                for (ColumnValuePair idColumn : idColumns) {
                    values.add(idColumn.value());
                }
                return values;
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

    private SQLTransaction.Statement buildUpdateStatement() {
        UniqueDataMetadata typeMetadata = holder.getDataManager().getMetadata(type);

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("UPDATE \"").append(typeMetadata.schema()).append("\".\"").append(typeMetadata.table()).append("\" SET ");
        for (Link entry : link) {
            String theirColumn = entry.columnInReferencedTable();
            sqlBuilder.append("\"").append(theirColumn).append("\" = ?, ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 2);
        sqlBuilder.append(" WHERE ");
        for (ColumnMetadata theirIdColumn : typeMetadata.idColumns()) {
            sqlBuilder.append("\"").append(theirIdColumn.name()).append("\" = ? AND ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 5);
        @Language("SQL") String sql = sqlBuilder.toString();
        return SQLTransaction.Statement.of(sql, sql);
    }

    private SQLTransaction.Statement buildClearStatement() {
        UniqueDataMetadata typeMetadata = holder.getDataManager().getMetadata(type);

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("UPDATE \"").append(typeMetadata.schema()).append("\".\"").append(typeMetadata.table()).append("\" SET ");
        for (Link entry : link) {
            String theirColumn = entry.columnInReferencedTable();
            sqlBuilder.append("\"").append(theirColumn).append("\" = NULL, ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 2);
        sqlBuilder.append(" WHERE ");
        for (Link entry : link) {
            String theirColumn = entry.columnInReferencedTable();
            sqlBuilder.append("\"").append(theirColumn).append("\" = ? AND ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 5);
        @Language("SQL") String sql = sqlBuilder.toString();
        return SQLTransaction.Statement.of(sql, sql);
    }

    public Set<ColumnValuePairs> getIds() {
        // note: we need the join since we support linking on non-id columnsInReferringTable
        Preconditions.checkArgument(!holder.isDeleted(), "Cannot get entries on a deleted UniqueData instance");
        Set<ColumnValuePairs> ids = new HashSet<>();
        UniqueDataMetadata holderMetadata = holder.getMetadata();
        UniqueDataMetadata typeMetadata = holder.getDataManager().getMetadata(type);
        DataAccessor dataAccessor = holder.getDataManager().getDataAccessor();
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT ");
        for (ColumnMetadata columnMetadata : typeMetadata.idColumns()) {
            sqlBuilder.append("\"").append(columnMetadata.name()).append("\", ");
        }
        for (ColumnMetadata columnMetadata : holderMetadata.idColumns()) {
            sqlBuilder.append("_source.\"").append(columnMetadata.name()).append("\", ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 2);
        sqlBuilder.append(" FROM \"").append(typeMetadata.schema()).append("\".\"").append(typeMetadata.table()).append("\" ");
        sqlBuilder.append("INNER JOIN \"").append(holderMetadata.schema()).append("\".\"").append(holderMetadata.table()).append("\" AS _source ON ");
        for (Link entry : link) {
            String myColumn = entry.columnInReferringTable();
            String theirColumn = entry.columnInReferencedTable();
            sqlBuilder.append("\"").append(typeMetadata.schema()).append("\".\"").append(typeMetadata.table()).append("\".\"").append(theirColumn).append("\" = _source.\"").append(myColumn).append("\" AND ");
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
                int i = 0;
                ColumnValuePair[] idColumns = new ColumnValuePair[typeMetadata.idColumns().size()];
                for (ColumnMetadata columnMetadata : typeMetadata.idColumns()) {
                    Object value = rs.getObject(columnMetadata.name());
                    idColumns[i++] = new ColumnValuePair(columnMetadata.name(), value);
                }
                ids.add(new ColumnValuePairs(idColumns));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return ids;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof PersistentOneToManyCollectionImpl<?> that)) return false;
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
        private final List<ColumnValuePairs> ids;
        private int index = 0;

        public IteratorImpl(Set<ColumnValuePairs> ids) {
            this.ids = new ArrayList<>(ids);
        }

        @Override
        public boolean hasNext() {
            return index < ids.size();
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            ColumnValuePairs idColumns = ids.get(index++);
            return holder.getDataManager().getInstance(type, idColumns);
        }

        @Override
        public void remove() {
            Preconditions.checkState(index > 0, "next() has not been called yet");
            removeIds(Collections.singletonList(ids.get(index - 1)));
            ids.remove(--index);
        }
    }
}
