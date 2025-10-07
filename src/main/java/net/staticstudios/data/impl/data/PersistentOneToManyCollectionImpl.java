package net.staticstudios.data.impl.data;

import com.google.common.base.Preconditions;
import net.staticstudios.data.DataAccessor;
import net.staticstudios.data.OneToMany;
import net.staticstudios.data.PersistentCollection;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.parse.ForeignKey;
import net.staticstudios.data.parse.SQLBuilder;
import net.staticstudios.data.util.*;
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
    private final List<ForeignKey.Link> link;

    public PersistentOneToManyCollectionImpl(UniqueData holder, Class<T> type, List<ForeignKey.Link> link) {
        this.holder = holder;
        this.type = type;
        this.link = link;
    }

    public static <T extends UniqueData> void createAndDelegate(PersistentCollection.ProxyPersistentCollection<T> proxy, List<ForeignKey.Link> link) {
        PersistentOneToManyCollectionImpl<T> delegate = new PersistentOneToManyCollectionImpl<>(
                proxy.getHolder(),
                proxy.getReferenceType(),
                link
        );
        proxy.setDelegate(delegate);
    }

    public static <T extends UniqueData> PersistentOneToManyCollectionImpl<T> create(UniqueData holder, Class<T> type, List<ForeignKey.Link> link) {
        return new PersistentOneToManyCollectionImpl<>(holder, type, link);
    }

    public static <T extends UniqueData> void delegate(T instance) {
        UniqueDataMetadata metadata = instance.getDataManager().getMetadata(instance.getClass());
        for (FieldInstancePair<@Nullable PersistentCollection> pair : ReflectionUtils.getFieldInstancePairs(instance, PersistentCollection.class)) {
            PersistentCollectionMetadata collectionMetadata = metadata.persistentCollectionMetadata().get(pair.field());
            if (!(collectionMetadata instanceof PersistentOneToManyCollectionMetadata oneToManyMetadata)) continue;

            if (pair.instance() instanceof PersistentCollection.ProxyPersistentCollection<?> proxyCollection) {
                createAndDelegate((PersistentCollection.ProxyPersistentCollection<? extends UniqueData>) proxyCollection, oneToManyMetadata.getLinks());
            } else {
                pair.field().setAccessible(true);
                try {
                    pair.field().set(instance, create(instance, oneToManyMetadata.getDataType(), oneToManyMetadata.getLinks()));
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static <T extends UniqueData> Map<Field, PersistentOneToManyCollectionMetadata> extractMetadata(Class<T> clazz) {
        Map<Field, PersistentOneToManyCollectionMetadata> metadataMap = new HashMap<>();
        for (Field field : ReflectionUtils.getFields(clazz, PersistentCollection.class)) {
            OneToMany oneToManyAnnotation = field.getAnnotation(OneToMany.class);
            if (oneToManyAnnotation == null) continue;
            Class<?> genericType = ReflectionUtils.getGenericType(field);
            if (genericType == null || !UniqueData.class.isAssignableFrom(genericType)) continue;
            Class<? extends UniqueData> referencedClass = genericType.asSubclass(UniqueData.class);
            metadataMap.put(field, new PersistentOneToManyCollectionMetadata(referencedClass, SQLBuilder.parseLinks(oneToManyAnnotation.link())));
        }

        return metadataMap;
    }


    @Override
    public UniqueData getHolder() {
        return holder;
    }

    @Override
    public Class<T> getReferenceType() {
        return type;
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
        Set<ColumnValuePair[]> ids = getIds();
        ColumnValuePair[] thatIdColumns = data.getIdColumns().getPairs();
        for (ColumnValuePair[] idColumns : ids) {
            if (Arrays.equals(idColumns, thatIdColumns)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public @NotNull Iterator<T> iterator() {
        return new IteratorImpl(getIds());
    }

    @Override
    public @NotNull Object @NotNull [] toArray() {
        Set<ColumnValuePair[]> ids = getIds();
        Object[] array = new Object[ids.size()];
        int i = 0;
        for (ColumnValuePair[] idColumns : ids) {
            T instance = holder.getDataManager().getInstance(type, idColumns);
            array[i++] = instance;
        }
        return array;
    }

    @SuppressWarnings("unchecked")
    @Override
    public @NotNull <T1> T1 @NotNull [] toArray(@NotNull T1 @NotNull [] a) {
        Set<ColumnValuePair[]> ids = getIds();
        if (a.length < ids.size()) {
            a = (T1[]) Array.newInstance(a.getClass().getComponentType(), ids.size());
        }
        int i = 0;
        for (ColumnValuePair[] idColumns : ids) {
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
        Set<ColumnValuePair[]> ids = getIds();
        for (Object o : c) {
            T data = type.cast(o);
            ColumnValuePair[] thatIdColumns = data.getIdColumns().getPairs();
            boolean found = false;
            for (ColumnValuePair[] idColumns : ids) {
                if (Arrays.equals(idColumns, thatIdColumns)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean addAll(@NotNull Collection<? extends T> c) {
        Preconditions.checkArgument(!holder.isDeleted(), "Cannot set entries on a deleted UniqueData instance");
        UniqueDataMetadata holderMetadata = holder.getMetadata();
        UniqueDataMetadata typeMetadata = holder.getDataManager().getMetadata(type);

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("UPDATE \"").append(typeMetadata.schema()).append("\".\"").append(typeMetadata.table()).append("\" SET ");
        for (ForeignKey.Link entry : link) {
            String theirColumn = entry.columnInReferencedTable();
            sqlBuilder.append("\"").append(theirColumn).append("\" = ?, ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 2);
        sqlBuilder.append(" WHERE ");
        for (ColumnMetadata theirIdColumn : typeMetadata.idColumns()) {
            sqlBuilder.append("\"").append(theirIdColumn.name()).append("\" = ? AND ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 5);
        @Language("SQL") String updateSql = sqlBuilder.toString();
        DataAccessor dataAccessor = holder.getDataManager().getDataAccessor();
        List<Object> myValues = getMyLinkingValues(holderMetadata, dataAccessor);

        for (T entry : c) {
            List<Object> values = new ArrayList<>(myValues);
            for (ColumnValuePair idColumn : entry.getIdColumns()) {
                values.add(idColumn.value());
            }
            try {
                dataAccessor.executeUpdate(updateSql, values, 0);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        return !c.isEmpty();
    }

    @Override
    public boolean removeAll(@NotNull Collection<?> c) {
        List<ColumnValuePair[]> ids = new ArrayList<>();
        for (Object o : c) {
            if (!type.isInstance(o)) {
                continue;
            }
            T data = type.cast(o);
            ColumnValuePair[] thatIdColumns = data.getIdColumns().getPairs();
            ids.add(thatIdColumns);
        }
        removeAll(ids);

        return !ids.isEmpty();
    }

    @Override
    public boolean retainAll(@NotNull Collection<?> c) {
        Set<ColumnValuePair[]> currentIds = getIds();
        Set<ColumnValuePair[]> idsToRetain = new HashSet<>();
        for (Object o : c) {
            if (!type.isInstance(o)) {
                continue;
            }
            T data = type.cast(o);
            ColumnValuePair[] thatIdColumns = data.getIdColumns().getPairs();
            idsToRetain.add(thatIdColumns);
        }

        List<ColumnValuePair[]> idsToRemove = new ArrayList<>();
        for (ColumnValuePair[] idColumns : currentIds) {
            boolean found = false;
            for (ColumnValuePair[] retainIdColumns : idsToRetain) {
                if (Arrays.equals(idColumns, retainIdColumns)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                idsToRemove.add(idColumns);
            }
        }

        if (!idsToRemove.isEmpty()) {
            removeAll(idsToRemove);
            return true;
        }

        return false;
    }

    @Override
    public void clear() {
        Preconditions.checkArgument(!holder.isDeleted(), "Cannot set entries on a deleted UniqueData instance");
        UniqueDataMetadata holderMetadata = holder.getMetadata();
        UniqueDataMetadata typeMetadata = holder.getDataManager().getMetadata(type);

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("UPDATE \"").append(typeMetadata.schema()).append("\".\"").append(typeMetadata.table()).append("\" SET ");
        for (ForeignKey.Link entry : link) {
            String theirColumn = entry.columnInReferencedTable();
            sqlBuilder.append("\"").append(theirColumn).append("\" = NULL, ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 2);
        sqlBuilder.append(" WHERE ");
        for (ForeignKey.Link entry : link) {
            String theirColumn = entry.columnInReferencedTable();
            sqlBuilder.append("\"").append(theirColumn).append("\" = ? AND ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 5);
        @Language("SQL") String updateSql = sqlBuilder.toString();
        DataAccessor dataAccessor = holder.getDataManager().getDataAccessor();
        List<Object> values = getMyLinkingValues(holderMetadata, dataAccessor);
        try {
            dataAccessor.executeUpdate(updateSql, values, 0);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void removeAll(List<ColumnValuePair[]> ids) {
        Preconditions.checkArgument(!holder.isDeleted(), "Cannot set entries on a deleted UniqueData instance");
        UniqueDataMetadata holderMetadata = holder.getMetadata();
        UniqueDataMetadata typeMetadata = holder.getDataManager().getMetadata(type);

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("UPDATE \"").append(typeMetadata.schema()).append("\".\"").append(typeMetadata.table()).append("\" SET ");
        for (ForeignKey.Link entry : link) {
            String theirColumn = entry.columnInReferencedTable();
            sqlBuilder.append("\"").append(theirColumn).append("\" = NULL, ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 2);
        sqlBuilder.append(" WHERE ");
        for (ForeignKey.Link entry : link) {
            String theirColumn = entry.columnInReferencedTable();
            sqlBuilder.append("\"").append(theirColumn).append("\" = ? AND ");
        }
        for (ColumnMetadata theirIdColumn : typeMetadata.idColumns()) {
            sqlBuilder.append("\"").append(theirIdColumn.name()).append("\" = ? AND ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 5);
        @Language("SQL") String updateSql = sqlBuilder.toString();
        DataAccessor dataAccessor = holder.getDataManager().getDataAccessor();
        List<Object> myValues = getMyLinkingValues(holderMetadata, dataAccessor);

        for (ColumnValuePair[] idColumns : ids) {
            List<Object> values = new ArrayList<>(myValues);
            for (ColumnValuePair idColumn : idColumns) {
                values.add(idColumn.value());
            }
            try {
                dataAccessor.executeUpdate(updateSql, values, 0);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private List<Object> getMyLinkingValues(UniqueDataMetadata holderMetadata, DataAccessor dataAccessor) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT ");
        for (ForeignKey.Link entry : link) {
            String myColumn = entry.columnInReferringTable();
            sqlBuilder.append("\"").append(myColumn).append("\", ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 2);
        sqlBuilder.append(" FROM \"").append(holderMetadata.schema()).append("\".\"").append(holderMetadata.table()).append("\" WHERE ");
        for (ColumnValuePair columnValuePair : holder.getIdColumns()) {
            sqlBuilder.append("\"").append(columnValuePair.column()).append("\" = ? AND ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 5);
        @Language("SQL") String selectSql = sqlBuilder.toString();

        List<Object> myValues = new ArrayList<>(link.size());
        try (ResultSet rs = dataAccessor.executeQuery(selectSql, holder.getIdColumns().stream().map(ColumnValuePair::value).toList())) {
            Preconditions.checkState(rs.next(), "Could not find holder row in database");
            for (ForeignKey.Link entry : link) {
                String myColumn = entry.columnInReferringTable();
                myValues.add(rs.getObject(myColumn));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return myValues;
    }

    private Set<ColumnValuePair[]> getIds() {
        Preconditions.checkArgument(!holder.isDeleted(), "Cannot get entries on a deleted UniqueData instance");
        Set<ColumnValuePair[]> ids = new HashSet<>();
        UniqueDataMetadata holderMetadata = holder.getMetadata();
        UniqueDataMetadata typeMetadata = holder.getDataManager().getMetadata(type);
        DataAccessor dataAccessor = holder.getDataManager().getDataAccessor();
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT ");
        for (ColumnMetadata columnMetadata : typeMetadata.idColumns()) {
            sqlBuilder.append("\"").append(columnMetadata.name()).append("\", ");
        }
        for (ColumnMetadata columnMetadata : holderMetadata.idColumns()) {
            sqlBuilder.append("source.\"").append(columnMetadata.name()).append("\", ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 2);
        sqlBuilder.append(" FROM \"").append(typeMetadata.schema()).append("\".\"").append(typeMetadata.table()).append("\" ");
        sqlBuilder.append("INNER JOIN \"").append(holderMetadata.schema()).append("\".\"").append(holderMetadata.table()).append("\" AS source ON ");
        for (ForeignKey.Link entry : link) {
            String myColumn = entry.columnInReferringTable();
            String theirColumn = entry.columnInReferencedTable();
            sqlBuilder.append("\"").append(typeMetadata.schema()).append("\".\"").append(typeMetadata.table()).append("\".\"").append(theirColumn).append("\" = source.\"").append(myColumn).append("\" AND ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 5);
        sqlBuilder.append(" WHERE ");

        for (ForeignKey.Link entry : link) {
            String theirColumn = entry.columnInReferencedTable();
            sqlBuilder.append("\"").append(theirColumn).append("\" = source.\"").append(entry.columnInReferringTable()).append("\" AND ");
        }
        for (ColumnValuePair columnValuePair : holder.getIdColumns()) {
            sqlBuilder.append("source.\"").append(columnValuePair.column()).append("\" = ? AND ");
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
                ids.add(idColumns);
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
        return Objects.equals(holder, that.holder) &&
                Objects.equals(type, that.type) &&
                Objects.equals(getIds(), that.getIds());
    }

    @Override
    public int hashCode() {
        return Objects.hash(holder, type, getIds());
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
        private final List<ColumnValuePair[]> ids;
        private int index = 0;

        public IteratorImpl(Set<ColumnValuePair[]> ids) {
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
            ColumnValuePair[] idColumns = ids.get(index++);
            return holder.getDataManager().getInstance(type, idColumns);
        }

        @Override
        public void remove() {
            Preconditions.checkState(index > 0, "next() has not been called yet");
            removeAll(Collections.singletonList(ids.get(index - 1)));
            ids.remove(--index);
        }
    }
}
