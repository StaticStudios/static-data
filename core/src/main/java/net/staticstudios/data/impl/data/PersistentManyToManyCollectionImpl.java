package net.staticstudios.data.impl.data;

import com.google.common.base.Preconditions;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.ManyToMany;
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

public class PersistentManyToManyCollectionImpl<T extends UniqueData> implements PersistentCollection<T> {
    private final UniqueData holder;
    private final Class<T> type;
    private final PersistentManyToManyCollectionMetadata metadata;

    @SuppressWarnings("unchecked")
    public PersistentManyToManyCollectionImpl(UniqueData holder, PersistentManyToManyCollectionMetadata metadata) {
        this.holder = holder;
        this.type = (Class<T>) metadata.getReferencedType();
        this.metadata = metadata;
    }

    public static <T extends UniqueData> void createAndDelegate(ProxyPersistentCollection<T> proxy, PersistentManyToManyCollectionMetadata metadata) {
        PersistentManyToManyCollectionImpl<T> impl = new PersistentManyToManyCollectionImpl<>(proxy.getHolder(), metadata);
        proxy.setDelegate(metadata, impl);
    }

    public static <T extends UniqueData> PersistentManyToManyCollectionImpl<T> create(UniqueData holder, PersistentManyToManyCollectionMetadata metadata) {
        return new PersistentManyToManyCollectionImpl<>(holder, metadata);
    }

    public static <T extends UniqueData> void delegate(T instance) {
        UniqueDataMetadata metadata = instance.getDataManager().getMetadata(instance.getClass());
        for (FieldInstancePair<@Nullable PersistentCollection> pair : ReflectionUtils.getFieldInstancePairs(instance, PersistentCollection.class)) {
            PersistentCollectionMetadata collectionMetadata = metadata.persistentCollectionMetadata().get(pair.field());
            if (!(collectionMetadata instanceof PersistentManyToManyCollectionMetadata oneToManyMetadata)) continue;

            if (pair.instance() instanceof PersistentCollection.ProxyPersistentCollection<?> proxyCollection) {
                createAndDelegate((PersistentCollection.ProxyPersistentCollection<? extends UniqueData>) proxyCollection, oneToManyMetadata);
            } else {
                pair.field().setAccessible(true);
                try {
                    pair.field().set(instance, create(instance, oneToManyMetadata));
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static <T extends UniqueData> Map<Field, PersistentManyToManyCollectionMetadata> extractMetadata(Class<T> clazz) {
        Map<Field, PersistentManyToManyCollectionMetadata> metadataMap = new HashMap<>();
        for (Field field : ReflectionUtils.getFields(clazz, PersistentCollection.class)) {
            ManyToMany manyToManyAnnotation = field.getAnnotation(ManyToMany.class);
            if (manyToManyAnnotation == null) continue;
            Class<?> genericType = ReflectionUtils.getGenericType(field);
            if (genericType == null) continue;
            Class<? extends UniqueData> referencedClass = genericType.asSubclass(UniqueData.class);
            String parsedJoinTableSchemaName = ValueUtils.parseValue(manyToManyAnnotation.joinTableSchema());
            String parsedJoinTableName = ValueUtils.parseValue(manyToManyAnnotation.joinTable());
            String links = manyToManyAnnotation.link();
            PersistentManyToManyCollectionMetadata metadata = new PersistentManyToManyCollectionMetadata(clazz, referencedClass, parsedJoinTableSchemaName, parsedJoinTableName, links);
            metadataMap.put(field, metadata);
        }

        return metadataMap;
    }

    public static String getJoinTableSchema(String parsedJoinTableSchema, String dataSchema) {
        if (!parsedJoinTableSchema.isEmpty()) {
            return parsedJoinTableSchema;
        } else {
            return dataSchema;
        }
    }

    public static String getJoinTableName(String parsedJoinTableName, String dataTable, String referencedTable) {
        if (!parsedJoinTableName.isEmpty()) {
            return parsedJoinTableName;
        } else {
            return dataTable + "_" + referencedTable;
        }
    }

    public static String getDataTableColumnPrefix(String dataTable) {
        return dataTable;
    }

    public static String getReferencedTableColumnPrefix(String dataTable, String referencedTable) {
        if (dataTable.equals(referencedTable)) {
            return referencedTable + "_ref";
        }
        return referencedTable;
    }

    public static List<Link> getJoinTableToDataTableLinks(String dataTable, String links) {
        List<Link> joinTableToDataTableLinks = new ArrayList<>();
        String dataTableColumnPrefix = getDataTableColumnPrefix(dataTable);
        for (Link link : SQLBuilder.parseLinks(links)) {
            String columnInDataTable = link.columnInReferringTable();
            String dataColumnInJoinTable = dataTableColumnPrefix + "_" + columnInDataTable;

            joinTableToDataTableLinks.add(new Link(columnInDataTable, dataColumnInJoinTable));
        }
        return joinTableToDataTableLinks;
    }

    public static List<Link> getJoinTableToReferencedTableLinks(String dataTable, String referencedTable, String links) {
        List<Link> joinTableToReferencedTableLinks = new ArrayList<>();
        String referencedTableColumnPrefix = getReferencedTableColumnPrefix(dataTable, referencedTable);
        for (Link link : SQLBuilder.parseLinks(links)) {
            String columnInReferencedTable = link.columnInReferencedTable();
            String referencedColumnInJoinTable = referencedTableColumnPrefix + "_" + columnInReferencedTable;

            joinTableToReferencedTableLinks.add(new Link(columnInReferencedTable, referencedColumnInJoinTable));
        }
        return joinTableToReferencedTableLinks;
    }

    public static SQLTransaction.Statement buildUpdateStatement(DataManager dataManager, PersistentManyToManyCollectionMetadata metadata) {
        List<Link> joinTableToDataTableLinks = metadata.getJoinTableToDataTableLinks(dataManager);
        List<Link> joinTableToReferencedTableLinks = metadata.getJoinTableToReferencedTableLinks(dataManager);

        String joinTableSchema = metadata.getJoinTableSchema(dataManager);
        String joinTableName = metadata.getJoinTableName(dataManager);
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("MERGE INTO \"").append(joinTableSchema).append("\".\"").append(joinTableName).append("\" AS _target USING (VALUES (");
        sqlBuilder.append("?, ".repeat(Math.max(0, joinTableToDataTableLinks.size() + joinTableToReferencedTableLinks.size())));
        sqlBuilder.setLength(sqlBuilder.length() - 2);
        sqlBuilder.append(")) AS _source (");
        for (Link entry : joinTableToDataTableLinks) {
            String joinColumn = entry.columnInReferringTable();
            sqlBuilder.append("\"").append(joinColumn).append("\", ");
        }
        for (Link entry : joinTableToReferencedTableLinks) {
            String joinColumn = entry.columnInReferringTable();
            sqlBuilder.append("\"").append(joinColumn).append("\", ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 2);
        sqlBuilder.append(") ON ");
        for (Link entry : joinTableToDataTableLinks) {
            String joinColumn = entry.columnInReferringTable();
            sqlBuilder.append("_target.\"").append(joinColumn).append("\" = _source.\"").append(joinColumn).append("\" AND ");
        }
        for (Link entry : joinTableToReferencedTableLinks) {
            String joinColumn = entry.columnInReferringTable();
            sqlBuilder.append("_target.\"").append(joinColumn).append("\" = _source.\"").append(joinColumn).append("\" AND ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 5);
        sqlBuilder.append(" WHEN NOT MATCHED THEN INSERT (");
        for (Link entry : joinTableToDataTableLinks) {
            String joinColumn = entry.columnInReferringTable();
            sqlBuilder.append("\"").append(joinColumn).append("\", ");
        }
        for (Link entry : joinTableToReferencedTableLinks) {
            String joinColumn = entry.columnInReferringTable();
            sqlBuilder.append("\"").append(joinColumn).append("\", ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 2);
        sqlBuilder.append(") VALUES (");
        for (Link entry : joinTableToDataTableLinks) {
            String joinColumn = entry.columnInReferringTable();
            sqlBuilder.append("_source.\"").append(joinColumn).append("\", ");
        }
        for (Link entry : joinTableToReferencedTableLinks) {
            String joinColumn = entry.columnInReferringTable();
            sqlBuilder.append("_source.\"").append(joinColumn).append("\", ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 2);
        sqlBuilder.append(")");
        @Language("SQL") String h2MergeSql = sqlBuilder.toString();

        sqlBuilder.setLength(0);
        sqlBuilder.append("INSERT INTO \"").append(joinTableSchema).append("\".\"").append(joinTableName).append("\" (");
        for (Link entry : joinTableToDataTableLinks) {
            String joinColumn = entry.columnInReferringTable();
            sqlBuilder.append("\"").append(joinColumn).append("\", ");
        }
        for (Link entry : joinTableToReferencedTableLinks) {
            String joinColumn = entry.columnInReferringTable();
            sqlBuilder.append("\"").append(joinColumn).append("\", ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 2);
        sqlBuilder.append(") VALUES (");
        sqlBuilder.append("?, ".repeat(Math.max(0, joinTableToDataTableLinks.size() + joinTableToReferencedTableLinks.size())));
        sqlBuilder.setLength(sqlBuilder.length() - 2);
        sqlBuilder.append(") ON CONFLICT DO NOTHING");
        @Language("SQL") String pgInsertSql = sqlBuilder.toString();

        return SQLTransaction.Statement.of(h2MergeSql, pgInsertSql);
    }

    @Override
    public <U extends UniqueData> PersistentCollection<T> onAdd(Class<U> holderClass, CollectionChangeHandler<U, T> addHandler) {
        throw new UnsupportedOperationException("Dynamically adding change handlers is not supported for PersistentCollections");
    }

    @Override
    public <U extends UniqueData> PersistentCollection<T> onRemove(Class<U> holderClass, CollectionChangeHandler<U, T> removeHandler) {
        throw new UnsupportedOperationException("Dynamically adding change handlers is not supported for PersistentCollections");
    }

    public PersistentManyToManyCollectionMetadata getMetadata() {
        return this.metadata;
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
        Set<ColumnValuePairs> ids = getIds();
        ColumnValuePairs thatIdColumns = data.getIdColumns();
        return ids.contains(thatIdColumns);
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
            ColumnValuePairs thatIdColumns = data.getIdColumns();
            if (!ids.contains(thatIdColumns)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean addAll(@NotNull Collection<? extends T> c) {
        Preconditions.checkArgument(!holder.isDeleted(), "Cannot set entries on a deleted UniqueData instance");
        if (c.isEmpty()) { //this operation isn't cheap, so we should avoid it if we can
            return false;
        }

        DataAccessor dataAccessor = holder.getDataManager().getDataAccessor();
        SQLTransaction.Statement selectDataIdsStatement = buildSelectDataIdsStatement();
        SQLTransaction.Statement selectReferencedIdsStatement = buildSelectReferencedIdsStatement();
        SQLTransaction.Statement updateStatement = buildUpdateStatement();
        List<Link> joinTableToDataTableLinks = metadata.getJoinTableToDataTableLinks(holder.getDataManager());
        List<Link> joinTableToReferencedTableLinks = metadata.getJoinTableToReferencedTableLinks(holder.getDataManager());

        List<Object> holderLinkingValues = new ArrayList<>(joinTableToDataTableLinks.size());
        List<Object> holderIdValues = holder.getIdColumns().stream().map(ColumnValuePair::value).toList();

        SQLTransaction transaction = new SQLTransaction();
        transaction.query(selectDataIdsStatement, () -> holderIdValues, rs -> {
            try {
                Preconditions.checkState(rs.next(), "Could not find holder row in database");
                for (Link entry : joinTableToDataTableLinks) {
                    String dataColumn = entry.columnInReferencedTable();
                    Object value = rs.getObject(dataColumn);
                    holderLinkingValues.add(value);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });


        for (T entry : c) {
            List<Object> referencedIdValues = entry.getIdColumns().stream().map(ColumnValuePair::value).toList();
            List<Object> referencedLinkingValues = new ArrayList<>(joinTableToReferencedTableLinks.size());
            transaction.query(selectReferencedIdsStatement, () -> referencedIdValues, rs -> {
                try {
                    Preconditions.checkState(rs.next(), "Could not find referenced row in database");
                    for (Link _entry : joinTableToReferencedTableLinks) {
                        String referencedColumn = _entry.columnInReferencedTable();
                        Object value = rs.getObject(referencedColumn);
                        referencedLinkingValues.add(value);
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });

            transaction.update(updateStatement, () -> {
                List<Object> values = new ArrayList<>(holderLinkingValues.size() + referencedLinkingValues.size());
                values.addAll(holderLinkingValues);
                values.addAll(referencedLinkingValues);
                return values;
            });
        }

        try {
            dataAccessor.executeTransaction(transaction, 0);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return !c.isEmpty();
    }

    @Override
    public boolean removeAll(@NotNull Collection<?> c) {
        List<ColumnValuePairs> idsToRemove = new ArrayList<>();
        for (Object o : c) {
            if (!type.isInstance(o)) {
                continue;
            }
            T data = type.cast(o);
            ColumnValuePairs idColumns = data.getIdColumns();
            idsToRemove.add(idColumns);
        }
        return removeIds(idsToRemove);
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
        DataAccessor dataAccessor = holder.getDataManager().getDataAccessor();
        SQLTransaction.Statement selectDataIdsStatement = buildSelectDataIdsStatement();
        SQLTransaction.Statement clearStatement = buildClearStatement();
        List<Link> joinTableToDataTableLinks = metadata.getJoinTableToDataTableLinks(holder.getDataManager());

        List<Object> holderLinkingValues = new ArrayList<>(joinTableToDataTableLinks.size());
        List<Object> holderIdValues = holder.getIdColumns().stream().map(ColumnValuePair::value).toList();

        SQLTransaction transaction = new SQLTransaction();
        transaction.query(selectDataIdsStatement, () -> holderIdValues, rs -> {
            try {
                Preconditions.checkState(rs.next(), "Could not find holder row in database");
                for (Link entry : joinTableToDataTableLinks) {
                    String dataColumn = entry.columnInReferencedTable();
                    Object value = rs.getObject(dataColumn);
                    holderLinkingValues.add(value);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        transaction.update(clearStatement, holderLinkingValues);

        try {
            dataAccessor.executeTransaction(transaction, 0);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean removeIds(List<ColumnValuePairs> idsToRemove) {
        if (idsToRemove.isEmpty()) { //this operation isn't cheap, so we should avoid it if we can
            return false;
        }

        DataAccessor dataAccessor = holder.getDataManager().getDataAccessor();
        SQLTransaction.Statement selectDataIdsStatement = buildSelectDataIdsStatement();
        SQLTransaction.Statement selectReferencedIdsStatement = buildSelectReferencedIdsStatement();
        SQLTransaction.Statement removeStatement = buildRemoveStatement();
        List<Link> joinTableToDataTableLinks = metadata.getJoinTableToDataTableLinks(holder.getDataManager());
        List<Link> joinTableToReferencedTableLinks = metadata.getJoinTableToReferencedTableLinks(holder.getDataManager());

        List<Object> holderLinkingValues = new ArrayList<>(joinTableToDataTableLinks.size());
        List<Object> holderIdValues = holder.getIdColumns().stream().map(ColumnValuePair::value).toList();

        SQLTransaction transaction = new SQLTransaction();
        transaction.query(selectDataIdsStatement, () -> holderIdValues, rs -> {
            try {
                Preconditions.checkState(rs.next(), "Could not find holder row in database");
                for (Link entry : joinTableToDataTableLinks) {
                    String dataColumn = entry.columnInReferencedTable();
                    Object value = rs.getObject(dataColumn);
                    holderLinkingValues.add(value);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });


        for (ColumnValuePairs idColumns : idsToRemove) {
            List<Object> referencedIdValues = idColumns.stream().map(ColumnValuePair::value).toList();
            List<Object> referencedLinkingValues = new ArrayList<>(joinTableToReferencedTableLinks.size());
            transaction.query(selectReferencedIdsStatement, () -> referencedIdValues, rs -> {
                try {
                    Preconditions.checkState(rs.next(), "Could not find referenced row in database");
                    for (Link _entry : joinTableToReferencedTableLinks) {
                        String referencedColumn = _entry.columnInReferencedTable();
                        Object value = rs.getObject(referencedColumn);
                        referencedLinkingValues.add(value);
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });

            transaction.update(removeStatement, () -> {
                List<Object> values = new ArrayList<>(holderLinkingValues.size() + referencedLinkingValues.size());
                values.addAll(holderLinkingValues);
                values.addAll(referencedLinkingValues);
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

    /**
     * get the ids for the referenced type that are linked to the holder
     *
     * @return set of id column value pairs for the referenced type
     */
    public Set<ColumnValuePairs> getIds() {
        //todo: this method is slow, plan to cache this later.
        Preconditions.checkArgument(!holder.isDeleted(), "Cannot get entries on a deleted UniqueData instance");
        Set<ColumnValuePairs> ids = new HashSet<>();
        UniqueDataMetadata holderMetadata = holder.getMetadata();
        UniqueDataMetadata target = holder.getDataManager().getMetadata(type);
        DataAccessor dataAccessor = holder.getDataManager().getDataAccessor();

        String joinTableSchema = metadata.getJoinTableSchema(holder.getDataManager());
        String joinTableName = metadata.getJoinTableName(holder.getDataManager());
        List<Link> joinTableToDataTableLinks = metadata.getJoinTableToDataTableLinks(holder.getDataManager());
        List<Link> joinTableToReferencedTableLinks = metadata.getJoinTableToReferencedTableLinks(holder.getDataManager());

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT ");
        for (ColumnMetadata columnMetadata : target.idColumns()) {
            sqlBuilder.append("_target.\"").append(columnMetadata.name()).append("\", ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 2);
        sqlBuilder.append(" FROM \"").append(joinTableSchema).append("\".\"").append(joinTableName).append("\" ");
        sqlBuilder.append("INNER JOIN \"").append(holderMetadata.schema()).append("\".\"").append(holderMetadata.table()).append("\" _data ON ");
        for (Link entry : joinTableToDataTableLinks) {
            String joinColumn = entry.columnInReferringTable();
            String dataColumn = entry.columnInReferencedTable();
            sqlBuilder.append("_data.\"").append(dataColumn).append("\" = \"").append(joinTableSchema).append("\".\"").append(joinTableName).append("\".\"").append(joinColumn).append("\" AND ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 5);
        sqlBuilder.append(" INNER JOIN \"").append(target.schema()).append("\".\"").append(target.table()).append("\" _target ON ");
        for (Link entry : joinTableToReferencedTableLinks) {
            String joinColumn = entry.columnInReferringTable();
            String referencedColumn = entry.columnInReferencedTable();
            sqlBuilder.append("_target.\"").append(referencedColumn).append("\" = \"").append(joinTableSchema).append("\".\"").append(joinTableName).append("\".\"").append(joinColumn).append("\" AND ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 5);

        sqlBuilder.append(" WHERE ");

        for (ColumnValuePair columnValuePair : holder.getIdColumns()) {
            sqlBuilder.append("_data.\"").append(columnValuePair.column()).append("\" = ? AND ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 5);

        @Language("SQL") String sql = sqlBuilder.toString();
        try (ResultSet rs = dataAccessor.executeQuery(sql, holder.getIdColumns().stream().map(ColumnValuePair::value).toList())) {
            while (rs.next()) {
                int i = 0;
                ColumnValuePair[] idColumns = new ColumnValuePair[target.idColumns().size()];
                for (ColumnMetadata columnMetadata : target.idColumns()) {
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

    private SQLTransaction.Statement buildSelectDataIdsStatement() {
        UniqueDataMetadata holderMetadata = holder.getMetadata();
        List<Link> joinTableToDataTableLinks = metadata.getJoinTableToDataTableLinks(holder.getDataManager());

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT ");
        for (Link entry : joinTableToDataTableLinks) {
            String dataColumn = entry.columnInReferencedTable();
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

    private SQLTransaction.Statement buildSelectReferencedIdsStatement() {
        UniqueDataMetadata typeMetadata = holder.getDataManager().getMetadata(type);
        List<Link> joinTableToReferencedTableLinks = metadata.getJoinTableToReferencedTableLinks(holder.getDataManager());

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT ");
        for (Link entry : joinTableToReferencedTableLinks) {
            String referencedColumn = entry.columnInReferencedTable();
            sqlBuilder.append("\"").append(referencedColumn).append("\", ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 2);
        sqlBuilder.append(" FROM \"").append(typeMetadata.schema()).append("\".\"").append(typeMetadata.table()).append("\" WHERE ");
        for (ColumnMetadata theirIdColumn : typeMetadata.idColumns()) {
            sqlBuilder.append("\"").append(theirIdColumn.name()).append("\" = ? AND ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 5);
        @Language("SQL") String sql = sqlBuilder.toString();
        return SQLTransaction.Statement.of(sql, sql);
    }

    private SQLTransaction.Statement buildUpdateStatement() {
        return buildUpdateStatement(holder.getDataManager(), metadata);
    }

    private SQLTransaction.Statement buildRemoveStatement() {
        List<Link> joinTableToDataTableLinks = metadata.getJoinTableToDataTableLinks(holder.getDataManager());
        List<Link> joinTableToReferencedTableLinks = metadata.getJoinTableToReferencedTableLinks(holder.getDataManager());

        String joinTableSchema = metadata.getJoinTableSchema(holder.getDataManager());
        String joinTableName = metadata.getJoinTableName(holder.getDataManager());
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("DELETE FROM \"").append(joinTableSchema).append("\".\"").append(joinTableName).append("\" WHERE ");
        for (Link entry : joinTableToDataTableLinks) {
            String joinColumn = entry.columnInReferringTable();
            sqlBuilder.append("\"").append(joinColumn).append("\" = ? AND ");
        }
        for (Link entry : joinTableToReferencedTableLinks) {
            String joinColumn = entry.columnInReferringTable();
            sqlBuilder.append("\"").append(joinColumn).append("\" = ? AND ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 5);
        @Language("SQL") String sql = sqlBuilder.toString();
        return SQLTransaction.Statement.of(sql, sql);
    }

    private SQLTransaction.Statement buildClearStatement() {
        List<Link> joinTableToDataTableLinks = metadata.getJoinTableToDataTableLinks(holder.getDataManager());

        String joinTableSchema = metadata.getJoinTableSchema(holder.getDataManager());
        String joinTableName = metadata.getJoinTableName(holder.getDataManager());
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("DELETE FROM \"").append(joinTableSchema).append("\".\"").append(joinTableName).append("\" WHERE ");
        for (Link entry : joinTableToDataTableLinks) {
            String joinColumn = entry.columnInReferringTable();
            sqlBuilder.append("\"").append(joinColumn).append("\" = ? AND ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 5);
        @Language("SQL") String sql = sqlBuilder.toString();
        return SQLTransaction.Statement.of(sql, sql);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof PersistentManyToManyCollectionImpl<?> that)) return false;
        boolean equals = Objects.equals(type, that.type) &&
                Objects.equals(metadata.getJoinTableSchema(holder.getDataManager()),
                        that.metadata.getJoinTableSchema(that.holder.getDataManager())) &&
                Objects.equals(metadata.getJoinTableName(holder.getDataManager()),
                        that.metadata.getJoinTableName(that.holder.getDataManager())) &&
                Objects.equals(metadata.getJoinTableToDataTableLinks(holder.getDataManager()),
                        that.metadata.getJoinTableToDataTableLinks(that.holder.getDataManager())) &&
                Objects.equals(metadata.getJoinTableToReferencedTableLinks(holder.getDataManager()),
                        that.metadata.getJoinTableToReferencedTableLinks(that.holder.getDataManager()));

        if (!equals) {
            return false;
        }

        Set<ColumnValuePairs> ids = getIds();
        Set<ColumnValuePairs> thatIds = that.getIds();

        return ids.equals(thatIds);
    }

    @Override
    public int hashCode() {
        int hash = Objects.hash(type,
                metadata.getJoinTableSchema(holder.getDataManager()),
                metadata.getJoinTableName(holder.getDataManager()),
                metadata.getJoinTableToDataTableLinks(holder.getDataManager()),
                metadata.getJoinTableToReferencedTableLinks(holder.getDataManager()));

        int arrayHash = 0; // the ids will not always be in the same order, so ensure this is commutative
        for (ColumnValuePairs idColumns : getIds()) {
            arrayHash += idColumns.hashCode();
        }

        hash = 31 * hash + arrayHash;
        return hash;
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
