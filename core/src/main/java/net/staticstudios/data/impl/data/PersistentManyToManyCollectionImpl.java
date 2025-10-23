package net.staticstudios.data.impl.data;

import com.google.common.base.Preconditions;
import net.staticstudios.data.*;
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

public class PersistentManyToManyCollectionImpl<T extends UniqueData> implements PersistentCollection<T> {
    private final UniqueData holder;
    private final Class<T> type;
    private final String parsedJoinTableSchema;
    private final String parsedJoinTableName;
    private final String links; // since we need information about the column prefixes in the join table, we have to compute these at runtime
    private @Nullable List<ForeignKey.Link> cachedJoinTableToDataTableLinks = null;
    private @Nullable List<ForeignKey.Link> cachedJoinTableToReferencedTableLinks = null;

    public PersistentManyToManyCollectionImpl(UniqueData holder, Class<T> type, String parsedJoinTableSchema, String parsedJoinTableName, String links) {
        this.holder = holder;
        this.type = type;
        this.parsedJoinTableSchema = parsedJoinTableSchema;
        this.parsedJoinTableName = parsedJoinTableName;
        this.links = links;
    }

    public static <T extends UniqueData> void createAndDelegate(ProxyPersistentCollection<T> proxy, PersistentManyToManyCollectionMetadata metadata) {
        PersistentManyToManyCollectionImpl<T> impl = new PersistentManyToManyCollectionImpl<>(proxy.getHolder(), proxy.getReferenceType(), metadata.getParsedJoinTableSchema(), metadata.getParsedJoinTableName(), metadata.getLinks());
        proxy.setDelegate(impl);
    }

    @SuppressWarnings("unchecked")
    public static <T extends UniqueData> PersistentManyToManyCollectionImpl<T> create(UniqueData holder, PersistentManyToManyCollectionMetadata metadata) {
        return new PersistentManyToManyCollectionImpl<>(holder, (Class<T>) metadata.getDataType(), metadata.getParsedJoinTableSchema(), metadata.getParsedJoinTableName(), metadata.getLinks());
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
            PersistentManyToManyCollectionMetadata metadata = new PersistentManyToManyCollectionMetadata(referencedClass, parsedJoinTableSchemaName, parsedJoinTableName, links);
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
        return "";
    }

    public static List<ForeignKey.Link> getJoinTableToDataTableLinks(String dataTable, String links) {
        List<ForeignKey.Link> joinTableToDataTableLinks = new ArrayList<>();
        String dataTableColumnPrefix = getDataTableColumnPrefix(dataTable);
        for (ForeignKey.Link link : SQLBuilder.parseLinks(links)) {
            String columnInDataTable = link.columnInReferringTable();
            String dataColumnInJoinTable = dataTableColumnPrefix + "_" + columnInDataTable;

            joinTableToDataTableLinks.add(new ForeignKey.Link(columnInDataTable, dataColumnInJoinTable));
        }
        return joinTableToDataTableLinks;
    }

    public static List<ForeignKey.Link> getJoinTableToReferencedTableLinks(String dataTable, String referencedTable, String links) {
        List<ForeignKey.Link> joinTableToReferencedTableLinks = new ArrayList<>();
        String referencedTableColumnPrefix = getReferencedTableColumnPrefix(dataTable, referencedTable);
        for (ForeignKey.Link link : SQLBuilder.parseLinks(links)) {
            String columnInReferencedTable = link.columnInReferencedTable();
            String referencedColumnInJoinTable = referencedTableColumnPrefix + "_" + columnInReferencedTable;

            joinTableToReferencedTableLinks.add(new ForeignKey.Link(columnInReferencedTable, referencedColumnInJoinTable));
        }
        return joinTableToReferencedTableLinks;
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
        if (c.isEmpty()) { //this operation isn't cheap, so we should avoid it if we can
            return false;
        }

        DataAccessor dataAccessor = holder.getDataManager().getDataAccessor();
        SQLTransaction.Statement selectDataIdsStatement = buildSelectDataIdsStatement();
        SQLTransaction.Statement selectReferencedIdsStatement = buildSelectReferencedIdsStatement();
        SQLTransaction.Statement updateStatement = buildUpdateStatement();
        List<ForeignKey.Link> joinTableToDataTableLinks = getCachedJoinTableToDataTableLinks();
        List<ForeignKey.Link> joinTableToReferencedTableLinks = getCachedJoinTableToReferencedTableLinks();

        List<Object> holderLinkingValues = new ArrayList<>(joinTableToDataTableLinks.size());
        List<Object> holderIdValues = holder.getIdColumns().stream().map(ColumnValuePair::value).toList();

        SQLTransaction transaction = new SQLTransaction();
        transaction.query(selectDataIdsStatement, () -> holderIdValues, rs -> {
            try {
                Preconditions.checkState(rs.next(), "Could not find holder row in database");
                for (ForeignKey.Link entry : joinTableToDataTableLinks) {
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
                    for (ForeignKey.Link _entry : joinTableToReferencedTableLinks) {
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
        List<ColumnValuePair[]> idsToRemove = new ArrayList<>();
        for (Object o : c) {
            if (!type.isInstance(o)) {
                continue;
            }
            T data = type.cast(o);
            ColumnValuePair[] idColumns = data.getIdColumns().getPairs();
            idsToRemove.add(idColumns);
        }
        return removeIds(idsToRemove);
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
        List<ForeignKey.Link> joinTableToDataTableLinks = getCachedJoinTableToDataTableLinks();

        List<Object> holderLinkingValues = new ArrayList<>(joinTableToDataTableLinks.size());
        List<Object> holderIdValues = holder.getIdColumns().stream().map(ColumnValuePair::value).toList();

        SQLTransaction transaction = new SQLTransaction();
        transaction.query(selectDataIdsStatement, () -> holderIdValues, rs -> {
            try {
                Preconditions.checkState(rs.next(), "Could not find holder row in database");
                for (ForeignKey.Link entry : joinTableToDataTableLinks) {
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


    public boolean removeIds(List<ColumnValuePair[]> idsToRemove) {
        if (idsToRemove.isEmpty()) { //this operation isn't cheap, so we should avoid it if we can
            return false;
        }

        DataAccessor dataAccessor = holder.getDataManager().getDataAccessor();
        SQLTransaction.Statement selectDataIdsStatement = buildSelectDataIdsStatement();
        SQLTransaction.Statement selectReferencedIdsStatement = buildSelectReferencedIdsStatement();
        SQLTransaction.Statement removeStatement = buildRemoveStatement();
        List<ForeignKey.Link> joinTableToDataTableLinks = getCachedJoinTableToDataTableLinks();
        List<ForeignKey.Link> joinTableToReferencedTableLinks = getCachedJoinTableToReferencedTableLinks();

        List<Object> holderLinkingValues = new ArrayList<>(joinTableToDataTableLinks.size());
        List<Object> holderIdValues = holder.getIdColumns().stream().map(ColumnValuePair::value).toList();

        SQLTransaction transaction = new SQLTransaction();
        transaction.query(selectDataIdsStatement, () -> holderIdValues, rs -> {
            try {
                Preconditions.checkState(rs.next(), "Could not find holder row in database");
                for (ForeignKey.Link entry : joinTableToDataTableLinks) {
                    String dataColumn = entry.columnInReferencedTable();
                    Object value = rs.getObject(dataColumn);
                    holderLinkingValues.add(value);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });


        for (ColumnValuePair[] idColumns : idsToRemove) {
            List<Object> referencedIdValues = Arrays.stream(idColumns).map(ColumnValuePair::value).toList();
            List<Object> referencedLinkingValues = new ArrayList<>(joinTableToReferencedTableLinks.size());
            transaction.query(selectReferencedIdsStatement, () -> referencedIdValues, rs -> {
                try {
                    Preconditions.checkState(rs.next(), "Could not find referenced row in database");
                    for (ForeignKey.Link _entry : joinTableToReferencedTableLinks) {
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
    private Set<ColumnValuePair[]> getIds() {
        //todo: this method is slow, plan to cache this later.
        Preconditions.checkArgument(!holder.isDeleted(), "Cannot get entries on a deleted UniqueData instance");
        Set<ColumnValuePair[]> ids = new HashSet<>();
        UniqueDataMetadata holderMetadata = holder.getMetadata();
        UniqueDataMetadata target = holder.getDataManager().getMetadata(type);
        DataAccessor dataAccessor = holder.getDataManager().getDataAccessor();

        String joinTableSchema = getJoinTableSchema(parsedJoinTableSchema, holderMetadata.schema());
        String joinTableName = getJoinTableName(parsedJoinTableName, holderMetadata.table(), target.table());
        List<ForeignKey.Link> joinTableToDataTableLinks = getJoinTableToDataTableLinks(holderMetadata.table(), links);
        List<ForeignKey.Link> joinTableToReferencedTableLinks = getJoinTableToReferencedTableLinks(holderMetadata.table(), target.table(), links);

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT ");
        for (ColumnMetadata columnMetadata : target.idColumns()) {
            sqlBuilder.append("_target.\"").append(columnMetadata.name()).append("\", ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 2);
        sqlBuilder.append(" FROM \"").append(joinTableSchema).append("\".\"").append(joinTableName).append("\" ");
        sqlBuilder.append("INNER JOIN \"").append(holderMetadata.schema()).append("\".\"").append(holderMetadata.table()).append("\" _data ON ");
        for (ForeignKey.Link entry : joinTableToDataTableLinks) {
            String joinColumn = entry.columnInReferringTable();
            String dataColumn = entry.columnInReferencedTable();
            sqlBuilder.append("_data.\"").append(dataColumn).append("\" = \"").append(joinTableSchema).append("\".\"").append(joinTableName).append("\".\"").append(joinColumn).append("\" AND ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 5);
        sqlBuilder.append(" INNER JOIN \"").append(target.schema()).append("\".\"").append(target.table()).append("\" _target ON ");
        for (ForeignKey.Link entry : joinTableToReferencedTableLinks) {
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
                ids.add(idColumns);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return ids;
    }


    private SQLTransaction.Statement buildSelectDataIdsStatement() {
        UniqueDataMetadata holderMetadata = holder.getMetadata();
        List<ForeignKey.Link> joinTableToDataTableLinks = getCachedJoinTableToDataTableLinks();

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT ");
        for (ForeignKey.Link entry : joinTableToDataTableLinks) {
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
        List<ForeignKey.Link> joinTableToReferencedTableLinks = getCachedJoinTableToReferencedTableLinks();

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT ");
        for (ForeignKey.Link entry : joinTableToReferencedTableLinks) {
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
        UniqueDataMetadata holderMetadata = holder.getMetadata();
        UniqueDataMetadata typeMetadata = holder.getDataManager().getMetadata(type);
        List<ForeignKey.Link> joinTableToDataTableLinks = getCachedJoinTableToDataTableLinks();
        List<ForeignKey.Link> joinTableToReferencedTableLinks = getCachedJoinTableToReferencedTableLinks();

        String joinTableSchema = getJoinTableSchema(parsedJoinTableSchema, holderMetadata.schema());
        String joinTableName = getJoinTableName(parsedJoinTableName, holderMetadata.table(), typeMetadata.table());
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("MERGE INTO \"").append(joinTableSchema).append("\".\"").append(joinTableName).append("\" AS _target USING (VALUES (");
        sqlBuilder.append("?, ".repeat(Math.max(0, joinTableToDataTableLinks.size() + joinTableToReferencedTableLinks.size())));
        sqlBuilder.setLength(sqlBuilder.length() - 2);
        sqlBuilder.append(")) AS _source (");
        for (ForeignKey.Link entry : joinTableToDataTableLinks) {
            String joinColumn = entry.columnInReferringTable();
            sqlBuilder.append("\"").append(joinColumn).append("\", ");
        }
        for (ForeignKey.Link entry : joinTableToReferencedTableLinks) {
            String joinColumn = entry.columnInReferringTable();
            sqlBuilder.append("\"").append(joinColumn).append("\", ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 2);
        sqlBuilder.append(") ON ");
        for (ForeignKey.Link entry : joinTableToDataTableLinks) {
            String joinColumn = entry.columnInReferringTable();
            sqlBuilder.append("_target.\"").append(joinColumn).append("\" = _source.\"").append(joinColumn).append("\" AND ");
        }
        for (ForeignKey.Link entry : joinTableToReferencedTableLinks) {
            String joinColumn = entry.columnInReferringTable();
            sqlBuilder.append("_target.\"").append(joinColumn).append("\" = _source.\"").append(joinColumn).append("\" AND ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 5);
        sqlBuilder.append(" WHEN NOT MATCHED THEN INSERT (");
        for (ForeignKey.Link entry : joinTableToDataTableLinks) {
            String joinColumn = entry.columnInReferringTable();
            sqlBuilder.append("\"").append(joinColumn).append("\", ");
        }
        for (ForeignKey.Link entry : joinTableToReferencedTableLinks) {
            String joinColumn = entry.columnInReferringTable();
            sqlBuilder.append("\"").append(joinColumn).append("\", ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 2);
        sqlBuilder.append(") VALUES (");
        for (ForeignKey.Link entry : joinTableToDataTableLinks) {
            String joinColumn = entry.columnInReferringTable();
            sqlBuilder.append("_source.\"").append(joinColumn).append("\", ");
        }
        for (ForeignKey.Link entry : joinTableToReferencedTableLinks) {
            String joinColumn = entry.columnInReferringTable();
            sqlBuilder.append("_source.\"").append(joinColumn).append("\", ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 2);
        sqlBuilder.append(")");
        @Language("SQL") String h2MergeSql = sqlBuilder.toString();

        sqlBuilder.setLength(0);
        sqlBuilder.append("INSERT INTO \"").append(joinTableSchema).append("\".\"").append(joinTableName).append("\" (");
        for (ForeignKey.Link entry : joinTableToDataTableLinks) {
            String joinColumn = entry.columnInReferringTable();
            sqlBuilder.append("\"").append(joinColumn).append("\", ");
        }
        for (ForeignKey.Link entry : joinTableToReferencedTableLinks) {
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

    private SQLTransaction.Statement buildRemoveStatement() {
        UniqueDataMetadata holderMetadata = holder.getMetadata();
        UniqueDataMetadata typeMetadata = holder.getDataManager().getMetadata(type);
        List<ForeignKey.Link> joinTableToDataTableLinks = getCachedJoinTableToDataTableLinks();
        List<ForeignKey.Link> joinTableToReferencedTableLinks = getCachedJoinTableToReferencedTableLinks();

        String joinTableSchema = getJoinTableSchema(parsedJoinTableSchema, holderMetadata.schema());
        String joinTableName = getJoinTableName(parsedJoinTableName, holderMetadata.table(), typeMetadata.table());
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("DELETE FROM \"").append(joinTableSchema).append("\".\"").append(joinTableName).append("\" WHERE ");
        for (ForeignKey.Link entry : joinTableToDataTableLinks) {
            String joinColumn = entry.columnInReferringTable();
            sqlBuilder.append("\"").append(joinColumn).append("\" = ? AND ");
        }
        for (ForeignKey.Link entry : joinTableToReferencedTableLinks) {
            String joinColumn = entry.columnInReferringTable();
            sqlBuilder.append("\"").append(joinColumn).append("\" = ? AND ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 5);
        @Language("SQL") String sql = sqlBuilder.toString();
        return SQLTransaction.Statement.of(sql, sql);
    }

    private SQLTransaction.Statement buildClearStatement() {
        UniqueDataMetadata holderMetadata = holder.getMetadata();
        UniqueDataMetadata typeMetadata = holder.getDataManager().getMetadata(type);
        List<ForeignKey.Link> joinTableToDataTableLinks = getCachedJoinTableToDataTableLinks();

        String joinTableSchema = getJoinTableSchema(parsedJoinTableSchema, holderMetadata.schema());
        String joinTableName = getJoinTableName(parsedJoinTableName, holderMetadata.table(), typeMetadata.table());
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("DELETE FROM \"").append(joinTableSchema).append("\".\"").append(joinTableName).append("\" WHERE ");
        for (ForeignKey.Link entry : joinTableToDataTableLinks) {
            String joinColumn = entry.columnInReferringTable();
            sqlBuilder.append("\"").append(joinColumn).append("\" = ? AND ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 5);
        @Language("SQL") String sql = sqlBuilder.toString();
        return SQLTransaction.Statement.of(sql, sql);
    }

    private List<ForeignKey.Link> getCachedJoinTableToDataTableLinks() {
        if (cachedJoinTableToDataTableLinks == null) {
            UniqueDataMetadata holderMetadata = holder.getMetadata();
            cachedJoinTableToDataTableLinks = getJoinTableToDataTableLinks(holderMetadata.table(), links);
        }
        return cachedJoinTableToDataTableLinks;
    }

    private List<ForeignKey.Link> getCachedJoinTableToReferencedTableLinks() {
        if (cachedJoinTableToReferencedTableLinks == null) {
            UniqueDataMetadata holderMetadata = holder.getMetadata();
            UniqueDataMetadata typeMetadata = holder.getDataManager().getMetadata(type);
            cachedJoinTableToReferencedTableLinks = getJoinTableToReferencedTableLinks(holderMetadata.table(), typeMetadata.table(), links);
        }
        return cachedJoinTableToReferencedTableLinks;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof PersistentManyToManyCollectionImpl<?> that)) return false;
        UniqueDataMetadata holderMetadata = holder.getMetadata();
        UniqueDataMetadata thatHolderMetadata = that.holder.getMetadata();
        UniqueDataMetadata typeMetadata = holder.getDataManager().getMetadata(type);
        boolean equals = Objects.equals(type, that.type) &&
                Objects.equals(getJoinTableSchema(parsedJoinTableSchema, holderMetadata.schema()),
                        getJoinTableSchema(that.parsedJoinTableSchema, thatHolderMetadata.schema())) &&
                Objects.equals(getJoinTableName(parsedJoinTableName, holderMetadata.table(), typeMetadata.table()),
                        getJoinTableName(that.parsedJoinTableName, thatHolderMetadata.table(), typeMetadata.table())) &&
                Objects.equals(getJoinTableToDataTableLinks(holderMetadata.table(), links),
                        getJoinTableToDataTableLinks(thatHolderMetadata.table(), that.links)) &&
                Objects.equals(getJoinTableToReferencedTableLinks(holderMetadata.table(), typeMetadata.table(), links),
                        getJoinTableToReferencedTableLinks(thatHolderMetadata.table(), typeMetadata.table(), that.links));

        if (!equals) {
            return false;
        }

        Set<ColumnValuePair[]> ids = getIds();
        Set<ColumnValuePair[]> thatIds = that.getIds();
        if (ids.size() != thatIds.size()) {
            return false;
        }

        for (ColumnValuePair[] idColumns : ids) {
            boolean found = false;
            for (ColumnValuePair[] thatIdColumns : thatIds) {
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
    public int hashCode() {
        UniqueDataMetadata holderMetadata = holder.getMetadata();
        UniqueDataMetadata typeMetadata = holder.getDataManager().getMetadata(type);
        int hash = Objects.hash(type,
                getJoinTableSchema(parsedJoinTableSchema, holderMetadata.schema()),
                getJoinTableName(parsedJoinTableName, holderMetadata.table(), typeMetadata.table()),
                getJoinTableToDataTableLinks(holderMetadata.table(), links),
                getJoinTableToReferencedTableLinks(holderMetadata.table(), typeMetadata.table(), links));

        int arrayHash = 0; // the ids will not always be in the same order, so ensure this is commutative
        for (ColumnValuePair[] idColumns : getIds()) {
            arrayHash += Arrays.hashCode(idColumns);
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
            removeIds(Collections.singletonList(ids.get(index - 1)));
            ids.remove(--index);
        }
    }
}
