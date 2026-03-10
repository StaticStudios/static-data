package net.staticstudios.data.impl.data;

import com.google.common.base.Preconditions;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.OneToOne;
import net.staticstudios.data.Reference;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.impl.DataAccessor;
import net.staticstudios.data.parse.SQLBuilder;
import net.staticstudios.data.util.*;
import net.staticstudios.data.utils.Link;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class ReferenceImpl<T extends UniqueData> implements Reference<T> {
    private final UniqueData holder;
    private final Class<T> type;
    private final List<Link> link;
    private final boolean updateReferencedTable;
    private final ReferenceMetadata metadata;

    public ReferenceImpl(UniqueData holder, ReferenceMetadata metadata) {
        this.holder = holder;
        this.type = (Class<T>) metadata.referencedClass();
        this.link = metadata.links();
        this.updateReferencedTable = metadata.updateReferencedTable();
        this.metadata = metadata;
    }

    public static <T extends UniqueData> void createAndDelegate(Reference.ProxyReference<T> proxy, ReferenceMetadata metadata) {
        ReferenceImpl<T> delegate = new ReferenceImpl<>(
                proxy.getHolder(),
                metadata
        );
        proxy.setDelegate(metadata, delegate);
    }

    public static <T extends UniqueData> ReferenceImpl<T> create(UniqueData holder, Class<T> type, ReferenceMetadata metadata) {
        return new ReferenceImpl<>(holder, metadata);
    }

    public static <T extends UniqueData> void delegate(T instance) {
        UniqueDataMetadata metadata = instance.getDataManager().getMetadata(instance.getClass());
        try {
            for (var entry : metadata.referenceMetadata().entrySet()) {
                Field field = entry.getKey();
                ReferenceMetadata referenceMetadata = entry.getValue();

                Object value = field.get(instance);
                if (value instanceof Reference.ProxyReference<?> proxyRef) {
                    ReferenceImpl.createAndDelegate(proxyRef, referenceMetadata);
                } else {
                    field.set(instance, ReferenceImpl.create(instance, referenceMetadata.referencedClass(), referenceMetadata));
                }
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T extends UniqueData> Map<Field, ReferenceMetadata> extractMetadata(Class<T> clazz) {
        Map<Field, ReferenceMetadata> metadataMap = new HashMap<>();
        for (Field field : ReflectionUtils.getFields(clazz, Reference.class)) {
            field.setAccessible(true);
            OneToOne oneToOneAnnotation = field.getAnnotation(OneToOne.class);
            Preconditions.checkNotNull(oneToOneAnnotation, "Field %s in class %s is missing @OneToOne annotation".formatted(field.getName(), clazz.getName()));
            Class<?> referencedClass = ReflectionUtils.getGenericType(field);
            Preconditions.checkNotNull(referencedClass, "Field %s in class %s is not parameterized".formatted(field.getName(), clazz.getName()));
            metadataMap.put(field, new ReferenceMetadata(clazz, (Class<? extends UniqueData>) referencedClass, SQLBuilder.parseLinks(oneToOneAnnotation.link()), oneToOneAnnotation.fkey(), oneToOneAnnotation.updateReferencedColumns()));
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
    public <U extends UniqueData> Reference<T> onUpdate(Class<U> holderClass, ReferenceUpdateHandler<U, T> updateHandler) {
        throw new UnsupportedOperationException("Dynamically adding update handlers is not supported");
    }

    @Override
    public @Nullable T get() {
        ColumnValuePairs referencedColumnValuePairs = getReferencedColumnValuePairs();
        if (referencedColumnValuePairs == null) {
            return null;
        }
        return holder.getDataManager().getInstance(type, referencedColumnValuePairs);
    }

    public ColumnValuePairs getReferencedColumnValuePairs() {
        Preconditions.checkArgument(!holder.isDeleted(), "Cannot get reference on a deleted UniqueData instance");
        UniqueDataMetadata referencedMetadata = holder.getDataManager().getMetadata(type);
        DataAccessor dataAccessor = holder.getDataManager().getDataAccessor();

        DataManager dataManager = holder.getDataManager();


        List<Object> values = new ArrayList<>(holder.getIdColumns().getPairs().length);
        for (ColumnValuePair columnValuePair : holder.getIdColumns()) {
            values.add(columnValuePair.value());
        }

        SelectQuery query = metadata.buildSelectReferencedColumnValuePairsSelectQuery(holder.getDataManager(), values);

        ReadCacheResult cached = dataManager.getRelationCacheResult(query);

        if (cached != null) {
            ColumnValuePairs columnValuePairs = (ColumnValuePairs) cached.getValue();
            List<ColumnMetadata> refIdColumns = referencedMetadata.idColumns();
            ColumnValuePair[] idColumns = new ColumnValuePair[refIdColumns.size()];
            for (int i = 0; i < refIdColumns.size(); i++) {
                ColumnMetadata idColumn = refIdColumns.get(i);
                Object val = ColumnValuePairs.getValue(idColumn.name(), columnValuePairs);
                if (val == null) {
                    return null;
                }
                idColumns[i] = new ColumnValuePair(idColumn.name(), val);
            }
            return new ColumnValuePairs(idColumns);
        }

        long generation = dataManager.getRelationCacheGeneration();
        try (ResultSet rs = dataAccessor.executeQuery(query.getSql(), query.getValues())) {
            if (!rs.next()) {
                return null;
            }

            for (Link entry : link) {
                String myColumn = entry.columnInReferringTable();
                if (rs.getObject(myColumn) == null) {
                    return null;
                }
            }

            List<ColumnMetadata> refIdColumns = referencedMetadata.idColumns();
            ColumnValuePair[] idColumns = new ColumnValuePair[refIdColumns.size()];
            for (int i = 0; i < refIdColumns.size(); i++) {
                ColumnMetadata idColumn = refIdColumns.get(i);
                Object val = rs.getObject(idColumn.name());
                if (val == null) {
                    return null;
                }
                idColumns[i] = new ColumnValuePair(idColumn.name(), val);
            }

            ColumnValuePairs theirIdColumns = new ColumnValuePairs(idColumns);

            Set<Cell> dependencies = new HashSet<>();
            for (Link entry : link) {
                String myColumn = entry.columnInReferringTable();
                String theirColumn = entry.columnInReferencedTable();
                dependencies.add(new Cell(holder.getMetadata().schema(), holder.getMetadata().table(), myColumn, holder.getIdColumns()));
                dependencies.add(new Cell(referencedMetadata.schema(), referencedMetadata.table(), theirColumn, theirIdColumns));
            }

            for (ColumnValuePair columnValuePair : holder.getIdColumns()) {
                dependencies.add(new Cell(holder.getMetadata().schema(), holder.getMetadata().table(), columnValuePair.column(), holder.getIdColumns()));
            }
            for (ColumnValuePair columnValuePair : theirIdColumns) {
                dependencies.add(new Cell(referencedMetadata.schema(), referencedMetadata.table(), columnValuePair.column(), theirIdColumns));
            }

            ReadCacheResult cacheResult = new ReadCacheResult(theirIdColumns, dependencies);
            dataManager.putRelationCacheResult(query, cacheResult, generation);

            return theirIdColumns;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void set(@Nullable T value) {
        if (updateReferencedTable) {
            setReferenced(value);
        } else {
            setLocal(value);
        }
    }

    private void setLocal(@Nullable T value) {
        Preconditions.checkArgument(!holder.isDeleted(), "Cannot set reference on a deleted UniqueData instance");
        UniqueDataMetadata holderMetadata = holder.getMetadata();
        List<Object> values = new ArrayList<>();
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("UPDATE \"").append(holderMetadata.schema()).append("\".\"").append(holderMetadata.table()).append("\" SET ");
        for (Link entry : link) {
            String myColumn = entry.columnInReferringTable();
            if (value == null) {
                sqlBuilder.append("\"").append(myColumn).append("\" = NULL, ");
                continue;
            }

            String theirColumn = entry.columnInReferencedTable();
            Object theirValue = null;
            for (ColumnValuePair columnValuePair : value.getIdColumns()) {
                if (columnValuePair.column().equals(theirColumn)) {
                    theirValue = columnValuePair.value();
                    break;
                }
            }
            Preconditions.checkNotNull(theirValue, "Could not find value for column %s in referenced object of type %s".formatted(theirColumn, type.getName()));

            sqlBuilder.append("\"").append(myColumn).append("\" = ?, ");
            values.add(theirValue);
        }

        sqlBuilder.setLength(sqlBuilder.length() - 2);
        sqlBuilder.append(" WHERE ");
        for (ColumnValuePair columnValuePair : holder.getIdColumns()) {
            sqlBuilder.append("\"").append(columnValuePair.column()).append("\" = ? AND ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 5);
        for (ColumnValuePair columnValuePair : holder.getIdColumns()) {
            values.add(columnValuePair.value());
        }

        @Language("SQL") String sql = sqlBuilder.toString();

        try {
            holder.getDataManager().getDataAccessor().executeUpdate(SQLTransaction.Statement.of(sql, sql), values, 0);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void setReferenced(@Nullable T value) {
        Preconditions.checkArgument(!holder.isDeleted(), "Cannot set reference on a deleted UniqueData instance");
        UniqueDataMetadata referencedMetadata = holder.getDataManager().getMetadata(type);

        List<Object> linkValues = new ArrayList<>();
        for (Link entry : link) {
            if (value == null) {
                continue;
            }
            String myColumn = entry.columnInReferringTable();
            Object myValue = null;
            for (ColumnValuePair columnValuePair : holder.getIdColumns()) {
                if (columnValuePair.column().equals(myColumn)) {
                    myValue = columnValuePair.value();
                    break;
                }
            }
            Preconditions.checkNotNull(myValue, "Could not find value for column %s in holder of type %s".formatted(myColumn, holder.getClass().getName()));
            linkValues.add(myValue);
        }

        StringBuilder unlinkSqlBuilder = new StringBuilder();
        unlinkSqlBuilder.append("UPDATE \"").append(referencedMetadata.schema()).append("\".\"").append(referencedMetadata.table()).append("\" SET ");
        for (Link entry : link) {
            String theirColumn = entry.columnInReferencedTable();
            unlinkSqlBuilder.append("\"").append(theirColumn).append("\" = NULL, ");
        }
        unlinkSqlBuilder.setLength(unlinkSqlBuilder.length() - 2);
        unlinkSqlBuilder.append(" WHERE ");
        for (Link entry : link) {
            String theirColumn = entry.columnInReferencedTable();
            unlinkSqlBuilder.append("\"").append(theirColumn).append("\" = ? AND ");
        }
        unlinkSqlBuilder.setLength(unlinkSqlBuilder.length() - 5);

        StringBuilder updateSqlBuilder = new StringBuilder();
        updateSqlBuilder.append("UPDATE \"").append(referencedMetadata.schema()).append("\".\"").append(referencedMetadata.table()).append("\" SET ");
        for (Link entry : link) {
            String theirColumn = entry.columnInReferencedTable();
            updateSqlBuilder.append("\"").append(theirColumn).append("\" = ?, ");
        }
        updateSqlBuilder.setLength(updateSqlBuilder.length() - 2);
        updateSqlBuilder.append(" WHERE ");
        for (ColumnMetadata idColumn : referencedMetadata.idColumns()) {
            updateSqlBuilder.append("\"").append(idColumn.name()).append("\" = ? AND ");
        }
        updateSqlBuilder.setLength(updateSqlBuilder.length() - 5);

        List<Object> holderLinkValues = new ArrayList<>();
        StringBuilder selectExistingSqlBuilder = new StringBuilder();
        selectExistingSqlBuilder.append("SELECT ");
        for (ColumnMetadata idColumn : referencedMetadata.idColumns()) {
            selectExistingSqlBuilder.append("\"").append(idColumn.name()).append("\", ");
        }
        selectExistingSqlBuilder.setLength(selectExistingSqlBuilder.length() - 2);
        selectExistingSqlBuilder.append(" FROM \"").append(referencedMetadata.schema()).append("\".\"").append(referencedMetadata.table()).append("\" WHERE ");
        for (Link entry : link) {
            String theirColumn = entry.columnInReferencedTable();
            String myColumn = entry.columnInReferringTable();
            selectExistingSqlBuilder.append("\"").append(theirColumn).append("\" = ? AND ");
            Object myValue = null;
            for (ColumnValuePair columnValuePair : holder.getIdColumns()) {
                if (columnValuePair.column().equals(myColumn)) {
                    myValue = columnValuePair.value();
                    break;
                }
            }
            Preconditions.checkNotNull(myValue, "Could not find value for column %s in holder of type %s".formatted(myColumn, holder.getClass().getName()));
            holderLinkValues.add(myValue);
        }
        selectExistingSqlBuilder.setLength(selectExistingSqlBuilder.length() - 5);

        @Language("SQL") String selectExistingSql = selectExistingSqlBuilder.toString();
        @Language("SQL") String unlinkSql = unlinkSqlBuilder.toString();
        @Language("SQL") String updateSql = updateSqlBuilder.toString();

        List<Object> existingIdValues = new ArrayList<>();
        SQLTransaction transaction = new SQLTransaction();

        transaction.query(SQLTransaction.Statement.of(selectExistingSql, selectExistingSql), () -> holderLinkValues, rs -> {
            try {
                while (rs.next()) {
                    for (ColumnMetadata idColumn : referencedMetadata.idColumns()) {
                        existingIdValues.add(rs.getObject(idColumn.name()));
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        transaction.update(SQLTransaction.Statement.of(unlinkSql, unlinkSql), () -> holderLinkValues, () -> !existingIdValues.isEmpty());

        if (value != null) {
            transaction.update(SQLTransaction.Statement.of(updateSql, updateSql), () -> {
                List<Object> valuesToSet = new ArrayList<>(linkValues);
                for (ColumnValuePair columnValuePair : value.getIdColumns()) {
                    valuesToSet.add(columnValuePair.value());
                }
                return valuesToSet;
            });
        }

        try {
            holder.getDataManager().getDataAccessor().executeTransaction(transaction, 0);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
