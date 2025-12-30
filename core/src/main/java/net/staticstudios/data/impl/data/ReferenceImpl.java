package net.staticstudios.data.impl.data;

import com.google.common.base.Preconditions;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReferenceImpl<T extends UniqueData> implements Reference<T> {
    private final UniqueData holder;
    private final Class<T> type;
    private final List<Link> link;

    public ReferenceImpl(UniqueData holder, Class<T> type, List<Link> link) {
        this.holder = holder;
        this.type = type;
        this.link = link;
    }

    public static <T extends UniqueData> void createAndDelegate(Reference.ProxyReference<T> proxy, ReferenceMetadata metadata) {
        ReferenceImpl<T> delegate = new ReferenceImpl<>(
                proxy.getHolder(),
                proxy.getReferenceType(),
                metadata.links()
        );
        proxy.setDelegate(metadata, delegate);
    }

    public static <T extends UniqueData> ReferenceImpl<T> create(UniqueData holder, Class<T> type, List<Link> link) {
        return new ReferenceImpl<>(holder, type, link);
    }

    public static <T extends UniqueData> void delegate(T instance) {
        UniqueDataMetadata metadata = instance.getDataManager().getMetadata(instance.getClass());
        for (FieldInstancePair<@Nullable Reference> pair : ReflectionUtils.getFieldInstancePairs(instance, Reference.class)) {
            ReferenceMetadata refMetadata = metadata.referenceMetadata().get(pair.field());

            if (pair.instance() instanceof Reference.ProxyReference<?> proxyRef) {
                createAndDelegate(proxyRef, refMetadata);
            } else {
                pair.field().setAccessible(true);
                try {
                    pair.field().set(instance, create(instance, refMetadata.referencedClass(), refMetadata.links()));
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static <T extends UniqueData> Map<Field, ReferenceMetadata> extractMetadata(Class<T> clazz) {
        Map<Field, ReferenceMetadata> metadataMap = new HashMap<>();
        for (Field field : ReflectionUtils.getFields(clazz, Reference.class)) {
            OneToOne oneToOneAnnotation = field.getAnnotation(OneToOne.class);
            Preconditions.checkNotNull(oneToOneAnnotation, "Field %s in class %s is missing @OneToOne annotation".formatted(field.getName(), clazz.getName()));
            Class<?> referencedClass = ReflectionUtils.getGenericType(field);
            Preconditions.checkNotNull(referencedClass, "Field %s in class %s is not parameterized".formatted(field.getName(), clazz.getName()));
            metadataMap.put(field, new ReferenceMetadata(clazz, (Class<? extends UniqueData>) referencedClass, SQLBuilder.parseLinks(oneToOneAnnotation.link()), oneToOneAnnotation.fkey()));
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
        ColumnValuePair[] idColumns = new ColumnValuePair[link.size()];
        int i = 0;
        UniqueDataMetadata holderMetadata = holder.getMetadata();
        UniqueDataMetadata referencedMetadata = holder.getDataManager().getMetadata(type);
        DataAccessor dataAccessor = holder.getDataManager().getDataAccessor();
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT ");
        for (ColumnMetadata idColumn : referencedMetadata.idColumns()) {
            sqlBuilder.append("_referenced.\"").append(idColumn.name()).append("\", ");
        }
        for (Link entry : link) {
            String myColumn = entry.columnInReferringTable();
            sqlBuilder.append("_referring.\"").append(myColumn).append("\", ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 2);

        sqlBuilder.append(" FROM \"").append(referencedMetadata.schema()).append("\".\"").append(referencedMetadata.table()).append("\" _referenced");
        sqlBuilder.append(" INNER JOIN \"").append(holderMetadata.schema()).append("\".\"").append(holderMetadata.table()).append("\" _referring ON ");
        for (Link entry : link) {
            String myColumn = entry.columnInReferringTable();
            String theirColumn = entry.columnInReferencedTable();
            sqlBuilder.append("_referenced.\"").append(theirColumn).append("\" = ");
            sqlBuilder.append("_referring.\"").append(myColumn).append("\" AND ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 5);

        sqlBuilder.append(" WHERE ");

        for (ColumnValuePair columnValuePair : holder.getIdColumns()) {
            sqlBuilder.append("_referring.\"").append(columnValuePair.column()).append("\" = ? AND ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 5);

        @Language("SQL") String sql = sqlBuilder.toString();
        try (ResultSet rs = dataAccessor.executeQuery(sql, holder.getIdColumns().stream().map(ColumnValuePair::value).toList())) {
            if (!rs.next()) {
                return null;
            }

            for (Link entry : link) {
                String myColumn = entry.columnInReferringTable();
                String theirColumn = entry.columnInReferencedTable();
                if (rs.getObject(myColumn) == null) {
                    return null;
                }
                idColumns[i++] = new ColumnValuePair(theirColumn, rs.getObject(myColumn));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return new ColumnValuePairs(idColumns);
    }

    @Override
    public void set(@Nullable T value) {
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
            Preconditions.checkNotNull(theirValue, "Could not find value for name %s in referenced object of type %s".formatted(theirColumn, type.getName()));

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
}
