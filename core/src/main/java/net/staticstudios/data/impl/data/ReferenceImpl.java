package net.staticstudios.data.impl.data;

import com.google.common.base.Preconditions;
import net.staticstudios.data.*;
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

    public static <T extends UniqueData> void createAndDelegate(Reference.ProxyReference<T> proxy, List<Link> link) {
        ReferenceImpl<T> delegate = new ReferenceImpl<>(
                proxy.getHolder(),
                proxy.getReferenceType(),
                link
        );
        proxy.setDelegate(delegate);
    }

    public static <T extends UniqueData> ReferenceImpl<T> create(UniqueData holder, Class<T> type, List<Link> link) {
        return new ReferenceImpl<>(holder, type, link);
    }

    public static <T extends UniqueData> void delegate(T instance) {
        UniqueDataMetadata metadata = instance.getDataManager().getMetadata(instance.getClass());
        for (FieldInstancePair<@Nullable Reference> pair : ReflectionUtils.getFieldInstancePairs(instance, Reference.class)) {
            ReferenceMetadata refMetadata = metadata.referenceMetadata().get(pair.field());

            if (pair.instance() instanceof Reference.ProxyReference<?> proxyRef) {
                createAndDelegate(proxyRef, refMetadata.getLinks());
            } else {
                pair.field().setAccessible(true);
                try {
                    pair.field().set(instance, create(instance, refMetadata.getReferencedClass(), refMetadata.getLinks()));
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
            metadataMap.put(field, new ReferenceMetadata((Class<? extends UniqueData>) referencedClass, SQLBuilder.parseLinks(oneToOneAnnotation.link())));
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
    public @Nullable T get() {
        Preconditions.checkArgument(!holder.isDeleted(), "Cannot get reference on a deleted UniqueData instance");
        ColumnValuePair[] idColumns = new ColumnValuePair[link.size()];
        int i = 0;
        UniqueDataMetadata holderMetadata = holder.getMetadata();
        DataAccessor dataAccessor = holder.getDataManager().getDataAccessor();
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT ");
        for (Link entry : link) {
            String myColumn = entry.columnInReferringTable();
            sqlBuilder.append("\"").append(myColumn).append("\", ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 2);
        sqlBuilder.append(" FROM \"").append(holderMetadata.schema()).append("\".\"").append(holderMetadata.table()).append("\" WHERE ");

        for (ColumnValuePair columnValuePair : holder.getIdColumns()) {
            sqlBuilder.append("\"").append(columnValuePair.column()).append("\" = ? AND ");
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

        return holder.getDataManager().getInstance(type, idColumns);
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
