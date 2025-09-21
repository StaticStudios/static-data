package net.staticstudios.data.impl.data;

import com.google.common.base.Preconditions;
import net.staticstudios.data.DataAccessor;
import net.staticstudios.data.OneToOne;
import net.staticstudios.data.Reference;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.util.*;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReferenceImpl<T extends UniqueData> implements Reference<T> {
    private final UniqueData holder;
    private final Class<T> type;
    private final Map<String, String> link;

    public ReferenceImpl(UniqueData holder, Class<T> type, Map<String, String> link) {
        this.holder = holder;
        this.type = type;
        this.link = link;
    }

    public static <T extends UniqueData> void createAndDelegate(Reference.ProxyReference<T> proxy, Map<String, String> link) {
        ReferenceImpl<T> delegate = new ReferenceImpl<>(
                proxy.getHolder(),
                proxy.getReferenceType(),
                link
        );
        proxy.setDelegate(delegate);
    }

    public static <T extends UniqueData> ReferenceImpl<T> create(UniqueData holder, Class<T> type, Map<String, String> link) {
        return new ReferenceImpl<>(holder, type, link);
    }

    public static <T extends UniqueData> void delegate(T instance) {
        for (FieldInstancePair<@Nullable Reference> pair : ReflectionUtils.getFieldInstancePairs(instance, Reference.class)) {
            OneToOne oneToOneAnnotation = pair.field().getAnnotation(OneToOne.class);
            Preconditions.checkNotNull(oneToOneAnnotation, "Field %s in class %s is missing @OneToOne annotation".formatted(pair.field().getName(), instance.getClass().getName()));
            Class<?> referencedClass = ReflectionUtils.getGenericType(pair.field());
            Preconditions.checkNotNull(referencedClass, "Field %s in class %s is not parameterized".formatted(pair.field().getName(), instance.getClass().getName()));
            Map<String, String> link = new HashMap<>();
            for (String l : StringUtils.parseCommaSeperatedList(oneToOneAnnotation.link())) {
                String[] split = l.split("=");
                Preconditions.checkArgument(split.length == 2, "Invalid link format in @OneToOne annotation on field %s in class %s".formatted(pair.field().getName(), instance.getClass().getName()));
                link.put(ValueUtils.parseValue(split[0].trim()), ValueUtils.parseValue(split[1].trim()));
            }

            if (pair.instance() instanceof Reference.ProxyReference<?> proxyRef) {
                createAndDelegate(proxyRef, link);
            } else {
                pair.field().setAccessible(true);
                try {
                    pair.field().set(instance, create(instance, (Class<? extends UniqueData>) referencedClass, link));
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
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
        ColumnValuePair[] idColumns = new ColumnValuePair[link.size()];
        int i = 0;
        UniqueDataMetadata holderMetadata = holder.getMetadata();
        DataAccessor dataAccessor = holder.getDataManager().getDataAccessor();
        for (Map.Entry<String, String> entry : link.entrySet()) {
            String myColumn = entry.getKey();
            String theirColumn = entry.getValue();

            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("SELECT \"").append(myColumn).append("\" FROM \"").append(holderMetadata.schema()).append("\".\"").append(holderMetadata.table()).append("\" WHERE ");
            for (ColumnValuePair columnValuePair : holder.getIdColumns()) {
                sqlBuilder.append("\"").append(columnValuePair.column()).append("\" = ? AND ");
            }
            sqlBuilder.setLength(sqlBuilder.length() - 5);
            @Language("SQL") String sql = sqlBuilder.toString();
            try (ResultSet rs = dataAccessor.executeQuery(sql, holder.getIdColumns().stream().map(ColumnValuePair::value).toList())) {
                if (rs.next()) {
                    if (rs.getObject(myColumn) == null) {
                        return null;
                    }
                    idColumns[i++] = new ColumnValuePair(theirColumn, rs.getObject(myColumn));
                } else {
                    return null;
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        return holder.getDataManager().getInstance(type, idColumns);
    }

    @Override
    public void set(@Nullable T value) {
        UniqueDataMetadata holderMetadata = holder.getMetadata();
        List<Object> values = new ArrayList<>();
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("UPDATE \"").append(holderMetadata.schema()).append("\".\"").append(holderMetadata.table()).append("\" SET ");
        for (Map.Entry<String, String> entry : link.entrySet()) {
            String myColumn = entry.getKey();
            if (value == null) {
                sqlBuilder.append("\"").append(myColumn).append("\" = NULL, ");
                continue;
            }
            
            String theirColumn = entry.getValue();
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

        try {
            holder.getDataManager().getDataAccessor().executeUpdate(sqlBuilder.toString(), values);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
