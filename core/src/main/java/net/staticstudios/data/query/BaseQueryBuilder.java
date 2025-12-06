package net.staticstudios.data.query;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.Order;
import net.staticstudios.data.UniqueData;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unused")
public abstract class BaseQueryBuilder<T extends UniqueData, W extends BaseQueryWhere> {
    protected final DataManager dataManager;
    protected final Class<T> type;
    protected final W where;
    private String orderBySchema = null;
    private String orderByTable = null;
    private String orderByColumn = null;
    private Order order = null;
    private int limit = -1;
    private int offset = -1;

    protected BaseQueryBuilder(DataManager dataManager, Class<T> type, W where) {
        this.dataManager = dataManager;
        this.type = type;
        this.where = where;
    }

    protected void setOrderBy(String schema, String table, String column, Order order) {
        this.orderBySchema = schema;
        this.orderByTable = table;
        this.orderByColumn = column;
        this.order = order;
    }

    protected void setLimit(int limit) {
        this.limit = limit;
    }

    protected void setOffset(int offset) {
        this.offset = offset;
    }

    public @Nullable T findOne() {
        ComputedClause computed = compute();
        List<T> result = dataManager.query(type, computed.sql(), computed.parameters());
        if (result.isEmpty()) {
            return null;
        }
        return result.getFirst();
    }

    public @NotNull List<T> findAll() { //todo: in the IJ plugin make sure the annotations are present for find all and fine one (nullable and notnull)
        ComputedClause computed = compute();
        return dataManager.query(type, computed.sql(), computed.parameters());
    }


    private ComputedClause compute() {
        StringBuilder sb = new StringBuilder();
        List<Object> parameters = new ArrayList<>();
        if (!where.isEmpty()) {
            for (InnerJoin join : where.getInnerJoins()) {
                sb.append("INNER JOIN \"").append(join.referencedSchema()).append("\".\"").append(join.referencedTable()).append("\" ON ");
                for (int i = 0; i < join.columnsInReferringTable().length; i++) {
                    sb.append("\"").append(join.referringSchema()).append("\".\"").append(join.referringTable()).append("\".\"").append(join.columnsInReferringTable()[i]).append("\" = \"")
                            .append(join.referencedSchema()).append("\".\"").append(join.referencedTable()).append("\".\"").append(join.columnsInReferencedTable()[i]).append("\"");
                    if (i < join.columnsInReferringTable().length - 1) {
                        sb.append(" AND ");
                    }
                }
                sb.append(" ");
            }
            sb.append("WHERE ");

            where.buildWhereClause(sb, parameters);
        }
        if (limit > 0) {
            sb.append(" LIMIT ").append(limit);
        }
        if (offset > 0) {
            sb.append(" OFFSET ").append(offset);
        }
        if (orderByColumn != null) {
            sb.append(" ORDER BY \"").append(orderBySchema).append("\".\"").append(orderByTable).append("\".\"").append(orderByColumn).append("\" ").append(order == Order.ASCENDING ? "ASC" : "DESC");
        }
        return new ComputedClause(sb.toString(), parameters);
    }

    @Override
    public String toString() {
        return compute().sql();
    }

    record ComputedClause(String sql, List<Object> parameters) {
    }
}
