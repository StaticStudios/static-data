package net.staticstudios.data.query;

import com.google.common.base.Preconditions;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.query.clause.AndClause;
import net.staticstudios.data.query.clause.Clause;
import net.staticstudios.data.query.clause.OrClause;
import net.staticstudios.data.query.clause.ValueClause;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

public abstract class AbstractQueryBuilder<Q extends AbstractQueryBuilder<Q, C, T>,
        C extends AbstractConditionalBuilder<Q, C, T>,
        T extends UniqueData>
        implements QueryLike<T> {
    protected final DataManager dataManager;
    private final Class<T> type;
    protected boolean temp = false;
    protected State state = State.NONE;
    protected Clause clause = null;
    private int limit = -1;
    private int offset = -1;
    private String orderBySchema;
    private String orderByTable;
    private String orderByColumn;
    private Order order = Order.ASCENDING;

    protected AbstractQueryBuilder(DataManager dataManager, Class<T> type) {
        this.dataManager = dataManager;
        this.type = type;
    }

    public Q and() {
        state = State.AND;
        return self();
    }

    public Q or() {
        state = State.OR;
        return self();
    }

    private ComputedClause compute(int limit, int offset) {
        Preconditions.checkState(!temp, "Cannot call compute on a temporary query builder");
        Preconditions.checkNotNull(clause, "No clause defined");
        StringBuilder sb = new StringBuilder("WHERE ");
        List<Object> parameters = clause.append(sb);
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

    public String toString() {
        return compute(limit, offset).sql();
    }

    protected void orderBy(String schema, String table, String column, Order order) {
        this.orderBySchema = schema;
        this.orderByTable = table;
        this.orderByColumn = column;
        this.order = order;
    }

    @SuppressWarnings("unchecked")
    protected Q self() {
        return (Q) this;
    }

    @Override
    public @Nullable T findOne() {
        ComputedClause computed = compute(1, -1);
        List<T> result = dataManager.query(type, computed.sql(), computed.parameters());
        if (result.isEmpty()) {
            return null;
        }
        return result.getFirst();
    }

    @Override
    public @NotNull List<T> findAll() {
        ComputedClause computed = compute(limit, offset);
        return dataManager.query(type, computed.sql(), computed.parameters());
    }

    @Override
    public Q limit(int limit) {
        this.limit = limit;
        return self();
    }

    @Override
    public Q offset(int offset) {
        this.offset = offset;
        return self();
    }


    protected C set(Clause clause) {
        switch (state) {
            case NONE -> this.clause = clause;
            case AND -> {
                if (!(clause instanceof ValueClause valueClause)) {
                    throw new IllegalStateException("AND clause must be a ValueClause");
                }
                this.clause = new AndClause(this.clause, valueClause);
            }
            case OR -> {
                if (!(clause instanceof ValueClause valueClause)) {
                    throw new IllegalStateException("OR clause must be a ValueClause");
                }
                this.clause = new OrClause(this.clause, valueClause);
            }
        }

        state = State.NONE;
        return createConditionalInstance();
    }

    protected abstract AbstractQueryBuilder<Q, C, T> createInstance();

    protected abstract C createConditionalInstance();

    protected enum State {
        NONE,
        AND,
        OR
    }

    record ComputedClause(String sql, List<Object> parameters) {
    }

}
