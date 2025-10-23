package net.staticstudios.data.query;

import com.google.common.base.Preconditions;
import net.staticstudios.data.Order;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.query.clause.AndClause;
import net.staticstudios.data.query.clause.OrClause;
import net.staticstudios.data.query.clause.ParenthesisClause;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.function.Consumer;

public abstract class AbstractConditionalBuilder<Q extends AbstractQueryBuilder<Q, C, T>, C extends AbstractConditionalBuilder<Q, C, T>, T extends UniqueData> implements QueryLike<T> {
    protected final AbstractQueryBuilder<Q, C, T> queryBuilder;

    public AbstractConditionalBuilder(AbstractQueryBuilder<Q, C, T> queryBuilder) {
        this.queryBuilder = queryBuilder;
    }

    /**
     * AND ...
     */
    public Q and() {
        queryBuilder.state = AbstractQueryBuilder.State.AND;
        return queryBuilder.self();
    }

    /**
     * OR ...
     */
    public Q or() {
        queryBuilder.state = AbstractQueryBuilder.State.OR;
        return queryBuilder.self();
    }

    /**
     * OR (...)
     */
    public Q or(Consumer<Q> conditional) {
        AbstractQueryBuilder<Q, C, T> inner = queryBuilder.createInstance();
        inner.temp = true;
        conditional.accept(inner.self());
        Preconditions.checkState(inner.state == AbstractQueryBuilder.State.NONE, "Clause not completed");

        queryBuilder.set(new OrClause(queryBuilder.clause, new ParenthesisClause(inner.clause)));

        return queryBuilder.self();
    }

    /**
     * AND (...)
     */
    public Q and(Consumer<Q> conditional) {
        AbstractQueryBuilder<Q, C, T> inner = queryBuilder.createInstance();
        inner.temp = true;
        conditional.accept(inner.self());
        Preconditions.checkState(inner.state == AbstractQueryBuilder.State.NONE, "Clause not completed");

        queryBuilder.set(new AndClause(queryBuilder.clause, new ParenthesisClause(inner.clause)));

        return queryBuilder.self();
    }

    @SuppressWarnings("unchecked")
    private C self() {
        return (C) this;
    }

    @Override
    public C limit(int limit) {
        queryBuilder.limit(limit);
        return self();
    }

    @Override
    public C offset(int offset) {
        queryBuilder.offset(offset);
        return self();
    }

    @Override
    public @Nullable T findOne() {
        return queryBuilder.findOne();
    }

    @Override
    public @NotNull List<T> findAll() {
        return queryBuilder.findAll();
    }

    protected void orderBy(String schema, String table, String column, Order order) {
        queryBuilder.orderBy(schema, table, column, order);
    }

    protected void innerJoin(String schema, String table, String[] columns, String foreignSchema, String foreignTable, String[] foreignColumns) {
        queryBuilder.innerJoin(schema, table, columns, foreignSchema, foreignTable, foreignColumns);
    }

    @Override
    public String toString() {
        return queryBuilder.toString();
    }
}