package net.staticstudios.data.query;

import com.google.common.base.Preconditions;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.Order;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.parse.ForeignKey;
import net.staticstudios.data.parse.SQLSchema;
import net.staticstudios.data.parse.SQLTable;
import net.staticstudios.data.util.UniqueDataMetadata;
import net.staticstudios.data.utils.Link;

import java.util.List;
import java.util.function.Function;

public class QueryBuilder<U extends UniqueData> extends BaseQueryBuilder<U, QueryBuilder.QueryWhere> {

    public QueryBuilder(DataManager dataManager, Class<U> type) {
        super(dataManager, type, new QueryWhere(dataManager, dataManager.getMetadata(type)));
    }

    public QueryBuilder<U> where(Function<QueryWhere, QueryWhere> function) {
        function.apply(super.where);
        return this;
    }

    public final QueryBuilder<U> limit(int limit) {
        super.setLimit(limit);
        return this;
    }

    public final QueryBuilder<U> offset(int offset) {
        super.setOffset(offset);
        return this;
    }

    public final QueryBuilder<U> orderBy(String schema, String table, String column, Order order) {
        super.setOrderBy(schema, table, column, order);
        return this;
    }

    public final QueryBuilder<U> orderBy(String column, Order order) {
        super.setOrderBy(super.where.metadata.schema(), super.where.metadata.table(), column, order);
        return this;
    }


    public static class QueryWhere extends BaseQueryWhere {
        private final DataManager dataManager;
        private final UniqueDataMetadata metadata;

        public QueryWhere(DataManager dataManager, UniqueDataMetadata metadata) {
            this.dataManager = dataManager;
            this.metadata = metadata;
        }

        public final QueryWhere group(Function<QueryWhere, QueryWhere> function) {
            super.pushGroup();
            function.apply(this);
            super.popGroup();
            return this;
        }

        public final QueryWhere and() {
            super.andClause();
            return this;
        }

        public final QueryWhere or() {
            super.orClause();
            return this;
        }

        public final QueryWhere is(String schema, String table, String column, Object value) {
            maybeAddInnerJoin(schema, table, column);
            super.equalsClause(schema, table, column, value);
            return this;
        }

        public final QueryWhere is(String column, Object value) {
            super.equalsClause(metadata.schema(), metadata.table(), column, value);
            return this;
        }

        public final QueryWhere isNot(String schema, String table, String column, Object value) {
            maybeAddInnerJoin(schema, table, column);
            super.notEqualsClause(schema, table, column, value);
            return this;
        }

        public final QueryWhere isNot(String column, Object value) {
            super.notEqualsClause(metadata.schema(), metadata.table(), column, value);
            return this;
        }

        public final QueryWhere isIgnoreCase(String schema, String table, String column, String value) {
            maybeAddInnerJoin(schema, table, column);
            super.equalsIgnoreCaseClause(schema, table, column, value);
            return this;
        }

        public final QueryWhere isIgnoreCase(String column, String value) {
            super.equalsIgnoreCaseClause(metadata.schema(), metadata.table(), column, value);
            return this;
        }

        public final QueryWhere isNotIgnoreCase(String schema, String table, String column, String value) {
            maybeAddInnerJoin(schema, table, column);
            super.notEqualsIgnoreCaseClause(schema, table, column, value);
            return this;
        }

        public final QueryWhere isNotIgnoreCase(String column, String value) {
            super.notEqualsIgnoreCaseClause(metadata.schema(), metadata.table(), column, value);
            return this;
        }

        public final QueryWhere isNull(String schema, String table, String column) {
            super.nullClause(schema, table, column);
            return this;
        }

        public final QueryWhere isNull(String column) {
            super.nullClause(metadata.schema(), metadata.table(), column);
            return this;
        }

        public final QueryWhere isNotNull(String schema, String table, String column) {
            super.notNullClause(schema, table, column);
            return this;
        }

        public final QueryWhere isNotNull(String column) {
            super.notNullClause(metadata.schema(), metadata.table(), column);
            return this;
        }

        public final QueryWhere like(String schema, String table, String column, String format) {
            maybeAddInnerJoin(schema, table, column);
            super.likeClause(schema, table, column, format);
            return this;
        }

        public final QueryWhere like(String column, String format) {
            super.likeClause(metadata.schema(), metadata.table(), column, format);
            return this;
        }

        public final QueryWhere notLike(String schema, String table, String column, String format) {
            maybeAddInnerJoin(schema, table, column);
            super.notLikeClause(schema, table, column, format);
            return this;
        }

        public final QueryWhere notLike(String column, String format) {
            super.notLikeClause(metadata.schema(), metadata.table(), column, format);
            return this;
        }

        public final QueryWhere in(String schema, String table, String column, Object[] in) {
            maybeAddInnerJoin(schema, table, column);
            super.inClause(schema, table, column, in);
            return this;
        }

        public final QueryWhere in(String column, Object[] in) {
            super.inClause(metadata.schema(), metadata.table(), column, in);
            return this;
        }

        public final QueryWhere notIn(String schema, String table, String column, Object[] in) {
            maybeAddInnerJoin(schema, table, column);
            super.notInClause(schema, table, column, in);
            return this;
        }

        public final QueryWhere notIn(String column, Object[] in) {
            super.notInClause(metadata.schema(), metadata.table(), column, in);
            return this;
        }

        public final QueryWhere in(String schema, String table, String column, List<Object> in) {
            maybeAddInnerJoin(schema, table, column);
            super.inClause(schema, table, column, in.toArray());
            return this;
        }

        public final QueryWhere in(String column, List<Object> in) {
            super.inClause(metadata.schema(), metadata.table(), column, in.toArray());
            return this;
        }

        public final QueryWhere notIn(String schema, String table, String column, List<Object> in) {
            maybeAddInnerJoin(schema, table, column);
            super.notInClause(schema, table, column, in.toArray());
            return this;
        }

        public final QueryWhere notIn(String column, List<Object> in) {
            super.notInClause(metadata.schema(), metadata.table(), column, in.toArray());
            return this;
        }

        public final QueryWhere between(String schema, String table, String column, Object min, Object max) {
            maybeAddInnerJoin(schema, table, column);
            super.betweenClause(schema, table, column, min, max);
            return this;
        }

        public final QueryWhere between(String column, Object min, Object max) {
            super.betweenClause(metadata.schema(), metadata.table(), column, min, max);
            return this;
        }

        public final QueryWhere notBetween(String schema, String table, String column, Object min, Object max) {
            maybeAddInnerJoin(schema, table, column);
            super.notBetweenClause(schema, table, column, min, max);
            return this;
        }

        public final QueryWhere notBetween(String column, Object min, Object max) {
            super.notBetweenClause(metadata.schema(), metadata.table(), column, min, max);
            return this;
        }

        public final QueryWhere greaterThan(String schema, String table, String column, Object value) {
            maybeAddInnerJoin(schema, table, column);
            super.greaterThanClause(schema, table, column, value);
            return this;
        }

        public final QueryWhere greaterThan(String column, Object value) {
            super.greaterThanClause(metadata.schema(), metadata.table(), column, value);
            return this;
        }

        public final QueryWhere greaterThanOrEqualTo(String schema, String table, String column, Object value) {
            maybeAddInnerJoin(schema, table, column);
            super.greaterThanOrEqualToClause(schema, table, column, value);
            return this;
        }

        public final QueryWhere greaterThanOrEqualTo(String column, Object value) {
            super.greaterThanOrEqualToClause(metadata.schema(), metadata.table(), column, value);
            return this;
        }

        public final QueryWhere lessThan(String schema, String table, String column, Object value) {
            maybeAddInnerJoin(schema, table, column);
            super.lessThanClause(schema, table, column, value);
            return this;
        }

        public final QueryWhere lessThan(String column, Object value) {
            super.lessThanClause(metadata.schema(), metadata.table(), column, value);
            return this;
        }

        public final QueryWhere lessThanOrEqualTo(String schema, String table, String column, Object value) {
            maybeAddInnerJoin(schema, table, column);
            super.lessThanOrEqualToClause(schema, table, column, value);
            return this;
        }

        private void maybeAddInnerJoin(String schema, String table, String column) {
            if (schema.equals(metadata.schema()) && table.equals(metadata.table())) {
                return;
            }

            ForeignKey fkey = null;
            SQLSchema sqlSchema = dataManager.getSQLBuilder().getSchema(metadata.schema());
            Preconditions.checkNotNull(sqlSchema, "Schema not found: " + metadata.schema());
            SQLTable sqlTable = sqlSchema.getTable(metadata.table());
            Preconditions.checkNotNull(sqlTable, "Table not found: " + metadata.table());

            for (ForeignKey foreignKey : sqlTable.getForeignKeys()) {
                if (!foreignKey.getReferencedSchema().equals(schema)) {
                    continue;
                }
                if (!foreignKey.getReferencedTable().equals(table)) {
                    continue;
                }
                if (foreignKey.getLinkingColumns().stream().anyMatch(l -> l.columnInReferencedTable().equals(column))) {
                    fkey = foreignKey;
                    break;
                }
            }

            Preconditions.checkNotNull(fkey, "No foreign key found from " + metadata.schema() + "." + metadata.table() + " to " + schema + "." + table + " for column " + column);
            super.addInnerJoin(
                    metadata.schema(),
                    metadata.table(),
                    fkey.getLinkingColumns().stream().map(Link::columnInReferringTable).toArray(String[]::new),
                    schema,
                    table,
                    fkey.getLinkingColumns().stream().map(Link::columnInReferencedTable).toArray(String[]::new)
            );
        }
    }
}
