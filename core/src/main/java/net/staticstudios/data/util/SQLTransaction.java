package net.staticstudios.data.util;

import com.google.common.base.Preconditions;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class SQLTransaction {
    private final List<Operation> operations = new ArrayList<>();

    public SQLTransaction() {
    }

    public List<Operation> getOperations() {
        return operations;
    }

    public SQLTransaction query(Statement statement, Supplier<List<Object>> valuesSupplier, @NotNull Consumer<ResultSet> resultHandler) {
        Preconditions.checkNotNull(resultHandler, "Use update() method for statements without result handlers");
        operations.add(new Operation(statement, valuesSupplier, resultHandler, () -> true));
        return this;
    }

    public SQLTransaction update(Statement statement, Supplier<List<Object>> valuesSupplier) {
        operations.add(new Operation(statement, valuesSupplier, null, () -> true));
        return this;
    }

    public SQLTransaction update(Statement statement, List<Object> values) {
        operations.add(new Operation(statement, () -> values, null, () -> true));
        return this;
    }

    public SQLTransaction update(Statement statement, Supplier<List<Object>> valuesSupplier, Supplier<Boolean> shouldRun) {
        operations.add(new Operation(statement, valuesSupplier, null, shouldRun));
        return this;
    }

    public SQLTransaction update(Statement statement, List<Object> values, Supplier<Boolean> shouldRun) {
        operations.add(new Operation(statement, () -> values, null, shouldRun));
        return this;
    }

    public static class Operation {
        private final Statement statement;
        private final Supplier<List<Object>> valuesSupplier;
        private final @Nullable Consumer<ResultSet> resultHandler;
        private final Supplier<Boolean> shouldRun;

        public Operation(Statement statement, Supplier<List<Object>> valuesSupplier, @Nullable Consumer<ResultSet> resultHandler, Supplier<Boolean> shouldRun) {
            this.statement = statement;
            this.valuesSupplier = valuesSupplier;
            this.resultHandler = resultHandler;
            this.shouldRun = shouldRun;
        }

        public Statement getStatement() {
            return statement;
        }

        public Supplier<List<Object>> getValuesSupplier() {
            return valuesSupplier;
        }

        public @Nullable Consumer<ResultSet> getResultHandler() {
            return resultHandler;
        }

        public boolean shouldRun() {
            return shouldRun.get();
        }

    }

    public static class Statement {
        private final @Language("SQL") String h2Sql;
        private final @Language("SQL") String pgSql;

        public Statement(@Language("SQL") String h2Sql, @Language("SQL") String pgSql) {
            this.h2Sql = h2Sql;
            this.pgSql = pgSql;
        }

        public static Statement of(@Language("SQL") String h2Sql, @Language("SQL") String pgSql) {
            return new Statement(h2Sql, pgSql);
        }

        public @Language("SQL") String getH2Sql() {
            return h2Sql;
        }

        public @Language("SQL") String getPgSql() {
            return pgSql;
        }
    }
}
