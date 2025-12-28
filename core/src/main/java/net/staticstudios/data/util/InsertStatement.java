package net.staticstudios.data.util;

import com.google.common.base.Preconditions;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.InsertStrategy;
import net.staticstudios.data.parse.ForeignKey;
import net.staticstudios.data.parse.SQLColumn;
import net.staticstudios.data.parse.SQLSchema;
import net.staticstudios.data.parse.SQLTable;
import net.staticstudios.data.utils.Link;

import java.util.*;

public class InsertStatement {
    private final DataManager dataManager;
    private final SQLTable table;
    private final ColumnValuePairs idColumns;
    private final Map<String, Value> columnValues = new HashMap<>();
    private final List<InsertStatement> dependantOn = new ArrayList<>();
    private List<DependencyRequirement> dependencyRequirements;

    public InsertStatement(DataManager dataManager, SQLTable table, ColumnValuePairs idColumns) {
        this.dataManager = dataManager;
        this.table = table;
        this.idColumns = idColumns;
        for (ColumnValuePair pair : idColumns.getPairs()) {
            columnValues.put(pair.column(), new Value(InsertStrategy.PREFER_EXISTING, pair.value()));
        }
    }

    public static boolean checkForCycles(List<InsertStatement> statements) {
        Set<InsertStatement> visited = new HashSet<>();
        Set<InsertStatement> recursionStack = new HashSet<>();

        for (InsertStatement statement : statements) {
            if (detectCycleDFS(statement, visited, recursionStack)) {
                return true;
            }
        }
        return false;
    }

    private static boolean detectCycleDFS(InsertStatement current, Set<InsertStatement> visited, Set<InsertStatement> stack) {
        if (stack.contains(current)) {
            throw new IllegalStateException("Dependency cycle detected involving table: " + current.getTable().getName());
        }
        if (visited.contains(current)) {
            return false;
        }

        visited.add(current);
        stack.add(current);

        for (InsertStatement dependency : current.dependantOn) {
            if (detectCycleDFS(dependency, visited, stack)) {
                return true;
            }
        }

        stack.remove(current);
        return false;
    }

    public static List<InsertStatement> sort(List<InsertStatement> statements) {
        List<InsertStatement> sortedList = new ArrayList<>();
        Set<InsertStatement> visited = new HashSet<>();

        for (InsertStatement statement : statements) {
            performTopoSortDFS(statement, visited, sortedList);
        }

        return sortedList;
    }

    private static void performTopoSortDFS(InsertStatement current, Set<InsertStatement> visited, List<InsertStatement> sortedList) {
        if (visited.contains(current)) {
            return;
        }
        visited.add(current);

        for (InsertStatement dependency : current.dependantOn) {
            performTopoSortDFS(dependency, visited, sortedList);
        }

        sortedList.add(current);
    }

    public SQLTable getTable() {
        return table;
    }

    public ColumnValuePairs getIdColumns() {
        return idColumns;
    }

    public void set(String column, InsertStrategy insertStrategy, Object value) {
        columnValues.put(column, new Value(insertStrategy, value));
    }

    public List<InsertStatement> createUnmetDependencyStatements(List<InsertStatement> existingStatements) {
        Preconditions.checkState(dependencyRequirements != null, "Must call calculateRequiredDependencies() before checking for unmet dependencies.");
        List<InsertStatement> unmetStatements = new ArrayList<>();
        for (DependencyRequirement requirement : dependencyRequirements) {
            boolean satisfied = false;
            for (InsertStatement existingStatement : existingStatements) {
                if (requirement.isSatisfiedBy(existingStatement)) {
                    satisfied = true;
                    dependantOn.add(existingStatement);
                    break;
                }
            }
            if (!satisfied) {
                InsertStatement satisfyingStatement = requirement.createSatisfyingStatement(dataManager);
                unmetStatements.add(satisfyingStatement);
                dependantOn.add(satisfyingStatement);
            }
        }
        return unmetStatements;
    }

    public void calculateRequiredDependencies() {
        Preconditions.checkState(dependantOn.isEmpty(), "Dependencies have already been satisfied.");
        dependencyRequirements = new ArrayList<>();


        // handle foreign keys. for each foreign key, if we have a value for the local column, we need to set the value for the foreign column. this consists of linking ids mostly.
        for (ForeignKey fKey : table.getForeignKeys()) {
            SQLSchema referencedSchema = Objects.requireNonNull(dataManager.getSQLBuilder().getSchema(fKey.getReferencedSchema()));
            SQLTable referencedTable = Objects.requireNonNull(referencedSchema.getTable(fKey.getReferencedTable()));

            boolean addRequirement = true;

            for (Link link : fKey.getLinkingColumns()) {
                String myColumnName = link.columnInReferringTable();

                if (!columnValues.containsKey(myColumnName)) {
                    addRequirement = false;
                    break;
                }

                String otherColumnName = link.columnInReferencedTable();
                SQLColumn otherColumn = Objects.requireNonNull(referencedTable.getColumn(otherColumnName));

                // if its nullable and we don't have a value, skip it.
                if (otherColumn.isNullable()) {
                    addRequirement = false;
                    break;
                }

            }

            // to satisfy this, we need a statement that contains the foreign column with this value.
            if (addRequirement) {
                List<ColumnValuePair> requiredColumnValues = new ArrayList<>();
                for (Link link : fKey.getLinkingColumns()) {
                    String myColumnName = link.columnInReferringTable();
                    String otherColumnName = link.columnInReferencedTable();
                    Value myValue = columnValues.get(myColumnName);
                    requiredColumnValues.add(new ColumnValuePair(otherColumnName, myValue.value()));
                }
                dependencyRequirements.add(new DependencyRequirement(
                        fKey.getReferencedSchema(),
                        fKey.getReferencedTable(),
                        requiredColumnValues
                ));
            }
        }
    }

    public SQlStatement asStatement() {
        String schemaName = table.getSchema().getName();
        String tableName = table.getName();

        StringBuilder h2SqlBuilder = new StringBuilder("MERGE INTO \"");
        h2SqlBuilder.append(schemaName).append("\".\"").append(tableName).append("\" AS target USING (VALUES (");
        h2SqlBuilder.append("?, ".repeat(columnValues.size()));
        h2SqlBuilder.setLength(h2SqlBuilder.length() - 2);
        h2SqlBuilder.append(")) AS source (");
        columnValues.forEach((column, value) -> {
            h2SqlBuilder.append("\"").append(column).append("\", ");
        });
        h2SqlBuilder.setLength(h2SqlBuilder.length() - 2);
        h2SqlBuilder.append(") ON ");
        for (ColumnMetadata idColumn : table.getIdColumns()) {
            h2SqlBuilder.append("target.\"").append(idColumn.name()).append("\" = source.\"").append(idColumn.name()).append("\" AND ");
        }
        h2SqlBuilder.setLength(h2SqlBuilder.length() - 5);
        h2SqlBuilder.append(" WHEN NOT MATCHED THEN INSERT (");

        columnValues.forEach((column, value) -> {
            h2SqlBuilder.append("\"").append(column).append("\", ");
        });
        h2SqlBuilder.setLength(h2SqlBuilder.length() - 2);
        h2SqlBuilder.append(") VALUES (");
        columnValues.forEach((column, value) -> {
            h2SqlBuilder.append("source.\"").append(column).append("\", ");
        });
        h2SqlBuilder.setLength(h2SqlBuilder.length() - 2);
        h2SqlBuilder.append(")");

        List<String> overwriteExisting = new ArrayList<>();
        columnValues.forEach((column, value) -> {
            if (value.insertStrategy() == InsertStrategy.OVERWRITE_EXISTING) {
                overwriteExisting.add(column);
            }
        });
        if (!overwriteExisting.isEmpty()) {
            h2SqlBuilder.append(" WHEN MATCHED THEN UPDATE SET ");
            for (String column : overwriteExisting) {
                h2SqlBuilder.append("\"").append(column).append("\" = source.\"").append(column).append("\", ");
            }
            h2SqlBuilder.setLength(h2SqlBuilder.length() - 2);
        }

        StringBuilder pgSqlBuilder = new StringBuilder("INSERT INTO \"");
        pgSqlBuilder.append(schemaName).append("\".\"").append(tableName).append("\" (");
        columnValues.forEach((column, value) -> {
            pgSqlBuilder.append("\"").append(column).append("\", ");
        });
        pgSqlBuilder.setLength(pgSqlBuilder.length() - 2);
        pgSqlBuilder.append(") VALUES (");
        pgSqlBuilder.append("?, ".repeat(columnValues.size()));
        pgSqlBuilder.setLength(pgSqlBuilder.length() - 2);
        pgSqlBuilder.append(")");

        pgSqlBuilder.append(" ON CONFLICT (");
        for (ColumnMetadata idColumn : table.getIdColumns()) {
            pgSqlBuilder.append("\"").append(idColumn.name()).append("\", ");
        }
        pgSqlBuilder.setLength(pgSqlBuilder.length() - 2);
        pgSqlBuilder.append(") DO ");
        if (!overwriteExisting.isEmpty()) {
            pgSqlBuilder.append("UPDATE SET ");
            for (String column : overwriteExisting) {
                pgSqlBuilder.append("\"").append(column).append("\" = EXCLUDED.\"").append(column).append("\", ");
            }
            pgSqlBuilder.setLength(pgSqlBuilder.length() - 2);
        } else {
            pgSqlBuilder.append("NOTHING");
        }

        String h2Sql = h2SqlBuilder.toString();
        String pgSql = pgSqlBuilder.toString();
        List<Object> values = new ArrayList<>();
        columnValues.forEach((column, value) -> {
            Object serializedValue = dataManager.serialize(value.value());
            values.add(serializedValue);
        });
        return new SQlStatement(h2Sql, pgSql, values);
    }

    public record Value(InsertStrategy insertStrategy, Object value) {

    }

    private static class DependencyRequirement {
        private final String schema;
        private final String table;
        private final List<ColumnValuePair> requiredColumnValues;

        public DependencyRequirement(String schema, String table, List<ColumnValuePair> requiredColumnValues) {
            this.schema = schema;
            this.table = table;
            this.requiredColumnValues = requiredColumnValues;
        }

        public boolean isSatisfiedBy(InsertStatement statement) {
            if (!statement.getTable().getSchema().getName().equals(schema)) {
                return false;
            }
            if (!statement.getTable().getName().equals(table)) {
                return false;
            }

            List<ColumnValuePair> lookingFor = new ArrayList<>(requiredColumnValues);

            statement.columnValues.forEach((columnName, value) -> {
                lookingFor.removeIf(pair -> pair.column().equals(columnName) && Objects.equals(pair.value(), value.value()));
            });
            return lookingFor.isEmpty();
        }

        public InsertStatement createSatisfyingStatement(DataManager dataManager) {
            SQLSchema sqlSchema = Objects.requireNonNull(dataManager.getSQLBuilder().getSchema(schema));
            SQLTable sqlTable = Objects.requireNonNull(sqlSchema.getTable(table));
            ColumnValuePair[] idColumns = new ColumnValuePair[sqlTable.getIdColumns().size()];

            for (ColumnMetadata idColumn : sqlTable.getIdColumns()) {
                Optional<ColumnValuePair> matchingPair = requiredColumnValues.stream()
                        .filter(pair -> pair.column().equals(idColumn.name()))
                        .findFirst();
                Preconditions.checkState(matchingPair.isPresent(), "Cannot create satisfying statement for dependency requirement: missing id column value for " + idColumn.name());
                idColumns[sqlTable.getIdColumns().indexOf(idColumn)] = matchingPair.get();
            }

            InsertStatement statement = new InsertStatement(dataManager, sqlTable, new ColumnValuePairs(idColumns));
            for (ColumnValuePair pair : requiredColumnValues) {
                if (sqlTable.getIdColumns().stream().noneMatch(col -> col.name().equals(pair.column()))) {
                    statement.set(pair.column(), InsertStrategy.PREFER_EXISTING, pair.value());
                }
            }

            return statement;
        }

    }

}
