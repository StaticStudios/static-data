package net.staticstudios.data.insert;

import net.staticstudios.data.util.ColumnValuePair;
import net.staticstudios.data.util.ColumnValuePairs;
import net.staticstudios.data.util.SQlStatement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UpdateColumnPostInsertAction implements PostInsertAction {
    private final String schema;
    private final String table;
    private final ColumnValuePairs idColumns;
    private final Map<String, Object> updateValues;


    public UpdateColumnPostInsertAction(String schema, String table, ColumnValuePairs idColumns, Map<String, Object> updateValues) {
        this.schema = schema;
        this.table = table;
        this.idColumns = idColumns;
        this.updateValues = updateValues;
    }

    @Override
    public List<SQlStatement> getStatements() {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("UPDATE ")
                .append("\"").append(schema).append("\".\"").append(table).append("\" SET ");
        List<Object> values = new ArrayList<>();
        for (Map.Entry<String, Object> entry : updateValues.entrySet()) {
            sqlBuilder.append("\"").append(entry.getKey()).append("\" = ?, ");
            values.add(entry.getValue());
        }
        sqlBuilder.setLength(sqlBuilder.length() - 2);
        sqlBuilder.append(" WHERE ");
        for (ColumnValuePair idColumn : idColumns) {
            sqlBuilder.append("\"").append(idColumn.column()).append("\" = ? AND ");
            values.add(idColumn.value());
        }
        sqlBuilder.setLength(sqlBuilder.length() - 5);

        return List.of(new SQlStatement(
                sqlBuilder.toString(),
                sqlBuilder.toString(),
                values
        ));
    }
}
