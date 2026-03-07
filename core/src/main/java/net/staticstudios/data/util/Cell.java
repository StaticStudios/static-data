package net.staticstudios.data.util;

import java.util.Objects;

public class Cell {
    private final String schema;
    private final String table;
    private final String column;
    private final ColumnValuePairs idColumnValuePairs;

    public Cell(String schema, String table, String column, ColumnValuePairs idColumnValuePairs) {
        this.schema = schema;
        this.table = table;
        this.column = column;
        this.idColumnValuePairs = idColumnValuePairs;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public String getColumn() {
        return column;
    }

    public ColumnValuePairs getIdColumnValuePairs() {
        return idColumnValuePairs;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof Cell other)) return false;
        return schema.equals(other.schema) && table.equals(other.table) && column.equals(other.column) && idColumnValuePairs.equals(other.idColumnValuePairs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, table, column, idColumnValuePairs);
    }

    @Override
    public String toString() {
        return "Cell[" +
                "schema=" + schema + ", " +
                "table=" + table + ", " +
                "column=" + column + ", " +
                "idColumnValuePairs=" + idColumnValuePairs + ']';
    }
}
