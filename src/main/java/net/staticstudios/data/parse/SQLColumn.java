package net.staticstudios.data.parse;

public class SQLColumn {
    private final SQLTable table;
    private final String name;
    private final boolean nullable;
    private final boolean indexed;

    public SQLColumn(SQLTable table, String name, boolean nullable, boolean indexed) {
        this.table = table;
        this.name = name;
        this.nullable = nullable;
        this.indexed = indexed;
    }

    public SQLTable getTable() {
        return table;
    }

    public String getName() {
        return name;
    }

    public boolean isNullable() {
        return nullable;
    }

    public boolean isIndexed() {
        return indexed;
    }

    //todo: equals, hashCode, toString methods if needed
}
