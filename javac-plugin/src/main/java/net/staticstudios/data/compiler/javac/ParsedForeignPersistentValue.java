package net.staticstudios.data.compiler.javac;

import com.sun.tools.javac.tree.JCTree;

class ParsedForeignPersistentValue extends ParsedPersistentValue {
    private final String insertStrategy;

    public ParsedForeignPersistentValue(String fieldName, String schema, String table, String column, JCTree.JCExpression type, String insertStrategy) {
        super(fieldName, schema, table, column, type);
        this.insertStrategy = insertStrategy;
    }

    public String getInsertStrategy() {
        return insertStrategy;
    }

    @Override
    public String toString() {
        return "ParsedForeignPersistentValue{" +
                "fieldName='" + getFieldName() + '\'' +
                ", schema='" + getSchema() + '\'' +
                ", table='" + getTable() + '\'' +
                ", column='" + getColumn() + '\'' +
                ", type=" + getType() +
                ", insertStrategy='" + insertStrategy + '\'' +
                '}';
    }
}
