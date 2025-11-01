package net.staticstudios.data.compiler.javac;

import com.sun.tools.javac.tree.JCTree;
import net.staticstudios.data.utils.Link;

import java.util.List;

class ParsedForeignPersistentValue extends ParsedPersistentValue {
    private final String insertStrategy;
    private final List<Link> links;

    public ParsedForeignPersistentValue(String fieldName, String schema, String table, String column, boolean nullable, JCTree.JCExpression type, String insertStrategy, List<Link> links) {
        super(fieldName, schema, table, column, nullable, type);
        this.insertStrategy = insertStrategy;
        this.links = links;
    }

    public String getInsertStrategy() {
        return insertStrategy;
    }

    public List<Link> getLinks() {
        return links;
    }

    @Override
    public String toString() {
        return "ParsedForeignPersistentValue{" +
                "fieldName='" + getFieldName() + '\'' +
                ", schema='" + getSchema() + '\'' +
                ", table='" + getTable() + '\'' +
                ", column='" + getColumn() + '\'' +
                ", nullable=" + isNullable() +
                ", type=" + getType() +
                ", insertStrategy='" + insertStrategy + '\'' +
                ", links=" + links +
                '}';
    }
}
