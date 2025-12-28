package net.staticstudios.data.compiler.javac.javac;

import net.staticstudios.data.InsertStrategy;
import net.staticstudios.data.utils.Link;

import javax.lang.model.element.TypeElement;
import java.util.List;

class ParsedForeignPersistentValue extends ParsedPersistentValue implements ParsedRelation {
    private final InsertStrategy insertStrategy;
    private final List<Link> links;

    public ParsedForeignPersistentValue(String fieldName, String schema, String table, String column, boolean nullable, TypeElement type, InsertStrategy insertStrategy, List<Link> links) {
        super(fieldName, schema, table, column, nullable, type);
        this.insertStrategy = insertStrategy;
        this.links = links;
    }

    public InsertStrategy getInsertStrategy() {
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
