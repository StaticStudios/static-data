package net.staticstudios.data.util;

import net.staticstudios.data.UniqueData;

import java.util.Objects;

public class PersistentManyToManyCollectionMetadata implements PersistentCollectionMetadata {
    private final Class<? extends UniqueData> dataType;
    private final String parsedJoinTableSchema;
    private final String parsedJoinTableName;
    private final String links;

    public PersistentManyToManyCollectionMetadata(Class<? extends UniqueData> dataType, String parsedJoinTableSchema, String parsedJoinTableName, String links) {
        this.dataType = dataType;
        this.parsedJoinTableSchema = parsedJoinTableSchema;
        this.parsedJoinTableName = parsedJoinTableName;
        this.links = links;
    }

    public Class<? extends UniqueData> getDataType() {
        return dataType;
    }

    public String getParsedJoinTableSchema() {
        return parsedJoinTableSchema;
    }

    public String getParsedJoinTableName() {
        return parsedJoinTableName;
    }

    public String getLinks() {
        return links;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        PersistentManyToManyCollectionMetadata that = (PersistentManyToManyCollectionMetadata) o;
        return Objects.equals(dataType, that.dataType) &&
                Objects.equals(parsedJoinTableSchema, that.parsedJoinTableSchema) &&
                Objects.equals(parsedJoinTableName, that.parsedJoinTableName) &&
                Objects.equals(links, that.links);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataType, parsedJoinTableSchema, parsedJoinTableName, links);
    }

    @Override
    public String toString() {
        return "PersistentManyToManyCollectionMetadata{" +
                "dataType=" + dataType +
                ", parsedJoinTableSchema='" + parsedJoinTableSchema + '\'' +
                ", parsedJoinTableName='" + parsedJoinTableName + '\'' +
                ", links='" + links + '\'' +
                '}';
    }
}
