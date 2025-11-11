package net.staticstudios.data.util;

import net.staticstudios.data.UniqueData;
import net.staticstudios.data.utils.Link;

import java.util.List;
import java.util.Objects;

public class PersistentOneToManyValueCollectionMetadata implements PersistentCollectionMetadata {
    private final Class<? extends UniqueData> holderClass;
    private final Class<?> dataType;
    private final String dataSchema;
    private final String dataTable;
    private final String dataColumn;
    private final List<Link> links;

    public PersistentOneToManyValueCollectionMetadata(Class<? extends UniqueData> holderClass, Class<?> dataType, String dataSchema, String dataTable, String dataColumn, List<Link> links) {
        this.holderClass = holderClass;
        this.dataType = dataType;
        this.dataSchema = dataSchema;
        this.dataTable = dataTable;
        this.dataColumn = dataColumn;
        this.links = links;
    }

    @Override
    public Class<? extends UniqueData> getHolderClass() {
        return holderClass;
    }

    public Class<?> getDataType() {
        return dataType;
    }

    public String getDataSchema() {
        return dataSchema;
    }

    public String getDataTable() {
        return dataTable;
    }

    public String getDataColumn() {
        return dataColumn;
    }

    public List<Link> getLinks() {
        return links;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        PersistentOneToManyValueCollectionMetadata that = (PersistentOneToManyValueCollectionMetadata) o;
        return Objects.equals(dataType, that.dataType) &&
                Objects.equals(dataSchema, that.dataSchema) &&
                Objects.equals(dataTable, that.dataTable) &&
                Objects.equals(dataColumn, that.dataColumn) &&
                Objects.equals(links, that.links);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataType, dataSchema, dataTable, dataColumn, links);
    }

    @Override
    public String toString() {
        return "PersistentOneToManyValueCollectionMetadata[" +
                "dataType=" + dataType +
                ", dataSchema='" + dataSchema + '\'' +
                ", dataTable='" + dataTable + '\'' +
                ", dataColumn='" + dataColumn + '\'' +
                ", links=" + links +
                ']';
    }
}
