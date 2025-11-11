package net.staticstudios.data.util;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.impl.data.PersistentManyToManyCollectionImpl;
import net.staticstudios.data.utils.Link;

import java.util.List;
import java.util.Objects;

public class PersistentManyToManyCollectionMetadata implements PersistentCollectionMetadata {
    private final Class<? extends UniqueData> holderClass;
    private final Class<? extends UniqueData> referencedType;
    private final String parsedJoinTableSchema;
    private final String parsedJoinTableName;
    private final String rawLinks;
    private String joinTableSchema;
    private String joinTableName;
    private List<Link> joinTableToDataTableLinks;
    private List<Link> joinTableToReferencedTableLinks;

    public PersistentManyToManyCollectionMetadata(Class<? extends UniqueData> holderClass, Class<? extends UniqueData> referencedType, String parsedJoinTableSchema, String parsedJoinTableName, String rawLinks) {
        this.holderClass = holderClass;
        this.referencedType = referencedType;
        this.parsedJoinTableSchema = parsedJoinTableSchema;
        this.parsedJoinTableName = parsedJoinTableName;
        this.rawLinks = rawLinks;
    }

    public Class<? extends UniqueData> getReferencedType() {
        return referencedType;
    }

    public synchronized String getJoinTableSchema(DataManager dataManager) {
        if (joinTableSchema == null) {
            UniqueDataMetadata holderMetadata = dataManager.getMetadata(holderClass);
            joinTableSchema = PersistentManyToManyCollectionImpl.getJoinTableSchema(parsedJoinTableSchema, holderMetadata.schema());
        }
        return joinTableSchema;
    }

    public synchronized String getJoinTableName(DataManager dataManager) {
        if (joinTableName == null) {
            UniqueDataMetadata holderMetadata = dataManager.getMetadata(holderClass);
            UniqueDataMetadata referencedMetadata = dataManager.getMetadata(referencedType);
            joinTableName = PersistentManyToManyCollectionImpl.getJoinTableName(parsedJoinTableName, holderMetadata.table(), referencedMetadata.table());
        }
        return joinTableName;
    }

    public synchronized List<Link> getJoinTableToDataTableLinks(DataManager dataManager) {
        if (joinTableToDataTableLinks == null) {
            UniqueDataMetadata holderMetadata = dataManager.getMetadata(holderClass);
            joinTableToDataTableLinks = PersistentManyToManyCollectionImpl.getJoinTableToDataTableLinks(holderMetadata.table(), rawLinks);
        }
        return joinTableToDataTableLinks;
    }

    public synchronized List<Link> getJoinTableToReferencedTableLinks(DataManager dataManager) {
        if (joinTableToReferencedTableLinks == null) {
            UniqueDataMetadata holderMetadata = dataManager.getMetadata(holderClass);
            UniqueDataMetadata referencedMetadata = dataManager.getMetadata(referencedType);
            joinTableToReferencedTableLinks = PersistentManyToManyCollectionImpl.getJoinTableToReferencedTableLinks(holderMetadata.table(), referencedMetadata.table(), rawLinks);
        }
        return joinTableToReferencedTableLinks;
    }

    @Override
    public Class<? extends UniqueData> getHolderClass() {
        return holderClass;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        PersistentManyToManyCollectionMetadata that = (PersistentManyToManyCollectionMetadata) o;
        return Objects.equals(referencedType, that.referencedType) &&
                Objects.equals(parsedJoinTableSchema, that.parsedJoinTableSchema) &&
                Objects.equals(parsedJoinTableName, that.parsedJoinTableName) &&
                Objects.equals(rawLinks, that.rawLinks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(referencedType, parsedJoinTableSchema, parsedJoinTableName, rawLinks);
    }

    @Override
    public String toString() {
        return "PersistentManyToManyCollectionMetadata{" +
                "dataType=" + referencedType +
                ", parsedJoinTableSchema='" + parsedJoinTableSchema + '\'' +
                ", parsedJoinTableName='" + parsedJoinTableName + '\'' +
                ", links='" + rawLinks + '\'' +
                '}';
    }
}
