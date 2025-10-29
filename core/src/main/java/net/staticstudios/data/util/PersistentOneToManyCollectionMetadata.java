package net.staticstudios.data.util;

import net.staticstudios.data.UniqueData;
import net.staticstudios.data.utils.Link;

import java.util.List;
import java.util.Objects;

public class PersistentOneToManyCollectionMetadata implements PersistentCollectionMetadata {
    private final Class<? extends UniqueData> dataType;
    private final List<Link> links;

    public PersistentOneToManyCollectionMetadata(Class<? extends UniqueData> dataType, List<Link> links) {
        this.dataType = dataType;
        this.links = links;
    }

    public Class<? extends UniqueData> getDataType() {
        return dataType;
    }

    public List<Link> getLinks() {
        return links;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        PersistentOneToManyCollectionMetadata that = (PersistentOneToManyCollectionMetadata) o;
        return Objects.equals(dataType, that.dataType) && Objects.equals(links, that.links);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataType, links);
    }

    @Override
    public String toString() {
        return "PersistentOneToManyCollectionMetadata[" +
                "links=" + links + ']';
    }
}
