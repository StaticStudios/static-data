package net.staticstudios.data.util;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.utils.Link;

import java.util.List;
import java.util.Objects;

public class PersistentOneToManyCollectionMetadata implements PersistentCollectionMetadata {
    private final Class<? extends UniqueData> holderClass;
    private final DataManager dataManager;
    private final Class<? extends UniqueData> referencedType;
    private final List<Link> links;

    public PersistentOneToManyCollectionMetadata(DataManager dataManager, Class<? extends UniqueData> holderClass, Class<? extends UniqueData> referencedType, List<Link> links) {
        this.dataManager = dataManager;
        this.holderClass = holderClass;
        this.referencedType = referencedType;
        this.links = links;
    }

    public Class<? extends UniqueData> getReferencedType() {
        return referencedType;
    }

    public List<Link> getLinks() {
        return links;
    }

    @Override
    public Class<? extends UniqueData> getHolderClass() {
        return holderClass;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        PersistentOneToManyCollectionMetadata that = (PersistentOneToManyCollectionMetadata) o;
        return Objects.equals(referencedType, that.referencedType) && Objects.equals(links, that.links);
    }

    @Override
    public int hashCode() {
        return Objects.hash(referencedType, links);
    }

    @Override
    public String toString() {
        return "PersistentOneToManyCollectionMetadata[" +
                "links=" + links + ']';
    }
}
