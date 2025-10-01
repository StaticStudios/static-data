package net.staticstudios.data.util;

import net.staticstudios.data.UniqueData;
import net.staticstudios.data.parse.ForeignKey;

import java.util.List;
import java.util.Objects;

public class ForeignPersistentValueMetadata extends PersistentValueMetadata {
    private final List<ForeignKey.Link> links;

    public ForeignPersistentValueMetadata(Class<? extends UniqueData> holderClass, ColumnMetadata columnMetadata, int updateInterval, List<ForeignKey.Link> links) {
        super(holderClass, columnMetadata, updateInterval);
        this.links = links;
    }

    public List<ForeignKey.Link> getLinks() {
        return links;
    }


    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        ForeignPersistentValueMetadata that = (ForeignPersistentValueMetadata) o;
        return Objects.equals(links, that.links);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), links);
    }

    @Override
    public String toString() {
        return "ForeignPersistentValueMetadata[" +
                "columnMetadata=" + getColumnMetadata() + ", " +
                "links=" + links +
                "]";
    }
}
