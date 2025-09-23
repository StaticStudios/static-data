package net.staticstudios.data.util;

import net.staticstudios.data.UniqueData;
import net.staticstudios.data.parse.ForeignKey;

import java.util.List;
import java.util.Objects;

public class ReferenceMetadata {
    private final Class<? extends UniqueData> referencedClass;
    private final List<ForeignKey.Link> links;

    public ReferenceMetadata(Class<? extends UniqueData> referencedClass, List<ForeignKey.Link> links) {
        this.referencedClass = referencedClass;
        this.links = links;
    }

    public Class<? extends UniqueData> getReferencedClass() {
        return referencedClass;
    }

    public List<ForeignKey.Link> getLinks() {
        return links;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ReferenceMetadata that = (ReferenceMetadata) o;
        return Objects.equals(referencedClass, that.referencedClass) && Objects.equals(links, that.links);
    }

    @Override
    public int hashCode() {
        return Objects.hash(referencedClass, links);
    }

    @Override
    public String toString() {
        return "ReferenceMetadata[" +
                "links=" + links + ']';
    }
}
