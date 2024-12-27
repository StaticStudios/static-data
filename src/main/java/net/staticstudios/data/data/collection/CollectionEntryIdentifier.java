package net.staticstudios.data.data.collection;

import com.google.common.base.Preconditions;

import java.util.Objects;
import java.util.UUID;

public class CollectionEntryIdentifier {
    private final String column;
    private final UUID id;

    public CollectionEntryIdentifier(String column, UUID id) {
        this.column = Preconditions.checkNotNull(column);
        this.id = id; //can be null for dummy instances
    }

    public static CollectionEntryIdentifier of(String column, UUID value) {
        return new CollectionEntryIdentifier(column, value);
    }

    public String getColumn() {
        return column;
    }

    public UUID getId() {
        return id;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        CollectionEntryIdentifier entryIdentifier = (CollectionEntryIdentifier) obj;
        return this.column.equals(entryIdentifier.column) && this.id.equals(entryIdentifier.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(column, id);
    }

    @Override
    public String toString() {
        return "CollectionEntryIdentifier{" +
                "column='" + column + '\'' +
                ", id=" + id +
                '}';
    }
}
