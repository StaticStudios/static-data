package net.staticstudios.data.data.collection;

import com.google.common.base.Preconditions;

import java.util.Objects;
import java.util.UUID;

public class CollectionEntryIdentifier {
    private final String entryIdColumn;
    private final UUID entryId;

    public CollectionEntryIdentifier(String entryIdColumn, UUID entryId) {
        this.entryIdColumn = Preconditions.checkNotNull(entryIdColumn);
        this.entryId = entryId; //can be null for dummy instances
    }

    public static CollectionEntryIdentifier of(String column, UUID value) {
        return new CollectionEntryIdentifier(column, value);
    }

    public String getEntryIdColumn() {
        return entryIdColumn;
    }

    public UUID getEntryId() {
        return entryId;
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
        return this.entryIdColumn.equals(entryIdentifier.entryIdColumn) && this.entryId.equals(entryIdentifier.entryId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entryIdColumn, entryId);
    }

    @Override
    public String toString() {
        return "CollectionEntryIdentifier{" +
                "entryIdColumn='" + entryIdColumn + '\'' +
                ", entryId=" + entryId +
                '}';
    }
}
