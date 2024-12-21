package net.staticstudios.data.key;

import com.impossibl.postgres.utils.guava.Preconditions;

import java.util.Objects;
import java.util.UUID;

public class UniqueIdentifier {
    private final String column;
    private final UUID id;

    public UniqueIdentifier(String column, UUID id) {
        this.column = Preconditions.checkNotNull(column);
        this.id = id; //can be null for dummy instances
    }

    public static UniqueIdentifier of(String column, UUID value) {
        return new UniqueIdentifier(column, value);
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

        UniqueIdentifier uniqueIdentifier = (UniqueIdentifier) obj;
        return this.column.equals(uniqueIdentifier.column) && this.id.equals(uniqueIdentifier.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(column, id);
    }

    @Override
    public String toString() {
        return "UniqueIdentifier{" +
                "column='" + column + '\'' +
                ", id=" + id +
                '}';
    }
}
