package net.staticstudios.data;

import com.impossibl.postgres.utils.guava.Preconditions;

import java.util.Objects;
import java.util.UUID;

public class PrimaryKey {
    private final String column;
    private final UUID id;

    public PrimaryKey(String column, UUID id) {
        this.column = Preconditions.checkNotNull(column);
        this.id = Preconditions.checkNotNull(id);
    }

    public static PrimaryKey of(String column, UUID value) {
        return new PrimaryKey(column, value);
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

        PrimaryKey primaryKey = (PrimaryKey) obj;
        return this.column.equals(primaryKey.column) && this.id.equals(primaryKey.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(column, id);
    }

    @Override
    public String toString() {
        return "PrimaryKey{" +
                "column='" + column + '\'' +
                ", id=" + id +
                '}';
    }
}
