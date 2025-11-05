package net.staticstudios.data.util;

import java.util.Objects;

public final class AutoIncrementingIntegerColumnMetadata extends ColumnMetadata {

    public AutoIncrementingIntegerColumnMetadata(String schema, String table, String name) {
        super(schema, table, name, Integer.class, false, false, "");
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (AutoIncrementingIntegerColumnMetadata) obj;
        return super.equals(that);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode());
    }

    @Override
    public String toString() {
        return "AutoIncrementingIntegerColumnMetadata[" +
                "schema=" + schema() + ", " +
                "table=" + table() + ", " +
                "name=" + name() +
                "]";
    }

}
