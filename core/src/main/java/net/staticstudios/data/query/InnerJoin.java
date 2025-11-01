package net.staticstudios.data.query;

import java.util.Arrays;
import java.util.Objects;

public record InnerJoin(String referringSchema, String referringTable, String[] columnsInReferringTable,
                        String referencedSchema, String referencedTable,
                        String[] columnsInReferencedTable) {

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        InnerJoin that = (InnerJoin) obj;
        return Objects.equals(referringSchema, that.referringSchema) &&
                Objects.equals(referringTable, that.referringTable) &&
                Arrays.equals(columnsInReferringTable, that.columnsInReferringTable) &&
                Objects.equals(referencedSchema, that.referencedSchema) &&
                Objects.equals(referencedTable, that.referencedTable) &&
                Arrays.equals(columnsInReferencedTable, that.columnsInReferencedTable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(referringSchema, referringTable, Arrays.hashCode(columnsInReferringTable), referencedSchema, referencedTable, Arrays.hashCode(columnsInReferencedTable));
    }
}
