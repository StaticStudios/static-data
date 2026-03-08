package net.staticstudios.data.util;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.utils.Link;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Objects;

public final class ReferenceMetadata {
    private final Class<? extends UniqueData> holderClass;
    private final Class<? extends UniqueData> referencedClass;
    private final List<Link> links;
    private final boolean generateFkey;
    private final boolean updateReferencedTable;
    private @Nullable String selectReferencedColumnValuePairsQuery;
    private boolean validatedUpdateHandlers = false;

    public ReferenceMetadata(Class<? extends UniqueData> holderClass, Class<? extends UniqueData> referencedClass,
                             List<Link> links, boolean generateFkey, boolean updateReferencedTable) {
        this.holderClass = holderClass;
        this.referencedClass = referencedClass;
        this.links = links;
        this.generateFkey = generateFkey;
        this.updateReferencedTable = updateReferencedTable;
    }

    private @Language("SQL") String buildSelectReferencedColumnValuePairsQuery(DataManager dataManager) {
        UniqueDataMetadata holderMetadata = dataManager.getMetadata(holderClass);
        UniqueDataMetadata referencedMetadata = dataManager.getMetadata(referencedClass);
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT ");
        for (ColumnMetadata idColumn : referencedMetadata.idColumns()) {
            sqlBuilder.append("_referenced.\"").append(idColumn.name()).append("\", ");
        }
        for (Link entry : links) {
            String myColumn = entry.columnInReferringTable();
            sqlBuilder.append("_referring.\"").append(myColumn).append("\", ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 2);

        sqlBuilder.append(" FROM \"").append(referencedMetadata.schema()).append("\".\"").append(referencedMetadata.table()).append("\" _referenced");
        sqlBuilder.append(" INNER JOIN \"").append(holderMetadata.schema()).append("\".\"").append(holderMetadata.table()).append("\" _referring ON ");
        for (Link entry : links) {
            String myColumn = entry.columnInReferringTable();
            String theirColumn = entry.columnInReferencedTable();
            sqlBuilder.append("_referenced.\"").append(theirColumn).append("\" = ");
            sqlBuilder.append("_referring.\"").append(myColumn).append("\" AND ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 5);

        sqlBuilder.append(" WHERE ");

        for (ColumnMetadata idColumn : holderMetadata.idColumns()) {
            sqlBuilder.append("_referring.\"").append(idColumn.name()).append("\" = ? AND ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 5);

        return sqlBuilder.toString();
    }


    public SelectQuery buildSelectReferencedColumnValuePairsSelectQuery(DataManager dataManager, List<Object> values) {
        if (selectReferencedColumnValuePairsQuery == null) {
            selectReferencedColumnValuePairsQuery = buildSelectReferencedColumnValuePairsQuery(dataManager);
        }
        return new SelectQuery(selectReferencedColumnValuePairsQuery, values);
    }

    public Class<? extends UniqueData> holderClass() {
        return holderClass;
    }

    public Class<? extends UniqueData> referencedClass() {
        return referencedClass;
    }

    public List<Link> links() {
        return links;
    }

    public boolean generateFkey() {
        return generateFkey;
    }

    public boolean updateReferencedTable() {
        return updateReferencedTable;
    }

    public boolean hasValidatedUpdateHandlers() {
        return validatedUpdateHandlers;
    }

    public void setValidatedUpdateHandlers(boolean validatedUpdateHandlers) {
        this.validatedUpdateHandlers = validatedUpdateHandlers;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (ReferenceMetadata) obj;
        return Objects.equals(this.holderClass, that.holderClass) &&
                Objects.equals(this.referencedClass, that.referencedClass) &&
                Objects.equals(this.links, that.links) &&
                this.generateFkey == that.generateFkey &&
                this.updateReferencedTable == that.updateReferencedTable;
    }

    @Override
    public int hashCode() {
        return Objects.hash(holderClass, referencedClass, links, generateFkey, updateReferencedTable);
    }

    @Override
    public String toString() {
        return "ReferenceMetadata[" +
                "holderClass=" + holderClass + ", " +
                "referencedClass=" + referencedClass + ", " +
                "links=" + links + ", " +
                "generateFkey=" + generateFkey + ", " +
                "updateReferencedTable=" + updateReferencedTable + ']';
    }


}
