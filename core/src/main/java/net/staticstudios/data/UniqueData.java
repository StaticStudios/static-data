package net.staticstudios.data;

import com.google.common.base.Preconditions;
import net.staticstudios.data.util.ColumnValuePair;
import net.staticstudios.data.util.ColumnValuePairs;
import net.staticstudios.data.util.UniqueDataMetadata;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.ApiStatus;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public abstract class UniqueData {
    private ColumnValuePairs idColumns;
    private DataManager dataManager;
    private volatile boolean isDeleted = false;

    @ApiStatus.Internal
    protected final void setDataManager(DataManager dataManager) {
        this.dataManager = dataManager;
    }

    @ApiStatus.Internal
    protected final synchronized void setIdColumns(ColumnValuePairs idColumns) {
        this.idColumns = idColumns;
    }

    protected final synchronized void markDeleted() {
        this.isDeleted = true;
    }

    public final synchronized boolean isDeleted() {
        return isDeleted;
    }

    public DataManager getDataManager() {
        return dataManager;
    }

    public synchronized ColumnValuePairs getIdColumns() {
        return idColumns;
    }

    public final UniqueDataMetadata getMetadata() {
        return dataManager.getMetadata(this.getClass());
    }

    public synchronized void delete() {
        Preconditions.checkState(!isDeleted, "This object has already been deleted!");
        UniqueDataMetadata metadata = getMetadata();

        StringBuilder stringBuilder = new StringBuilder("DELETE FROM \"" + metadata.schema() + "\".\"" + metadata.table() + "\" WHERE ");
        List<Object> values = new ArrayList<>();
        for (ColumnValuePair idColumn : idColumns) {
            stringBuilder.append("\"").append(idColumn.column()).append("\" = ? AND ");
            values.add(idColumn.value());
        }
        stringBuilder.setLength(stringBuilder.length() - 5);
        @Language("SQL") String sql = stringBuilder.toString();

        try {
            dataManager.getDataAccessor().executeUpdate(SQLTransaction.Statement.of(sql, sql), values, 0);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("{");
        if (this.idColumns != null) {
            for (ColumnValuePair idColumn : idColumns) {
                sb.append(idColumn.column()).append("=").append(idColumn.value()).append(", ");
            }
        }
        if (isDeleted) {
            sb.append("deleted=true, ");
        }
        sb.append("dataManager=").append(dataManager);
        sb.append("}");
        return sb.toString();
    }

    @Override
    public final int hashCode() {
        return idColumns.hashCode();
    }

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof UniqueData other)) return false;
        if (!this.getClass().equals(other.getClass())) return false;
        return this.dataManager.equals(other.dataManager) && this.idColumns.equals(other.idColumns);
    }
}
