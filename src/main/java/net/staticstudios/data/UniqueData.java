package net.staticstudios.data;

import net.staticstudios.data.util.ColumnValuePair;
import net.staticstudios.data.util.ColumnValuePairs;
import net.staticstudios.data.util.UniqueDataMetadata;
import org.jetbrains.annotations.ApiStatus;

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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("{");
        for (ColumnValuePair idColumn : idColumns) {
            sb.append(idColumn.column()).append("=").append(idColumn.value()).append(", ");
        }
        if (!idColumns.isEmpty()) {
            sb.setLength(sb.length() - 2);
        }
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
