package net.staticstudios.data;

import com.google.common.base.Preconditions;
import net.staticstudios.data.util.ColumnValuePair;
import net.staticstudios.data.util.ColumnValuePairs;
import net.staticstudios.data.util.SQLTransaction;
import net.staticstudios.data.util.UniqueDataMetadata;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.ApiStatus;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class UniqueData {
    private final AtomicBoolean deleteStarted = new AtomicBoolean(false);
    private volatile ColumnValuePairs idColumns;
    private volatile DataManager dataManager;
    private volatile boolean isDeleted = false;
    private volatile boolean isSnapshot = false;

    @ApiStatus.Internal
    protected final void setDataManager(DataManager dataManager, boolean isSnapshot) {
        this.dataManager = dataManager;
        this.isSnapshot = isSnapshot;
    }

    @ApiStatus.Internal
    protected final void setIdColumns(ColumnValuePairs idColumns) {
        this.idColumns = idColumns;
    }

    protected final void markDeleted() {
        this.isDeleted = true;
        this.deleteStarted.set(true);
    }

    public final boolean isDeleted() {
        return isDeleted;
    }

    public DataManager getDataManager() {
        return dataManager;
    }

    public ColumnValuePairs getIdColumns() {
        return idColumns;
    }

    public final UniqueDataMetadata getMetadata() {
        return dataManager.getMetadata(this.getClass());
    }

    public void delete() {

        Preconditions.checkState(!isSnapshot, "Cannot delete a snapshot!");

        if (!deleteStarted.compareAndSet(false, true)) {
            return;
        }

        DataManager dataManager = this.dataManager;
        ColumnValuePairs ids = this.idColumns;
        Preconditions.checkNotNull(ids, "Cannot delete object without ID columns");

        UniqueDataMetadata metadata = dataManager.getMetadata(this.getClass());

        StringBuilder stringBuilder =
                new StringBuilder("DELETE FROM \"")
                        .append(metadata.schema())
                        .append("\".\"")
                        .append(metadata.table())
                        .append("\" WHERE ");

        List<Object> values = new ArrayList<>();

        for (ColumnValuePair idColumn : ids) {
            stringBuilder.append("\"").append(idColumn.column()).append("\" = ? AND ");
            values.add(idColumn.value());
        }

        stringBuilder.setLength(stringBuilder.length() - 5);

        @Language("SQL")
        String sql = stringBuilder.toString();

        try {
            dataManager.getDataAccessor().executeUpdate(
                    ids,
                    SQLTransaction.Statement.of(sql, sql),
                    values,
                    0
            );
        } catch (SQLException e) {
            deleteStarted.set(false);
            throw new RuntimeException(e);
        }
    }

    public final boolean isSnapshot() {
        return isSnapshot;
    }

    @Override
    public String toString() {
        DataManager dataManager = this.dataManager;
        ColumnValuePairs ids = this.idColumns;

        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("{");
        if (ids != null) {
            for (ColumnValuePair idColumn : ids) {
                sb.append(idColumn.column()).append("=").append(idColumn.value()).append(", ");
            }
        }
        if (isDeleted) {
            sb.append("deleted=true, ");
        }
        if (isSnapshot) {
            sb.append("snapshot=true, ");
        }
        sb.append("dataManager=").append(dataManager);
        sb.append("}");
        return sb.toString();
    }

    @Override
    public final int hashCode() {
        DataManager dataManager = this.dataManager;
        ColumnValuePairs ids = this.idColumns;
        return Objects.hash(dataManager, ids);
    }

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof UniqueData other)) return false;
        if (!this.getClass().equals(other.getClass())) return false;

        DataManager dataManager = this.dataManager;
        ColumnValuePairs ids = this.idColumns;
        DataManager otherDataManager = other.dataManager;
        ColumnValuePairs otherIds = other.idColumns;

        return Objects.equals(dataManager, otherDataManager)
                && Objects.equals(ids, otherIds);
    }
}
