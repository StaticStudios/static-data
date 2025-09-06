package net.staticstudios.data;

import net.staticstudios.data.parse.UniqueDataMetadata;
import net.staticstudios.data.util.PrimaryKey;

import java.util.List;
import java.util.UUID;

public abstract class UniqueData implements PrimaryKey {
    private final DataManager dataManager;

    public UniqueData(DataManager dataManager) {
        this.dataManager = dataManager;
    }

    public abstract UUID getId();

    public DataManager getDataManager() {
        return dataManager;
    }

    @Override
    public List<ColumnValuePair> getWhereClause() {
        return List.of(new ColumnValuePair(getMetadata().idColumn(), getId()));
    }

    public final UniqueDataMetadata getMetadata() {
        return dataManager.getMetadata(this.getClass());
    }
}
