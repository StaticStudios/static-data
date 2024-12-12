package net.staticstudios.data.data;


import net.staticstudios.data.DataManager;
import net.staticstudios.data.PrimaryKey;

import java.util.UUID;

public class UniqueData implements DataHolder {
    private final DataManager dataManager;
    private final String schema;
    private final String table;
    private final UUID id;
    private final PrimaryKey pKey;

    public UniqueData(DataManager dataManager, String schema, String table, UUID id) {
        this.dataManager = dataManager;
        this.schema = schema;
        this.table = table;
        this.id = id;
        this.pKey = PrimaryKey.of("id", id);
    }

    public UUID getId() {
        return id;
    }

    public String getTable() {
        return table;
    }

    public String getSchema() {
        return schema;
    }

    public PrimaryKey getPKey() {
        return pKey;
    }

    @Override
    public DataManager getDataManager() {
        return dataManager;
    }

    @Override
    public UniqueData getRootHolder() {
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        UniqueData that = (UniqueData) obj;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
