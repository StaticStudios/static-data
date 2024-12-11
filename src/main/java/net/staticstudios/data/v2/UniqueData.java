package net.staticstudios.data.v2;


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

    @Override
    public PrimaryKey getPKey() {
        return pKey;
    }

    @Override
    public DataManager getDataManager() {
        return dataManager;
    }
}
