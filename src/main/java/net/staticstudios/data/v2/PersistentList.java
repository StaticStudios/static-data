package net.staticstudios.data.v2;

import java.util.List;

public class PersistentList<T extends DataHolder> implements Collection {
    private final UniqueData holder;
    private final DataManager dataManager;
    private final String schema;
    private final String table;
    private final String linkingColumn;

    public PersistentList(UniqueData holder, DataManager dataManager, String schema, String table, String linkingColumn) {
        this.holder = holder;
        this.dataManager = dataManager;
        this.schema = schema;
        this.table = table;
        this.linkingColumn = linkingColumn;
        //todo: think about storing holders, not values. when we implement the holder map back, then this should be much easier. just do a lookup.
    }

    public static <T> PersistentList<T> of(UniqueData holder, DataManager dataManager, String schema, String table, String linkingColumn) {
        return new PersistentList<>(holder, dataManager, schema, table, linkingColumn);
    }

    @Override
    public PrimaryKey getPKey() {
        return null;
    }

    @Override
    public DataKey getCollectionKey() {
        /* The idea behind collections is to use this collection key as a way to map this collection
         * to its entries. We will have to maintain a multi map in the data TYPE manager to track these
         */
        //todo: this should be pulled out to a data type manager
        return DataKey.of("sql", "collection", schema, table, linkingColumn, holder.getId());
    }

    @Override
    public DataManager getDataManager() {
        return dataManager;
    }

    public List<T> getAll() {
        //todo: lookup based on the collection key and return those values
    }
}
