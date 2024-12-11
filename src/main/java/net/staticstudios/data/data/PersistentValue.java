package net.staticstudios.data.data;

import net.staticstudios.data.DataKey;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.PrimaryKey;
import net.staticstudios.data.impl.PersistentDataManager;

public class PersistentValue<T> implements PersistentData<T> {
    private final String schema;
    private final String table;
    private final String column;
    private final Class<T> dataType;
    private final PrimaryKey holderPkey;
    private final DataManager dataManager;

    public PersistentValue(String schema, String table, String column, Class<T> dataType, PrimaryKey holderPkey, DataManager dataManager) {
        //todo: validate that the dataType is supported or has a serializer
        this.schema = schema;
        this.table = table;
        this.column = column;
        this.dataType = dataType;
        this.holderPkey = holderPkey;
        this.dataManager = dataManager;
    }

    public static <T> PersistentValue<T> of(DataHolder holder, Class<T> dataType, String schemaTableColumn) {
        String[] parts = schemaTableColumn.split("\\.");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Invalid schema.table.column format: " + schemaTableColumn);
        }

        return new PersistentValue<>(parts[0], parts[1], parts[2], dataType, holder.getPKey(), holder.getDataManager());
    }

    public static <T> PersistentValue<T> of(DataHolder holder, Class<T> dataType, String schema, String table, String column) {
        return new PersistentValue<>(schema, table, column, dataType, holder.getPKey(), holder.getDataManager());
    }

    public static <T> PersistentValue<T> of(UniqueData holder, Class<T> dataType, String column) {
        return new PersistentValue<>(holder.getSchema(), holder.getTable(), column, dataType, holder.getPKey(), holder.getDataManager());
    }

    @Override
    public String getSchema() {
        return schema;
    }

    @Override
    public String getTable() {
        return table;
    }

    @Override
    public String getColumn() {
        return column;
    }

    @Override
    public Class<T> getDataType() {
        return dataType;
    }

    @Override
    public DataManager getDataManager() {
        return dataManager;
    }

    @Override
    public DataKey getKey() {
        return PersistentDataManager.createDataKey(schema, table, column, holderPkey);
    }

    @Override
    public PrimaryKey getHolderPrimaryKey() {
        return holderPkey;
    }

    @Override
    public String toString() {
        return "PersistentValue{" +
                "schema='" + schema + '\'' +
                ", table='" + table + '\'' +
                ", column='" + column + '\'' +
                ", parentPkey=" + holderPkey +
                '}';
    }
}
