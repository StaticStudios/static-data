package net.staticstudios.data.data;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.ValueUpdateHandler;
import net.staticstudios.data.impl.PersistentValueManager;
import net.staticstudios.data.key.CellKey;
import net.staticstudios.data.key.DataKey;

import java.util.List;
import java.util.function.Supplier;

public class PersistentValue<T> implements Value<T> {
    private final String schema;
    private final String table;
    private final String column;
    private final Class<T> dataType;
    private final DataHolder holder;
    private final String idColumn;
    private final DataManager dataManager;
    private Supplier<T> defaultValueSupplier;

    public PersistentValue(String schema, String table, String column, String idColumn, Class<T> dataType, DataHolder holder, DataManager dataManager) {
        if (!holder.getDataManager().isSupportedType(dataType)) {
            throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }

        this.schema = schema;
        this.table = table;
        this.column = column;
        this.dataType = dataType;
        this.holder = holder;
        this.idColumn = idColumn;
        this.dataManager = dataManager;
    }

    public static <T> PersistentValue<T> of(DataHolder holder, Class<T> dataType, String schemaTableColumn) {
        String[] parts = schemaTableColumn.split("\\.");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Invalid schema.table.column format: " + schemaTableColumn);
        }

        return new PersistentValue<>(parts[0], parts[1], parts[2], holder.getRootHolder().getIdentifier().getColumn(), dataType, holder, holder.getDataManager());
    }

    public static <T> PersistentValue<T> of(DataHolder holder, Class<T> dataType, String schema, String table, String column) {
        return new PersistentValue<>(schema, table, column, holder.getRootHolder().getIdentifier().getColumn(), dataType, holder, holder.getDataManager());
    }

    public static <T> PersistentValue<T> of(UniqueData holder, Class<T> dataType, String column) {
        return new PersistentValue<>(holder.getSchema(), holder.getTable(), column, holder.getRootHolder().getIdentifier().getColumn(), dataType, holder, holder.getDataManager());
    }

    public static <T> PersistentValue<T> foreign(UniqueData holder, Class<T> dataType, String schemaTableColumn, String foreignIdColumn) {
        String[] parts = schemaTableColumn.split("\\.");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Invalid schema.table.column format: " + schemaTableColumn);
        }
        return new PersistentValue<>(parts[0], parts[1], parts[2], foreignIdColumn, dataType, holder, holder.getDataManager());
    }

    public static <T> PersistentValue<T> foreign(UniqueData holder, Class<T> dataType, String schema, String table, String column, String foreignIdColumn) {
        return new PersistentValue<>(schema, table, column, foreignIdColumn, dataType, holder, holder.getDataManager());
    }

    public InitialPersistentValue initial(T value) {
        return new InitialPersistentValue(this, value);
    }

    public PersistentValue<T> onUpdate(ValueUpdateHandler<T> updateHandler) {
        dataManager.registerValueUpdateHandler(this.getKey(), updateHandler);
        return this;
    }

    public PersistentValue<T> withDefault(T defaultValue) {
        //todo: id like to validate that if this is a primative, that the value cant be null for certain entries. for example, an int.
        // further more, id like to ensure even if with default isnt called, that the value is never null. we can probably add this check in the
        // initial persistent value constructor, and in #set.
        this.defaultValueSupplier = () -> defaultValue;
        return this;
    }

    public PersistentValue<T> withDefault(Supplier<T> defaultValueSupplier) {
        this.defaultValueSupplier = defaultValueSupplier;
        return this;
    }

    public T getDefaultValue() {
        return defaultValueSupplier == null ? null : defaultValueSupplier.get();
    }

    @Override
    public DataKey getKey() {
        return new CellKey(this);
    }

    public T get() {
        return getDataManager().get(this);
    }

    public void set(T value) {
        PersistentValueManager manager = PersistentValueManager.getInstance();
        manager.updateCache(this, value);

        try {
            manager.setInDatabase(List.of(new InitialPersistentValue(this, value)));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public String getColumn() {
        return column;
    }

    @Override
    public Class<T> getDataType() {
        return dataType;
    }

    public String getIdColumn() {
        return idColumn;
    }

    @Override
    public DataManager getDataManager() {
        return dataManager;
    }

    @Override
    public DataHolder getHolder() {
        return holder;
    }

    @Override
    public String toString() {
        return "PersistentValue{" +
                "schema='" + schema + '\'' +
                ", table='" + table + '\'' +
                ", column='" + column + '\'' +
                '}';
    }
}
