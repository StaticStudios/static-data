package net.staticstudios.data.data.value.persistent;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.DeletionStrategy;
import net.staticstudios.data.ValueUpdate;
import net.staticstudios.data.ValueUpdateHandler;
import net.staticstudios.data.data.DataHolder;
import net.staticstudios.data.data.UniqueData;
import net.staticstudios.data.data.value.Value;
import net.staticstudios.data.impl.PersistentValueManager;
import net.staticstudios.data.key.CellKey;
import net.staticstudios.data.key.DataKey;
import net.staticstudios.utils.ThreadUtils;
import org.jetbrains.annotations.Blocking;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.SQLException;
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
    private DeletionStrategy deletionStrategy;

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

    public static <T> PersistentValue<T> of(UniqueData holder, Class<T> dataType, String column) {
        PersistentValue<T> pv = new PersistentValue<>(holder.getSchema(), holder.getTable(), column, holder.getRootHolder().getIdentifier().getColumn(), dataType, holder, holder.getDataManager());
        pv.deletionStrategy = DeletionStrategy.CASCADE;
        return pv;
    }

    public static <T> PersistentValue<T> foreign(UniqueData holder, Class<T> dataType, String schemaTableColumn, String foreignIdColumn) {
        String[] parts = schemaTableColumn.split("\\.");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Invalid schema.table.column format: " + schemaTableColumn);
        }
        PersistentValue<T> pv = new PersistentValue<>(parts[0], parts[1], parts[2], foreignIdColumn, dataType, holder, holder.getDataManager());
        pv.deletionStrategy = DeletionStrategy.NO_ACTION;
        return pv;
    }

    public static <T> PersistentValue<T> foreign(UniqueData holder, Class<T> dataType, String schema, String table, String column, String foreignIdColumn) {
        PersistentValue<T> pv = new PersistentValue<>(schema, table, column, foreignIdColumn, dataType, holder, holder.getDataManager());
        pv.deletionStrategy = DeletionStrategy.NO_ACTION;
        return pv;
    }

    public InitialPersistentValue initial(T value) {
        return new InitialPersistentValue(this, value);
    }

    @SuppressWarnings("unchecked")
    public PersistentValue<T> onUpdate(ValueUpdateHandler<T> updateHandler) {
        dataManager.registerValueUpdateHandler(this.getKey(), update -> ThreadUtils.submit(() -> updateHandler.handle((ValueUpdate<T>) update)));
        return this;
    }

    public PersistentValue<T> withDefault(T defaultValue) {
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
        PersistentValueManager manager = dataManager.getPersistentValueManager();
        manager.updateCache(this, value);

        ThreadUtils.submit(() -> {
            try (Connection connection = dataManager.getConnection()) {
                manager.updateInDatabase(connection, this, value);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    @Blocking
    public void set(Connection connection, T value) throws SQLException {
        PersistentValueManager manager = dataManager.getPersistentValueManager();
        manager.updateCache(this, value);

        manager.updateInDatabase(connection, this, value);
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
    public PersistentValue<T> deletionStrategy(DeletionStrategy strategy) {
        if (holder.getRootHolder().getSchema().equals(schema) && holder.getRootHolder().getTable().equals(table)) {
            throw new IllegalArgumentException("Cannot set deletion strategy for a PersistentValue in the same table as it's holder!");
        }
        this.deletionStrategy = strategy;
        return this;
    }

    @Override
    public @NotNull DeletionStrategy getDeletionStrategy() {
        return deletionStrategy == null ? DeletionStrategy.NO_ACTION : deletionStrategy;
    }

    @Override
    public int hashCode() {
        return getKey().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof PersistentValue<?> other)) {
            return false;
        }

        return getKey().equals(other.getKey());
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
