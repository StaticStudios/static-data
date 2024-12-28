package net.staticstudios.data.data.value.persistent;

import net.staticstudios.data.DataManager;
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

import java.sql.Connection;
import java.sql.SQLException;
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
                manager.setInDatabase(connection, List.of(new InitialPersistentValue(this, value)));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    @Blocking
    public void set(Connection connection, T value) throws SQLException {
        PersistentValueManager manager = dataManager.getPersistentValueManager();
        manager.updateCache(this, value);

        manager.setInDatabase(connection, List.of(new InitialPersistentValue(this, value)));
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
