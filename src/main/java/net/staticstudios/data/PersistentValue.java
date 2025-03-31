package net.staticstudios.data;

import net.staticstudios.data.data.DataHolder;
import net.staticstudios.data.data.value.InitialPersistentValue;
import net.staticstudios.data.data.value.Value;
import net.staticstudios.data.impl.PersistentValueManager;
import net.staticstudios.data.key.CellKey;
import net.staticstudios.data.util.DeletionStrategy;
import net.staticstudios.data.util.InsertionStrategy;
import net.staticstudios.data.util.ValueUpdate;
import net.staticstudios.data.util.ValueUpdateHandler;
import net.staticstudios.utils.ThreadUtils;
import org.jetbrains.annotations.Blocking;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Supplier;

/**
 * Represents a value that is stored in a database table.
 *
 * @param <T> The type of data that this value stores.
 */
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
    private InsertionStrategy insertionStrategy;
    private int updateInterval = -1;

    private PersistentValue(String schema, String table, String column, String idColumn, Class<T> dataType, DataHolder holder, DataManager dataManager) {
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

    /**
     * Create a new {@link PersistentValue} object.
     *
     * @param holder   The holder of this value.
     * @param dataType The type of data that this value stores.
     * @param column   The column in the holder's table that stores this value.
     * @param <T>      The type of data that this value stores.
     * @return The persistent value.
     */
    public static <T> PersistentValue<T> of(UniqueData holder, Class<T> dataType, String column) {
        PersistentValue<T> pv = new PersistentValue<>(holder.getSchema(), holder.getTable(), column, holder.getRootHolder().getIdentifier().getColumn(), dataType, holder, holder.getDataManager());
        pv.deletionStrategy = DeletionStrategy.CASCADE;
        return pv;
    }

    /**
     * Create a new {@link PersistentValue} object that stores a value in a different table than its holder.
     * By default, this {@link PersistentValue} will have a deletion strategy of {@link DeletionStrategy#NO_ACTION} and
     * an insertion strategy of {@link InsertionStrategy#PREFER_EXISTING}.
     *
     * @param holder            The holder of this value.
     * @param dataType          The type of data that this value stores.
     * @param schemaTableColumn The schema.table.column that stores this value.
     * @param foreignIdColumn   The column in the holder's table that stores the foreign key.
     * @param <T>               The type of data that this value stores.
     * @return The persistent value.
     */
    public static <T> PersistentValue<T> foreign(UniqueData holder, Class<T> dataType, String schemaTableColumn, String foreignIdColumn) {
        String[] parts = schemaTableColumn.split("\\.");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Invalid schema.table.column format: " + schemaTableColumn);
        }
        PersistentValue<T> pv = new PersistentValue<>(parts[0], parts[1], parts[2], foreignIdColumn, dataType, holder, holder.getDataManager());
        pv.deletionStrategy = DeletionStrategy.NO_ACTION;
        pv.insertionStrategy = InsertionStrategy.PREFER_EXISTING;
        return pv;
    }

    /**
     * Create a new {@link PersistentValue} object that stores a value in a different table than its holder.
     * By default, this {@link PersistentValue} will have a deletion strategy of {@link DeletionStrategy#NO_ACTION} and
     * an insertion strategy of {@link InsertionStrategy#PREFER_EXISTING}.
     *
     * @param holder          The holder of this value.
     * @param dataType        The type of data that this value stores.
     * @param schema          The schema that stores the value.
     * @param table           The table that stores the value.
     * @param column          The column that stores the value.
     * @param foreignIdColumn The column in the holder's table that stores the foreign key.
     * @param <T>             The type of data that this value stores.
     * @return The persistent value.
     */
    public static <T> PersistentValue<T> foreign(UniqueData holder, Class<T> dataType, String schema, String table, String column, String foreignIdColumn) {
        PersistentValue<T> pv = new PersistentValue<>(schema, table, column, foreignIdColumn, dataType, holder, holder.getDataManager());
        pv.deletionStrategy = DeletionStrategy.NO_ACTION;
        pv.insertionStrategy = InsertionStrategy.PREFER_EXISTING;
        return pv;
    }

    /**
     * Set the initial value for this object
     *
     * @param value the initial value, or null if there is no initial value
     * @return the initial value object
     */
    public InitialPersistentValue initial(@Nullable T value) {
        return new InitialPersistentValue(this, value);
    }

    /**
     * Add an update handler to this value.
     * Note that update handlers are run asynchronously.
     *
     * @param updateHandler the update handler
     * @return this
     */
    @SuppressWarnings("unchecked")
    public PersistentValue<T> onUpdate(ValueUpdateHandler<T> updateHandler) {
        dataManager.registerValueUpdateHandler(this.getKey(), update -> ThreadUtils.submit(() -> updateHandler.handle((ValueUpdate<T>) update)));
        return this;
    }

    /**
     * Set the default value for this value.
     *
     * @param defaultValue the default value, or null if there is no default value
     * @return this
     */
    public PersistentValue<T> withDefault(@Nullable T defaultValue) {
        this.defaultValueSupplier = () -> defaultValue;
        return this;
    }

    /**
     * Set the default value for this value.
     *
     * @param defaultValueSupplier the default value supplier, or null if there is no default value
     * @return this
     */
    public PersistentValue<T> withDefault(@Nullable Supplier<@Nullable T> defaultValueSupplier) {
        this.defaultValueSupplier = defaultValueSupplier;
        return this;
    }

    /**
     * Get the default value for this value.
     *
     * @return the default value, or null if there is no default value
     */
    public @Nullable T getDefaultValue() {
        return defaultValueSupplier == null ? null : defaultValueSupplier.get();
    }

    @Override
    public CellKey getKey() {
        return new CellKey(this);
    }

    public T get() {
        return getDataManager().get(this);
    }

    public void set(T value) {
        PersistentValueManager manager = dataManager.getPersistentValueManager();
        manager.updateCache(this, value);

        Runnable runnable = () -> dataManager.submitAsyncTask(connection -> manager.updateInDatabase(connection, this, value));

        if (updateInterval > 0) {
            manager.enqueueRunnable(getKey(), updateInterval, runnable);
        } else {
            runnable.run();
        }
    }

    /**
     * Set the value of this persistent value.
     * This method will block until the value is set in the database.
     *
     * @param value the value to set
     */
    @Blocking
    public void setNow(T value) {
        PersistentValueManager manager = dataManager.getPersistentValueManager();
        manager.updateCache(this, value);

        Runnable runnable = () -> dataManager.submitBlockingTask(connection -> manager.updateInDatabase(connection, this, value));

        if (updateInterval > 0) {
            manager.enqueueRunnable(getKey(), updateInterval, runnable);
        } else {
            runnable.run();
        }
    }

    /**
     * Get the schema that this value is stored in.
     *
     * @return the schema
     */
    public String getSchema() {
        return schema;
    }

    /**
     * Get the table that this value is stored in.
     *
     * @return the table
     */
    public String getTable() {
        return table;
    }

    /**
     * Get the column that this value is stored in.
     *
     * @return the column
     */
    public String getColumn() {
        return column;
    }

    @Override
    public Class<T> getDataType() {
        return dataType;
    }

    /**
     * Get the column that stores the id of this value.
     *
     * @return the id column
     */
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

    /**
     * Set the insertion strategy for this value.
     *
     * @param strategy the insertion strategy
     * @return this
     * @throws IllegalArgumentException if the holder and value are in the same table
     */
    public PersistentValue<T> insertionStrategy(InsertionStrategy strategy) {
        if (holder.getRootHolder().getSchema().equals(schema) && holder.getRootHolder().getTable().equals(table)) {
            throw new IllegalArgumentException("Cannot set deletion strategy for a PersistentValue in the same table as it's holder!");
        }
        this.insertionStrategy = strategy;
        return this;
    }

    @Override
    public @NotNull DeletionStrategy getDeletionStrategy() {
        return deletionStrategy == null ? DeletionStrategy.NO_ACTION : deletionStrategy;
    }

    /**
     * Get the insertion strategy for this value.
     *
     * @return the insertion strategy
     */
    public @NotNull InsertionStrategy getInsertionStrategy() {
        return insertionStrategy == null ? InsertionStrategy.OVERWRITE_EXISTING : insertionStrategy;
    }

    /**
     * Set the update interval for this value.
     * When set to an integer greater than 0, the database will be updated every n milliseconds, provided the value has changed.
     * Further, if the value has changed 10 times, only the 10th change will be written to the database.
     *
     * @param updateInterval the update interval
     * @return this
     */
    public PersistentValue<T> updateInterval(int updateInterval) {
        this.updateInterval = updateInterval;
        return this;
    }

    /**
     * Get the update interval for this value.
     *
     * @return the update interval
     */
    public int getUpdateInterval() {
        return updateInterval;
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
