package net.staticstudios.data;


import net.staticstudios.data.data.Data;
import net.staticstudios.data.data.DataHolder;
import net.staticstudios.data.data.value.InitialPersistentValue;
import net.staticstudios.data.key.DataKey;
import net.staticstudios.data.util.DataDoesNotExistException;
import net.staticstudios.data.util.DeletionStrategy;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

/**
 * Represents a one-to-one relationship between two {@link UniqueData} objects.
 *
 * @param <T> The type of data that this reference points to.
 */
public class Reference<T extends UniqueData> implements DataHolder, Data<T> {
    private final DataHolder holder;
    private final PersistentValue<UUID> id;
    private final Class<T> clazz;
    private DeletionStrategy deletionStrategy;

    private Reference(UniqueData holder, Class<T> clazz, String foreignIdColumn) {
        this.holder = holder;
        this.clazz = clazz;
        this.id = PersistentValue.of(holder, UUID.class, foreignIdColumn);
        this.deletionStrategy = DeletionStrategy.NO_ACTION;
    }

    /**
     * Create a reference to another {@link UniqueData} object.
     *
     * @param holder          The holder of this reference.
     * @param clazz           The class of the data that this reference points to.
     * @param foreignIdColumn The column in the holder's table that stores the foreign key, if the value is null then the reference is considered to be null.
     * @param <T>             The type of data that this reference points to.
     * @return The reference.
     */
    public static <T extends UniqueData> Reference<T> of(UniqueData holder, Class<T> clazz, String foreignIdColumn) {
        return new Reference<>(holder, clazz, foreignIdColumn);
    }

    @Override
    public Reference<T> deletionStrategy(DeletionStrategy strategy) {
        this.deletionStrategy = strategy;
        return this;
    }

    @Override
    public @NotNull DeletionStrategy getDeletionStrategy() {
        return this.deletionStrategy == null ? DeletionStrategy.NO_ACTION : this.deletionStrategy;
    }

    /**
     * Set the foreign id of this reference.
     *
     * @param id The id of the foreign data, or null if the reference should be null.
     */
    public void setForeignId(@Nullable UUID id) {
        this.id.set(id);
    }

    /**
     * Get the foreign id of this reference.
     *
     * @return The id of the foreign data, or null if the reference is null.
     */
    public @Nullable UUID getForeignId() {
        try {
            return id.get();
        } catch (DataDoesNotExistException e) {
            return null;
        }
    }

    /**
     * Set the initial value for this reference.
     *
     * @param data The data to set the reference to, or null if the reference should be null.
     * @return An {@link InitialPersistentValue} object that can be used to insert the reference into the database.
     */
    public InitialPersistentValue initial(T data) {
        if (data == null) {
            return id.initial(null);
        }
        return id.initial(data.getId());
    }

    /**
     * Set the initial value for this reference.
     *
     * @param id The id of the data to set the reference to, or null if the reference should be null.
     * @return An {@link InitialPersistentValue} object that can be used to insert the reference into the database.
     */
    public InitialPersistentValue initial(UUID id) {
        return this.id.initial(id);
    }

    /**
     * Set the value of this reference.
     *
     * @param data The data to set the reference to, or null if the reference should be null.
     */
    public void set(@Nullable T data) {
        if (data == null) {
            id.set(null);
            return;
        }

        id.set(data.getId());
    }

    /**
     * Get the data that this reference points to.
     *
     * @return The data that this reference points to, or null if the reference is null.
     */
    public @Nullable T get() {
        try {
            return getDataManager().get(clazz, id.get());
        } catch (DataDoesNotExistException e) {
            return null;
        }
    }

    @Override
    public Class<T> getDataType() {
        return clazz;
    }

    @Override
    public DataManager getDataManager() {
        return holder.getDataManager();
    }

    @Override
    public DataKey getKey() {
        return id.getKey();
    }

    @Override
    public DataHolder getHolder() {
        return holder;
    }

    @Override
    public UniqueData getRootHolder() {
        return holder.getRootHolder();
    }

    /**
     * Get the backing {@link PersistentValue} object that stores the foreign id.
     * This is for internal use only.
     *
     * @return The backing {@link PersistentValue} object.
     */
    public PersistentValue<UUID> getBackingValue() {
        return id;
    }

    @Override
    public String toString() {
        return "Reference{" +
                "dataType=" + clazz.getSimpleName() +
                ", id=" + id.get() +
                '}';
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

        if (!(obj instanceof Reference<?> other)) {
            return false;
        }

        return getKey().equals(other.getKey());
    }
}
