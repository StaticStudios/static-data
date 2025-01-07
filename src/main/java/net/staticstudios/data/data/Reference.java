package net.staticstudios.data.data;


import net.staticstudios.data.DataDoesNotExistException;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.DeletionStrategy;
import net.staticstudios.data.data.value.persistent.InitialPersistentValue;
import net.staticstudios.data.data.value.persistent.PersistentValue;
import net.staticstudios.data.key.DataKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

public class Reference<T extends UniqueData> implements DataHolder, Data<T> {
    private final DataHolder holder;
    private final PersistentValue<UUID> id;
    private final Class<T> clazz;
    private DeletionStrategy deletionStrategy;

    public Reference(UniqueData holder, Class<T> clazz, String foreignIdColumn) {
        this.holder = holder;
        this.clazz = clazz;
        this.id = PersistentValue.of(holder, UUID.class, foreignIdColumn);
        this.deletionStrategy = DeletionStrategy.NO_ACTION;
    }

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

    public void setForeignId(UUID id) {
        this.id.set(id);
    }

    public UUID getForeignId() {
        try {
            return id.get();
        } catch (DataDoesNotExistException e) {
            return null;
        }
    }

    public InitialPersistentValue initial(T data) {
        if (data == null) {
            return id.initial(null);
        }
        return id.initial(data.getId());
    }

    public InitialPersistentValue initial(UUID id) {
        return this.id.initial(id);
    }

    public void set(T data) {
        if (data == null) {
            id.set(null);
            return;
        }

        id.set(data.getId());
    }

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
}
