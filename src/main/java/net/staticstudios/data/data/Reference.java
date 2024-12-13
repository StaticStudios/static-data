package net.staticstudios.data.data;


import net.staticstudios.data.DataDoesNotExistException;
import net.staticstudios.data.DataManager;

import java.util.UUID;

public class Reference<T extends UniqueData> implements DataHolder {
    private final DataHolder holder;
    private final PersistentValue<UUID> id;
    private final Class<T> clazz;

    public Reference(UniqueData holder, Class<T> clazz, String foreignIdColumn) {
        this.holder = holder;
        this.clazz = clazz;
        this.id = PersistentValue.of(holder, UUID.class, foreignIdColumn);
    }

    public static <T extends UniqueData> Reference<T> of(UniqueData holder, Class<T> clazz, String foreignIdColumn) {
        return new Reference<>(holder, clazz, foreignIdColumn);
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

    public void set(T data) {
        if (data == null) {
            id.set(null);
            return;
        }

        id.set(data.getId());
    }

    public T get() {
        try {
            return getDataManager().getUniqueData(clazz, id.get());
        } catch (DataDoesNotExistException e) {
            return null;
        }
    }

    @Override
    public DataManager getDataManager() {
        return holder.getDataManager();
    }

    @Override
    public UniqueData getRootHolder() {
        return holder.getRootHolder();
    }
}
