package net.staticstudios.data.data;


import net.staticstudios.data.DataManager;
import net.staticstudios.data.PrimaryKey;

import java.lang.reflect.Constructor;
import java.util.UUID;

public class OtherData<T extends UniqueData> implements DataHolder {
    private final DataHolder holder;
    private final Class<T> otherClass;
    private final PersistentValue<UUID> id;
    private final PrimaryKey pKey;

    //todo: this class should be redone, this was just a POC
    public OtherData(UniqueData holder, String schema, String table, String col, Class<T> clazz) {
        this.holder = holder;
        this.otherClass = clazz;
        this.pKey = PrimaryKey.of(col, holder.getId());
        this.id = PersistentValue.of(this, UUID.class, schema, table, "id");
    }

    public static <T extends UniqueData> OtherData<T> of(UniqueData holder, Class<T> clazz, String schema, String table, String col) {
        return new OtherData<>(holder, schema, table, col, clazz);
    }

    public void setId(UUID id) {
        this.id.set(id);
    }

    public T get() {
        try {
            Constructor<T> constructor = otherClass.getDeclaredConstructor(DataManager.class, UUID.class);
            constructor.setAccessible(true);
            return constructor.newInstance(holder.getDataManager(), id.get());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public PrimaryKey getPKey() {
        return pKey;
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
