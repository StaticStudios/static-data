package net.staticstudios.data.data;

import net.staticstudios.data.impl.DataTypeManager;
import net.staticstudios.data.impl.PersistentDataManager;

public interface PersistentData<T> extends Data<T> {
    String getSchema();

    String getTable();

    String getColumn();

    Class<T> getDataType();

    @Override
    default Class<? extends DataTypeManager<?, ?>> getDataTypeManagerClass() {
        return PersistentDataManager.class;
    }

    default InitialPersistentData initial(T value) {
        return new InitialPersistentData(this, value);
    }

    default T get() {
        return getDataManager().get(this);
    }

    default void set(T value) {
        getDataManager().cache(getKey(), value);

        try {
            PersistentDataManager manager = (PersistentDataManager) getDataManager().getDataTypeManager(getDataTypeManagerClass());
            manager.updateInDataSource(this, value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
