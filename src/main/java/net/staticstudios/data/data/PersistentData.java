package net.staticstudios.data.data;

import net.staticstudios.data.impl.DataTypeManager;
import net.staticstudios.data.impl.PersistentDataManager;
import net.staticstudios.data.key.CellKey;
import net.staticstudios.data.key.DataKey;

import java.util.List;

public interface PersistentData<T> extends SingleData<T> {
    String getSchema();

    String getTable();

    String getColumn();

    Class<T> getDataType();

    String getIdColumn();

    @Override
    default Class<? extends DataTypeManager<?, ?>> getDataTypeManagerClass() {
        return PersistentDataManager.class;
    }

    default InitialPersistentData initial(T value) {
        return new InitialPersistentData(this, value);
    }

    @Override
    default DataKey getKey() {
        return new CellKey(this);
    }

    default T get() {
        return getDataManager().get(this);
    }

    default void set(T value) {
        getDataManager().cache(getKey(), value);

        try {
            PersistentDataManager manager = (PersistentDataManager) getDataManager().getDataTypeManager(getDataTypeManagerClass());
            manager.setInDataSource(List.of(new InitialPersistentData(this, value)));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
