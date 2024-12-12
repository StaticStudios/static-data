package net.staticstudios.data.data;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.impl.DataTypeManager;
import net.staticstudios.data.key.DataKey;

public interface Data<T> {

    Class<T> getDataType();

    DataManager getDataManager();

    Class<? extends DataTypeManager<?, ?>> getDataTypeManagerClass();

    DataKey getKey();

    DataHolder getHolder();
}
