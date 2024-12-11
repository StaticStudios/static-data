package net.staticstudios.data.data;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.PrimaryKey;
import net.staticstudios.data.impl.DataTypeManager;

public interface Data<T> extends Keyed {

    DataManager getDataManager();

    PrimaryKey getHolderPrimaryKey();

    Class<? extends DataTypeManager<?, ?>> getDataTypeManagerClass();

    T get();

    void set(T value);
}
