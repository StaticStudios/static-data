package net.staticstudios.data.data;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.key.DataKey;

public interface Data<T> {

    Class<T> getDataType();

    DataManager getDataManager();

    DataKey getKey();

    DataHolder getHolder();
}
