package net.staticstudios.data.data;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.Deletable;
import net.staticstudios.data.key.DataKey;

public interface Data<T> extends Deletable {

    Class<T> getDataType();

    DataManager getDataManager();

    DataKey getKey();

    DataHolder getHolder();
}
