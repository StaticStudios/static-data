package net.staticstudios.data.data;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.key.DataKey;

public interface Data<T> extends Deletable {

    /**
     * Get the data type of data being stored
     *
     * @return the data type
     */
    Class<T> getDataType();

    /**
     * Get the data manager that this data belongs to
     *
     * @return the data manager
     */
    DataManager getDataManager();

    /**
     * Get the key for this data object
     *
     * @return the key
     */
    DataKey getKey();

    /**
     * Get the holder for this data object
     *
     * @return the holder
     */
    DataHolder getHolder();
}
