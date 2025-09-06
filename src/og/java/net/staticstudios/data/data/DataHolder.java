package net.staticstudios.data.data;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.UniqueData;

public interface DataHolder {

    /**
     * Get the data manager that this holder belongs to.
     *
     * @return The data manager.
     */
    DataManager getDataManager();

    /**
     * Get the root holder of this data holder.
     *
     * @return The root holder, or this if this is the root holder.
     */
    UniqueData getRootHolder();
}
