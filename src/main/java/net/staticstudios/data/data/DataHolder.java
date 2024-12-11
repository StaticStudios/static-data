package net.staticstudios.data.data;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.PrimaryKey;

public interface DataHolder {
    PrimaryKey getPKey();

    DataManager getDataManager();

    UniqueData getRootHolder();
}
