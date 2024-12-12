package net.staticstudios.data.data;

import net.staticstudios.data.DataManager;

public interface DataHolder {

    DataManager getDataManager();

    UniqueData getRootHolder();
}
