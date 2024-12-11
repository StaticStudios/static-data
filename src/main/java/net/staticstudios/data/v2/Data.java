package net.staticstudios.data.v2;

public interface Data<T> {

    DataManager getDataManager();

    DataKey getKey();

    PrimaryKey getHolderPrimaryKey();

    Class<? extends DataTypeManager<?, ?>> getDataTypeManagerClass();

    T get();

    void set(T value);
}
