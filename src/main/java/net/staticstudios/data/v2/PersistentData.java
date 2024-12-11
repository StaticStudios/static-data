package net.staticstudios.data.v2;

public interface PersistentData<T> extends Data<T> {
    String getSchema();

    String getTable();

    String getColumn();

    Class<T> getDataType();

    @Override
    default Class<? extends DataTypeManager<?, ?>> getDataTypeManagerClass() {
        return PersistentDataManager.class;
    }

    default InitialPersistentData initial(T value) {
        return new InitialPersistentData(this, value);
    }

    default T get() {
        return getDataManager().getOrLookupDataValue(this);
    }

    default void set(T value) {
        getDataManager().cache(getKey(), value);

        try {
            PersistentDataManager manager = (PersistentDataManager) getDataManager().getDataTypeManager(getDataTypeManagerClass());
            manager.updateInDataSource(this, value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
