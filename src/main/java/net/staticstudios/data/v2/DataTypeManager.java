package net.staticstudios.data.v2;

import org.jetbrains.annotations.Blocking;

import java.util.List;

public interface DataTypeManager<D extends Data<?>, I extends InitialData<D>> {

    @Blocking
    void insertIntoDataSource(UniqueData holder, List<I> initialData) throws Exception;

    @Blocking
    List<Object> loadFromDataSource(List<D> dataList) throws Exception;

    @Blocking
    void updateInDataSource(List<D> dataList, Object value) throws Exception;

    @Blocking
    default void updateInDataSource(D data, Object value) throws Exception {
        updateInDataSource(List.of(data), value);
    }

    @Blocking
    default Object loadFromDataSource(D data) throws Exception {
        return loadFromDataSource(List.of(data)).getFirst();
    }

    @Blocking
    @SuppressWarnings("unchecked")
    default Object unsafeLoadFromDataSource(Data<?> data) throws Exception {
        return loadFromDataSource((D) data);
    }

    @Blocking
    @SuppressWarnings("unchecked")
    default void unsafeUpdateInDataSource(Data<?> data, Object value) throws Exception {
        updateInDataSource((D) data, value);
    }


}
