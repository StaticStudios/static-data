package net.staticstudios.data.impl;

import net.staticstudios.data.data.InitialData;
import net.staticstudios.data.data.Keyed;
import net.staticstudios.data.data.UniqueData;
import org.jetbrains.annotations.Blocking;

import java.util.List;

public interface DataTypeManager<K extends Keyed, I extends InitialData<K, ?>> {

    @Blocking
    void insertIntoDataSource(UniqueData holder, List<I> initialData) throws Exception;

    @Blocking
    void updateInDataSource(List<K> dataList, Object value) throws Exception;

    @Blocking
    default void updateInDataSource(K keyed, Object value) throws Exception {
        updateInDataSource(List.of(keyed), value);
    }

    @Blocking
    @SuppressWarnings("unchecked")
    default void unsafeUpdateInDataSource(Keyed keyed, Object value) throws Exception {
        updateInDataSource((K) keyed, value);
    }

    //todo: load all from DS
}
