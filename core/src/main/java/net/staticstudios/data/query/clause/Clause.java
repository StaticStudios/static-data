package net.staticstudios.data.query.clause;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.util.UniqueDataMetadata;

import java.util.List;

public interface Clause {

    List<Object> append(StringBuilder sb);

    default List<Object> append(StringBuilder sb, DataManager dataManager, UniqueDataMetadata holderMetadata) {
        return append(sb);
    }
}
