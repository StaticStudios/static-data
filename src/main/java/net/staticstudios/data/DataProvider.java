package net.staticstudios.data;

import org.slf4j.Logger;

import java.sql.SQLException;
import java.util.Collection;
import java.util.UUID;

public interface DataProvider<T extends UniqueData> {
    Class<T> getDataType();

    Collection<T> getAll();

    T get(UUID id);

    void set(UniqueData data);

    void remove(UUID id);

    Logger getLogger();

    default void loadAllSync(DataManager dataManager) {
        long start = System.currentTimeMillis();
        getLogger().info("Loading {}s...", getDataType().getSimpleName());
        try {
            Collection<T> results = dataManager.selectAll(getDataType());
            clear();
            results.forEach(this::set);
            getLogger().info("Loaded {} {}s in {}ms", getAll().size(), getDataType().getSimpleName(), System.currentTimeMillis() - start);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    void clear();
}
