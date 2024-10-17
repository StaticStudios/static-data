package net.staticstudios.data.mocks.game;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.DataProvider;
import net.staticstudios.data.UniqueData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class MockPlayerProvider<P extends MockGenericPlayer> implements DataProvider<P> {
    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    private final Map<UUID, P> PLAYERS;

    public MockPlayerProvider(DataManager dataManager) {
        this.PLAYERS = Collections.synchronizedMap(new HashMap<>());
        dataManager.registerDataProvider(this);

        loadAllSync(dataManager);
    }


    @Override
    public Collection<P> getAll() {
        return new HashSet<>(PLAYERS.values());
    }

    @Override
    public P get(UUID id) {
        return PLAYERS.get(id);
    }

    @Override
    public void set(UniqueData data) {
        if (!getDataType().isInstance(data)) {
            throw new IllegalArgumentException("Data must be of type MockPlayer");
        }

        PLAYERS.put(data.getId(), (P) data);
    }

    @Override
    public void remove(UUID id) {
        PLAYERS.remove(id);
    }

    @Override
    public void clear() {
        PLAYERS.clear();
    }

    @Override
    public Logger getLogger() {
        return LOGGER;
    }
}
