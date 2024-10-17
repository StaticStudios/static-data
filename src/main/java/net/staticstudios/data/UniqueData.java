package net.staticstudios.data;

import java.util.Objects;
import java.util.UUID;

/**
 * Represents a data object that has a unique identifier.
 */
public abstract class UniqueData {
    private DataManager dataManager;
    private UUID id;

    public UniqueData() {
    }

    public UniqueData(DataManager dataManager, UUID id) {
        this.dataManager = dataManager;
        this.id = id;
    }

    private void setDataManager(DataManager dataManager) {
        this.dataManager = dataManager;
    }

    public final DataManager getDataManager() {
        return dataManager;
    }

    public final UUID getId() {
        return id;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UniqueData that = (UniqueData) o;
        return Objects.equals(getId(), that.getId());
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }
}
