package net.staticstudios.data;

import java.util.Objects;
import java.util.UUID;

/**
 * Represents a data object that has a unique identifier.
 */
public abstract class UniqueData {
    private DataManager dataManager;
    private UUID id;

    /**
     * This constructor must be called by any subclass's default constructor. All subclasses must define a default constructor
     */
    protected UniqueData() {
    }

    /**
     * Create a new data object
     *
     * @param dataManager The data manager that this object should use
     * @param id          The unique id of this object
     */
    public UniqueData(DataManager dataManager, UUID id) {
        this.dataManager = dataManager;
        this.id = id;
    }

    @SuppressWarnings("unused") //Called reflectively
    private void setDataManager(DataManager dataManager) {
        this.dataManager = dataManager;
    }

    /**
     * Get the data manager that this object uses
     *
     * @return the data manager
     */
    public final DataManager getDataManager() {
        return dataManager;
    }

    /**
     * Get the unique id associated with this data object
     *
     * @return the data object's id
     */
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

    @Override
    public String toString() {
        return getClass() + "{" + id + "}";
    }
}
