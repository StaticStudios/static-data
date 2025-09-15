package net.staticstudios.data;

import net.staticstudios.data.util.ColumnValuePairs;
import net.staticstudios.data.util.UniqueDataMetadata;
import org.jetbrains.annotations.ApiStatus;

public abstract class UniqueData {
    private ColumnValuePairs idColumns;
    private DataManager dataManager;

    //todo: when an update is done to an id name, we need to handle it here.
    //todo: when this row is deleted from the database, we should mark this with a deleted flag and throw an error if any operations are attempted on it. more specifically, any pvs referencing this object should throw an error if this has been deleted.
    @ApiStatus.Internal
    protected final void setDataManager(DataManager dataManager) {
        this.dataManager = dataManager;
    }

    @ApiStatus.Internal
    protected final synchronized void setIdColumns(ColumnValuePairs idColumns) {
        this.idColumns = idColumns;
    }

    public DataManager getDataManager() {
        return dataManager;
    }

    public synchronized ColumnValuePairs getIdColumns() {
        return idColumns;
    }

    public final UniqueDataMetadata getMetadata() {
        return dataManager.getMetadata(this.getClass());
    }

    //todo: toString, equals, hashcode - all based on the id columns and class type
}
