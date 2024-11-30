package net.staticstudios.data.value;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.DataUtils;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.UpdatedValue;
import net.staticstudios.data.meta.persistant.value.ForeignPersistentValueMetadata;
import net.staticstudios.utils.ThreadUtils;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Blocking;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * A foreign persistent value is a value that is stored in a database and is synced between multiple servers.
 * This value is a reference to a {@link PersistentValue} stored on a different parent.
 */
public class ForeignPersistentValue<T> extends AbstractPersistentValue<T, ForeignPersistentValueMetadata> {
    private final String foreignTable;
    private final String linkingTable;
    private final String thisLinkingColumn;
    private final String foreignLinkingColumn;

    private @Nullable UUID foreignObjectId;


    private ForeignPersistentValue(UniqueData parent, String foreignTable, String linkingTable, String thisLinkingColumn, String foreignLinkingColumn, String column, Class<T> type, UpdateHandler<T> updateHandler) {
        super(parent, column, type, updateHandler, ForeignPersistentValue.class, ForeignPersistentValueMetadata.class);

        this.foreignTable = foreignTable;
        this.linkingTable = linkingTable;
        this.thisLinkingColumn = thisLinkingColumn;
        this.foreignLinkingColumn = foreignLinkingColumn;
    }

    /**
     * Create a new ForeignPersistentValue.
     * This is used to reference a {@link PersistentValue} stored on a different parent.
     *
     * @param parent               The parent object
     * @param type                 The type of the value
     * @param foreignTable         The table of the foreign object
     * @param column               The column of the foreign object
     * @param linkingTable         The linking table
     * @param thisLinkingColumn    The column in the linking table that contains this object's id
     * @param foreignLinkingColumn The column in the linking table that contains the foreign object's id
     * @return The new ForeignPersistentValue
     */
    @SuppressWarnings("unused")
    public static <T> ForeignPersistentValue<T> of(UniqueData parent, Class<T> type, String foreignTable, String column, String linkingTable, String thisLinkingColumn, String foreignLinkingColumn) {
        return new ForeignPersistentValue<>(parent, foreignTable, linkingTable, thisLinkingColumn, foreignLinkingColumn, column, type, updated -> {
        });
    }

    /**
     * Create a new ForeignPersistentValue.
     * This is used to reference a {@link PersistentValue} stored on a different parent.
     *
     * @param parent               The parent object
     * @param type                 The type of the value
     * @param foreignTable         The table of the foreign object
     * @param column               The column of the foreign object
     * @param linkingTable         The linking table
     * @param thisLinkingColumn    The column in the linking table that contains this object's id
     * @param foreignLinkingColumn The column in the linking table that contains the foreign object's id
     * @param updateHandler        The update handler
     * @return The new ForeignPersistentValue
     */
    public static <T> ForeignPersistentValue<T> of(UniqueData parent, Class<T> type, String foreignTable, String column, String linkingTable, String thisLinkingColumn, String foreignLinkingColumn, UpdateHandler<T> updateHandler) {
        return new ForeignPersistentValue<>(parent, foreignTable, linkingTable, thisLinkingColumn, foreignLinkingColumn, column, type, updateHandler);
    }

    /**
     * Get the table that links this object to the foreign object.
     *
     * @return The linking table
     */
    public String getLinkingTable() {
        return linkingTable;
    }

    /**
     * Get the column in the linking table that contains this object's id.
     *
     * @return The column
     */
    public String getThisLinkingColumn() {
        return thisLinkingColumn;
    }

    /**
     * Get the column in the linking table that contains the foreign object's id.
     *
     * @return The column
     */
    public String getForeignLinkingColumn() {
        return foreignLinkingColumn;
    }

    /**
     * Set the foreign object.
     *
     * @param foreignObjectId The id of the foreign object
     * @return A future that completes when the object has been linked. Note that it will complete exceptionally if the foreign object does not exist.
     */
    public CompletableFuture<T> setForeignObject(UUID foreignObjectId) {
        CompletableFuture<T> future = new CompletableFuture<>();

        ThreadUtils.submit(() -> {
            Connection connection = getMetadata().getDataManager().getConnection();
            try {
                setForeignObject(connection, foreignObjectId);
                future.complete(get());
            } catch (ForeignReferenceDoesNotExistException | SQLException e) {
                future.completeExceptionally(e);
            }
        });

        return future;
    }

    /**
     * Set the foreign object.
     *
     * @param connection      The connection to use
     * @param foreignObjectId The id of the foreign object
     * @throws SQLException                          If an error occurs
     * @throws ForeignReferenceDoesNotExistException If the foreign object does not exist
     */
    @Blocking
    public synchronized void setForeignObject(Connection connection, @Nullable UUID foreignObjectId) throws SQLException, ForeignReferenceDoesNotExistException {
        DataManager dataManager = getMetadata().getDataManager();

        boolean isAutoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        dataManager.linkForeignObjects(connection, this, foreignObjectId); //This will call #setInternalForeignObject
        connection.setAutoCommit(isAutoCommit);
    }

    @ApiStatus.Internal
    @SuppressWarnings("unchecked")
    public synchronized void setInternalForeignObject(@Nullable UUID foreignObjectId, Object value) {
        DataManager dataManager = getMetadata().getDataManager();
        if (foreignObjectId == null) {
            unlinkForeignObject();
            return;
        }

        value = DataUtils.getValue(getType(), value); //Fix weird boxing issues with primitive types
        dataManager.addDataWrapperToLookupTable(getDataAddress(foreignObjectId), this);
        T oldValue = get();
        setInternal(value);
        setInternalForeignObjectId(foreignObjectId);
        this.getUpdateHandler().onUpdate(new UpdatedValue<>(oldValue, (T) value));
    }

    @ApiStatus.Internal
    public synchronized void setInternalForeignObjectId(@Nullable UUID id) {
        this.foreignObjectId = id;
    }

    @ApiStatus.Internal
    @SuppressWarnings("unchecked")
    public void unlinkForeignObject() {
        T value = (T) DataUtils.getValue(getType(), get());
        DataManager dataManager = getMetadata().getDataManager();

        UUID foreignObjectId = this.foreignObjectId;

        setInternal(null);
        setInternalForeignObjectId(null);


        dataManager.removeDataWrapperFromLookupTable(getDataAddress(foreignObjectId), this);
        this.getUpdateHandler().onUpdate(new UpdatedValue<>(value, null));
    }

    /**
     * Get the id of the foreign object.
     *
     * @return The foreign object's id, or null if not set
     */
    public @Nullable UUID getForeignObjectId() {
        return foreignObjectId;
    }

    @Override
    public String getTable() {
        return foreignTable;
    }

    @Override
    @Nullable
    public T get() {
        return super.get();
    }

    @Override
    protected UUID getDataId() {
        if (foreignObjectId == null) {
            throw new ForeignReferenceNotSetException("foreignObjectId is not set!");
        }

        return foreignObjectId;
    }

    @Override
    public String getDataAddress() {
        if (foreignObjectId == null) {
            return null;
        }

        return super.getDataAddress();
    }
}
