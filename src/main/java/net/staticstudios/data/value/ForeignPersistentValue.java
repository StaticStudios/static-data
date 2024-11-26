package net.staticstudios.data.value;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.meta.persistant.value.ForeignPersistentValueMetadata;
import net.staticstudios.utils.ThreadUtils;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Blocking;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

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
     *
     * @return The new ForeignPersistentValue
     */
    @SuppressWarnings("unused")
    public static <T> ForeignPersistentValue<T> of(UniqueData parent, Class<T> type, String foreignTable, String column, String linkingTable, String thisLinkingColumn, String foreignLinkingColumn) {
        return new ForeignPersistentValue<>(parent, foreignTable, linkingTable, thisLinkingColumn, foreignLinkingColumn, column, type, updated -> {
        });
    }

    public static <T> ForeignPersistentValue<T> of(UniqueData parent, Class<T> type, String foreignTable, String column, String linkingTable, String thisLinkingColumn, String foreignLinkingColumn, UpdateHandler<T> updateHandler) {
        return new ForeignPersistentValue<>(parent, foreignTable, linkingTable, thisLinkingColumn, foreignLinkingColumn, column, type, updateHandler);
    }

    //todo: other of() methods
    //todo: jd
    //
//    /**
//     * Create a new ForeignPersistentValue.
//     * This method allows you to specify an update handler that will be called when the value is set.
//     * Note that the update handler is fired on each instance when an update is received.
//     * Also note that the update handler is not guaranteed to run on a specific thread.
//     *
//     * @param parent             The parent data object
//     * @param type               The type of the value that will be stored
//     * @param foreignTable       The table name that contains the foreign column
//     * @param foreignColumn      The column name that contains the foreign value
//     * @param localLinkingColumn The column name that contains the foreign object's id
//     * @param updateHandler      The update handler to use
//     * @return The new ForeignPersistentValue
//     */
//    @SuppressWarnings("unused")
//    public static <T> ForeignPersistentValue<T> of(UniqueData parent, Class<T> type, String foreignTable, String foreignColumn, String localLinkingColumn, UpdateHandler<T> updateHandler) {
//        return new ForeignPersistentValue<>(parent, foreignTable, foreignColumn, localLinkingColumn, type, updateHandler);
//    }
//
//    /**
//     * Create a new ForeignPersistentValue with a default value.
//     *
//     * @param parent             The parent data object
//     * @param type               The type of the value that will be stored
//     * @param defaultValue       The default (initial) value to use if the value is not present in the database
//     * @param foreignTable       The table name that contains the foreign column
//     * @param foreignColumn      The column name that contains the foreign value
//     * @param localLinkingColumn The column name that contains the foreign object's id
//     * @return The new ForeignPersistentValue
//     */
//    @SuppressWarnings("unused")
//    public static <T> ForeignPersistentValue<T> withDefault(UniqueData parent, Class<T> type, T defaultValue, String foreignTable, String foreignColumn, String localLinkingColumn) {
//        ForeignPersistentValue<T> value = new ForeignPersistentValue<>(parent, foreignTable, foreignColumn, localLinkingColumn, type, (oldVal, newVal) -> newVal);
//        value.setInternal(defaultValue);
//        return value;
//    }
//
//    /**
//     * Create a new ForeignPersistentValue with a default value.
//     * This method allows you to specify an update handler that will be called when the value is set.
//     * Note that the update handler is fired on each instance when an update is received.
//     * Also note that the update handler is not guaranteed to run on a specific thread.
//     *
//     * @param parent             The parent data object
//     * @param type               The type of the value that will be stored
//     * @param defaultValue       The default (initial) value to use if the value is not present in the database
//     * @param foreignTable       The table name that contains the foreign column
//     * @param foreignColumn      The column name that contains the foreign value
//     * @param localLinkingColumn The column name that contains the foreign object's id
//     * @return The new ForeignPersistentValue
//     */
//    @SuppressWarnings("unused")
//    public static <T> ForeignPersistentValue<T> withDefault(UniqueData parent, Class<T> type, T defaultValue, String foreignTable, String foreignColumn, String localLinkingColumn, UpdateHandler<T> updateHandler) {
//        ForeignPersistentValue<T> value = new ForeignPersistentValue<>(parent, foreignTable, foreignColumn, localLinkingColumn, type, updateHandler);
//        value.setInternal(defaultValue);
//        return value;
//    }
//
//    /**
//     * Create a new ForeignPersistentValue with a default value supplier.
//     *
//     * @param parent               The parent data object
//     * @param type                 The type of the value that will be stored
//     * @param defaultValueSupplier The supplier that will be called to get the default (initial) value to use if the value is not present in the database
//     * @param foreignTable         The table name that contains the foreign column
//     * @param foreignColumn        The column name that contains the foreign value
//     * @param localLinkingColumn   The column name that contains the foreign object's id
//     * @return The new ForeignPersistentValue
//     */
//    @SuppressWarnings("unused")
//    public static <T> ForeignPersistentValue<T> supplyDefault(UniqueData parent, Class<T> type, Supplier<T> defaultValueSupplier, String foreignTable, String foreignColumn, String localLinkingColumn) {
//        ForeignPersistentValue<T> value = new ForeignPersistentValue<>(parent, foreignTable, foreignColumn, localLinkingColumn, type, (oldVal, newVal) -> newVal);
//        value.setInternal(defaultValueSupplier.get());
//        return value;
//    }
//
//    /**
//     * Create a new ForeignPersistentValue with a default value supplier.
//     * This method allows you to specify an update handler that will be called when the value is set.
//     * Note that the update handler is fired on each instance when an update is received.
//     * Also note that the update handler is not guaranteed to run on a specific thread.
//     *
//     * @param parent               The parent data object
//     * @param type                 The type of the value that will be stored
//     * @param defaultValueSupplier The supplier that will be called to get the default (initial) value to use if the value is not present in the database
//     * @param foreignTable         The table name that contains the foreign column
//     * @param foreignColumn        The column name that contains the foreign value
//     * @param localLinkingColumn   The column name that contains the foreign object's id
//     * @return The new ForeignPersistentValue
//     */
//    @SuppressWarnings("unused")
//    public static <T> ForeignPersistentValue<T> supplyDefault(UniqueData parent, Class<T> type, Supplier<T> defaultValueSupplier, String foreignTable, String foreignColumn, String localLinkingColumn, UpdateHandler<T> updateHandler) {
//        ForeignPersistentValue<T> value = new ForeignPersistentValue<>(parent, foreignTable, foreignColumn, localLinkingColumn, type, updateHandler);
//        value.setInternal(defaultValueSupplier.get());
//        return value;
//    }


    public String getLinkingTable() {
        return linkingTable;
    }

    public String getThisLinkingColumn() {
        return thisLinkingColumn;
    }

    public String getForeignLinkingColumn() {
        return foreignLinkingColumn;
    }

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

    @Blocking
    public synchronized void setForeignObject(Connection connection, UUID foreignObjectId) throws SQLException, ForeignReferenceDoesNotExistException {
        DataManager dataManager = getMetadata().getDataManager();

        if (this.foreignObjectId != null && foreignObjectId == null) {
            dataManager.removeDataWrapperFromLookupTable(getDataAddress(this.foreignObjectId), this);
        }

        boolean isAutoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        dataManager.linkForeignObjects(connection, this, foreignObjectId); //This will call #setInternalForeignObject
        connection.setAutoCommit(isAutoCommit);
    }

    @ApiStatus.Internal
    public synchronized void setInternalForeignObject(UUID foreignObjectId, Object value) {
        if (foreignObjectId == null) {
            //TODO: stop tracking, update the value
            setInternal(null);
            return;
        }

        DataManager dataManager = getMetadata().getDataManager();
        dataManager.addDataWrapperToLookupTable(getDataAddress(foreignObjectId), this);
        setInternal(value);
        setInternalForeignObjectId(foreignObjectId);

//
//        Collection<DataWrapper> otherWrappers = dataManager.getDataWrappers(getDataAddress(id));
//        for (DataWrapper wrapper : otherWrappers) {
//            //This won't be other FPVs since they aren't in the lookup table if the foreign object id is null, so we won't support that
//            PersistentValue<?> otherPv = (PersistentValue<?>) wrapper;
//            otherPv.setInternal(value);
////            otherPv.getUpdateHandler().onUpdate(new UpdatedValue<>(oldValue, value));
//        }

        //todo: shouldnt we call the update handler? test to make sure the update handler is called
        //todo: we shouldnt call it here but i dont think its called on fpvs everywhere it should be.

    }

    @ApiStatus.Internal
    public synchronized void setInternalForeignObjectId(@Nullable UUID id) {
        this.foreignObjectId = id;
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
