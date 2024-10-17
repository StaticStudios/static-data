package net.staticstudios.data.messaging.handle;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.DataProvider;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.messaging.DataLookupMessage;
import net.staticstudios.data.meta.UniqueDataMetadata;
import net.staticstudios.messaging.MessageHandler;
import net.staticstudios.messaging.MessageMetadata;

import java.sql.SQLException;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * This class is responsible for handling messages that insert data into the database.
 * When a {@link DataLookupMessage} is received, the handler will retrieve the data from the database,
 * and add it to its appropriate {@link DataProvider}.
 */
public class DataInsertMessageHandler implements MessageHandler<DataLookupMessage> {

    private final DataManager dataManager;

    public DataInsertMessageHandler(DataManager dataManager) {
        this.dataManager = dataManager;
    }

    @Override
    public CompletableFuture<String> onMessage(MessageMetadata metadata, DataLookupMessage payload) {
        long start = System.currentTimeMillis();
        UniqueDataMetadata uniqueDataMetadata = dataManager.getUniqueDataMetadata(payload.topLevelTable());

        if (uniqueDataMetadata != null) {
            handleTopLevelInsert(start, uniqueDataMetadata, payload.uniqueId());
            return null;
        }

        // If the uniqueDataMetadata is null, we aren't actively tracking/using this data type.
        DataManager.debug("Received a DataInsertMessage for a data type that is not being tracked. Top level table: " + payload.topLevelTable() + " UniqueId: " + payload.uniqueId());
        return null;

    }

    private void handleTopLevelInsert(long start, UniqueDataMetadata metadata, UUID uniqueId) {
        //Select all relevant data from the datasource. This will ONLY give SharedValues that we care about on this instance.
        try {
            Collection<UniqueData> results = dataManager.select(metadata, true, uniqueId);

            if (results.isEmpty()) {
                throw new IllegalStateException("Received a DataInsertMessage for a data object that does not exist in the database.");
            }

            if (results.size() > 1) {
                throw new IllegalStateException("Received a DataInsertMessage for a data object that has multiple entries in the database.");
            }

            UniqueData data = results.iterator().next();
            metadata.getProvider().set(data);

            DataManager.debug("DataInsertMessageHandler.onMessage took " + (System.currentTimeMillis() - start) + "ms to retrieve data from the database.");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean handleAsync() {
        return true;
    }

    @Override
    public boolean handleResponseAsync() {
        return true;
    }

    @Override
    public boolean shouldHandleOwnMessages() {
        return false;
    }
}
