package net.staticstudios.data.messaging.handle;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.DataProvider;
import net.staticstudios.data.messaging.DataLookupMessage;
import net.staticstudios.data.meta.UniqueDataMetadata;
import net.staticstudios.messaging.MessageHandler;
import net.staticstudios.messaging.MessageMetadata;

import java.util.concurrent.CompletableFuture;

/**
 * This class is responsible for handling messages that delete data from the database.
 * When a {@link DataLookupMessage} is received, the handler will remove the data from its appropriate {@link DataProvider}.
 * At the time this message is received, the data has already been deleted from the database, so this handler will just remove it from its provider.
 */
public class DataDeleteAllMessageHandler implements MessageHandler<DataLookupMessage> {

    private final DataManager dataManager;

    public DataDeleteAllMessageHandler(DataManager dataManager) {
        this.dataManager = dataManager;
    }

    @Override
    public CompletableFuture<String> onMessage(MessageMetadata metadata, DataLookupMessage payload) {
        UniqueDataMetadata uniqueDataMetadata = dataManager.getUniqueDataMetadata(payload.tables());

        // If the uniqueDataMetadata is null, we aren't actively tracking/using this data type.
        if (uniqueDataMetadata == null) {
            DataManager.debug("Received a DataLookupMessage for a data type that is not being tracked. Tables: " + payload.tables() + " UniqueId: " + payload.uniqueId());
            return null;
        }

        DataProvider<?> provider = uniqueDataMetadata.getProvider();
        provider.remove(payload.uniqueId());

        return null;
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
