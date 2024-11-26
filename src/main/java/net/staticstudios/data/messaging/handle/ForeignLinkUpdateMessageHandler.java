package net.staticstudios.data.messaging.handle;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.messaging.ForeignLinkUpdateMessage;
import net.staticstudios.messaging.MessageHandler;
import net.staticstudios.messaging.MessageMetadata;

import java.sql.Connection;
import java.util.concurrent.CompletableFuture;

public class ForeignLinkUpdateMessageHandler implements MessageHandler<ForeignLinkUpdateMessage> {

    private final DataManager dataManager;

    public ForeignLinkUpdateMessageHandler(DataManager dataManager) {
        this.dataManager = dataManager;
    }

    @Override
    public CompletableFuture<String> onMessage(MessageMetadata metadata, ForeignLinkUpdateMessage payload) {
        try (Connection connection = dataManager.getConnection()) {
            dataManager.linkLocalForeignPersistentValues(connection, payload.linkingTable(), payload.column1(), payload.column2(), payload.id1(), payload.id2(), payload.value2());
        } catch (Exception e) {
            e.printStackTrace();
        }
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
