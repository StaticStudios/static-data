package net.staticstudios.data.messaging.handle;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.messaging.ForeignLinkDeleteMessage;
import net.staticstudios.messaging.MessageHandler;
import net.staticstudios.messaging.MessageMetadata;

import java.util.concurrent.CompletableFuture;

public class ForeignLinkDeleteMessageHandler implements MessageHandler<ForeignLinkDeleteMessage> {

    private final DataManager dataManager;

    public ForeignLinkDeleteMessageHandler(DataManager dataManager) {
        this.dataManager = dataManager;
    }

    @Override
    public CompletableFuture<String> onMessage(MessageMetadata metadata, ForeignLinkDeleteMessage payload) {
        dataManager.unlinkForeignPersistentValues(payload.dataAddress());
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
