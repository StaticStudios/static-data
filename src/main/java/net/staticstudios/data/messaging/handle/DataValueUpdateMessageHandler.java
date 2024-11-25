package net.staticstudios.data.messaging.handle;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.DatabaseSupportedType;
import net.staticstudios.data.messaging.DataValueUpdateMessage;
import net.staticstudios.data.meta.SharedValueMetadata;
import net.staticstudios.data.shared.DataWrapper;
import net.staticstudios.data.shared.SharedValue;
import net.staticstudios.messaging.MessageHandler;
import net.staticstudios.messaging.MessageMetadata;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

public class DataValueUpdateMessageHandler implements MessageHandler<DataValueUpdateMessage> {

    private final DataManager dataManager;

    public DataValueUpdateMessageHandler(DataManager dataManager) {
        this.dataManager = dataManager;
    }

    @Override
    public CompletableFuture<String> onMessage(MessageMetadata metadata, DataValueUpdateMessage payload) {
        Collection<DataWrapper> dataWrappers = dataManager.getDataWrappers(payload.dataAddress());

        for (DataWrapper dataWrapper : dataWrappers) {
            if (!(dataWrapper instanceof SharedValue<?> sharedValue)) { //This should never happen
                DataManager.debug("Received a DataValueUpdateMessage for a data object that is not a SharedValue. Address: " + payload.dataAddress());
                continue;
            }

            SharedValueMetadata<?> sharedValueMetadata = sharedValue.getMetadata();

            Object serialized = DatabaseSupportedType.decode(payload.encodedValue());
            Object deserialized = dataManager.deserialize(sharedValueMetadata.getType(), serialized);
            sharedValue.getUpdateHandler().unsafeHandleUpdate(sharedValue.get(), deserialized);
            sharedValue.setInternal(deserialized);
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
