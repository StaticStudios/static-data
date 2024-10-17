package net.staticstudios.data.messaging.handle;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.DataProvider;
import net.staticstudios.data.DatabaseSupportedType;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.messaging.DataValueUpdateMessage;
import net.staticstudios.data.meta.SharedValueMetadata;
import net.staticstudios.data.meta.UniqueDataMetadata;
import net.staticstudios.data.shared.SharedValue;
import net.staticstudios.messaging.MessageHandler;
import net.staticstudios.messaging.MessageMetadata;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class DataValueUpdateMessageHandler implements MessageHandler<DataValueUpdateMessage> {

    private final DataManager dataManager;

    public DataValueUpdateMessageHandler(DataManager dataManager) {
        this.dataManager = dataManager;
    }


    @Override
    public CompletableFuture<String> onMessage(MessageMetadata metadata, DataValueUpdateMessage payload) {
        SharedValueMetadata<?> sharedValueMetadata = (SharedValueMetadata<?>) dataManager.getMetadata(payload.address());

        // If the sharedValueMetadata is null, we aren't actively tracking/using this data type.
        if (sharedValueMetadata == null) {
            DataManager.debug("Received a DataValueUpdateMessage for a data type that is not being tracked. Address: " + payload.address() + " UniqueId: " + payload.uniqueId());
            return null;
        }

        UniqueDataMetadata uniqueDataMetadata = Objects.requireNonNull(dataManager.getUniqueDataMetadata(sharedValueMetadata.getTable()));
        DataProvider<?> provider = uniqueDataMetadata.getProvider();
        UniqueData data = provider.get(payload.uniqueId());


        // If the data is null, we aren't actively tracking/using this data type.
        // This is only okay if the top level table is not being tracked.
        if (data == null) {
            if (dataManager.getUniqueDataMetadata(payload.parentTopLevelTable()) == null) {
                DataManager.debug("Received a DataValueUpdateMessage for a data object that is not being tracked. Top level tables differ. Theirs: " +
                        payload.parentTopLevelTable() + " vs our: " + uniqueDataMetadata.getTopLevelTable() +
                        ". Address: " + payload.address() + " UniqueId: " + payload.uniqueId());
                return null;
            }

            throw new IllegalStateException("Received a DataValueUpdateMessage for a data object that does not exist in the database.");
        }

        SharedValue<?> sharedValue = uniqueDataMetadata.getValue(data, payload.address());

        Object serialized = DatabaseSupportedType.decode(payload.encodedValue());
        Object deserialized = dataManager.deserialize(sharedValueMetadata.getType(), serialized);
        Object toSet = sharedValue.getUpdateHandler().unsafeHandleUpdate(sharedValue.get(), deserialized);
        sharedValue.setInternal(toSet);

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
