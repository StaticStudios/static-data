package net.staticstudios.data.messaging.handle;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.DataProvider;
import net.staticstudios.data.DatabaseSupportedType;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.messaging.DataMessageUtils;
import net.staticstudios.data.messaging.PersistentCollectionChangeMessage;
import net.staticstudios.data.meta.PersistentCollectionMetadata;
import net.staticstudios.data.meta.PersistentEntryValueMetadata;
import net.staticstudios.data.value.PersistentCollection;
import net.staticstudios.data.value.PersistentEntryValue;
import net.staticstudios.messaging.MessageHandler;
import net.staticstudios.messaging.MessageMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class PersistentCollectionRemoveMessageHandler implements MessageHandler<PersistentCollectionChangeMessage> {

    private final DataManager dataManager;

    public PersistentCollectionRemoveMessageHandler(DataManager dataManager) {
        this.dataManager = dataManager;
    }

    @Override
    public CompletableFuture<String> onMessage(MessageMetadata metadata, PersistentCollectionChangeMessage payload) {
        PersistentCollectionMetadata collectionMetadata = (PersistentCollectionMetadata) dataManager.getMetadata(payload.address());

        // If the collectionMetadata is null, we aren't actively tracking/using this data type.
        if (collectionMetadata == null) {
            DataManager.debug("Received a PersistentCollectionChangeMessage for a data type that is not being tracked. Address: " + payload.address());
            return null;
        }
        DataProvider<?> provider = dataManager.getDataProvider(collectionMetadata.getParentClass());

        UniqueData entity = provider.get(payload.uniqueId());

        PersistentCollection<?> collection = collectionMetadata.getCollection(entity);
        List<PersistentEntryValueMetadata> entryValueMetadataList = collectionMetadata.getEntryValueMetadata();

        collection.removeAllInternalIf(entry -> {
            Map<String, PersistentEntryValue<?>> pkeyValues = new HashMap<>();

            for (PersistentEntryValueMetadata entryValueMetadata : entryValueMetadataList) {
                if (!entryValueMetadata.isPkey()) {
                    continue;
                }

                PersistentEntryValue<?> entryValue = entryValueMetadata.getValue(entry);
                pkeyValues.put(entryValue.getColumn(), entryValue);
            }

            if (pkeyValues.size() != payload.values().getFirst().size()) {
                throw new RuntimeException("Failed to find all pkey values for entry: " + entry);
            }

            for (Map<String, String> pkeyValueMap : payload.values()) {
                boolean matches = true;

                for (Map.Entry<String, String> pkeyValueEntry : pkeyValueMap.entrySet()) {
                    String column = pkeyValueEntry.getKey();
                    String value = pkeyValueEntry.getValue();

                    PersistentEntryValue<?> pkeyValue = pkeyValues.get(column);

                    if (pkeyValue == null) {
                        matches = false;
                        break;
                    }

                    Object decoded = DatabaseSupportedType.decode(value);
                    Object deserialize = dataManager.deserialize(pkeyValue.getType(), decoded);

                    if (!Objects.deepEquals(pkeyValue.getSyncedValue(), deserialize)) {
                        matches = false;
                        break;
                    }
                }

                if (matches) {
                    return true;
                }
            }
            return false;
        });

        return null;
    }

    @Override
    public String serialize(PersistentCollectionChangeMessage payload) {
        return DataMessageUtils.getGson().toJson(payload);
    }

    @Override
    public PersistentCollectionChangeMessage deserialize(String message) {
        return DataMessageUtils.getGson().fromJson(message, PersistentCollectionChangeMessage.class);
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
