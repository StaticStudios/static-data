package net.staticstudios.data.messaging.handle;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.DataProvider;
import net.staticstudios.data.DatabaseSupportedType;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.messaging.DataMessageUtils;
import net.staticstudios.data.messaging.PersistentCollectionChangeMessage;
import net.staticstudios.data.meta.PersistentCollectionMetadata;
import net.staticstudios.data.meta.PersistentEntryValueMetadata;
import net.staticstudios.data.shared.CollectionEntry;
import net.staticstudios.data.value.PersistentCollection;
import net.staticstudios.data.value.PersistentEntryValue;
import net.staticstudios.messaging.MessageHandler;
import net.staticstudios.messaging.MessageMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class PersistentCollectionAddMessageHandler implements MessageHandler<PersistentCollectionChangeMessage> {

    private final DataManager dataManager;

    public PersistentCollectionAddMessageHandler(DataManager dataManager) {
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
        List<CollectionEntry> entriesToAdd = new ArrayList<>();

        for (Map<String, String> values : payload.values()) {
            CollectionEntry entry = collectionMetadata.createEntry(dataManager);
            List<PersistentEntryValueMetadata> entryValueMetadataList = collectionMetadata.getEntryValueMetadata();

            for (Map.Entry<String, String> entries : values.entrySet()) {
                PersistentEntryValue<?> entryValue = null;

                for (PersistentEntryValueMetadata entryValueMetadata : entryValueMetadataList) {
                    if (entryValueMetadata.getColumn().equals(entries.getKey())) {
                        entryValue = entryValueMetadata.getValue(entry);
                    }
                }

                if (entryValue == null) {
                    throw new RuntimeException("Failed to find entry value for column: " + entries.getKey());
                }

                Object decoded = DatabaseSupportedType.decode(entries.getValue());
                Object deserialize = dataManager.deserialize(entryValue.getType(), decoded);
                entryValue.setInitialValue(deserialize);
            }

            entriesToAdd.add(entry);
        }

        PersistentCollection<?> collection = collectionMetadata.getCollection(entity);
        collection.addAllInternal(entriesToAdd);
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
