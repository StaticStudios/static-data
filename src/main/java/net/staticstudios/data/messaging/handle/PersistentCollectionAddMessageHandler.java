package net.staticstudios.data.messaging.handle;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.DatabaseSupportedType;
import net.staticstudios.data.messaging.DataMessageUtils;
import net.staticstudios.data.messaging.PersistentCollectionChangeMessage;
import net.staticstudios.data.meta.persistant.collection.PersistentCollectionMetadata;
import net.staticstudios.data.meta.persistant.collection.PersistentEntryValueMetadata;
import net.staticstudios.data.shared.CollectionEntry;
import net.staticstudios.data.shared.DataWrapper;
import net.staticstudios.data.value.PersistentCollection;
import net.staticstudios.data.value.PersistentEntryValue;
import net.staticstudios.messaging.MessageHandler;
import net.staticstudios.messaging.MessageMetadata;

import java.util.ArrayList;
import java.util.Collection;
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
        Collection<DataWrapper> dataWrappers = dataManager.getDataWrappers(payload.collectionAddress());

        for (DataWrapper dataWrapper : dataWrappers) {
            if (!(dataWrapper instanceof PersistentCollection<?> collection)) { //This should never happen
                DataManager.debug("Received a PersistentCollectionChangeMessage for a data object that is not a PersistentCollection. Address: " + payload.collectionAddress());
                continue;
            }
            PersistentCollectionMetadata collectionMetadata = collection.getMetadata();

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

            collection.addAllInternal(entriesToAdd, true);
        }

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
