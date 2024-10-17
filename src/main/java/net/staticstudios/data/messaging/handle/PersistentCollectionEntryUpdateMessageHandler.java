package net.staticstudios.data.messaging.handle;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.DataProvider;
import net.staticstudios.data.DatabaseSupportedType;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.messaging.CollectionEntryUpdate;
import net.staticstudios.data.messaging.CollectionEntryUpdateMessage;
import net.staticstudios.data.meta.PersistentCollectionMetadata;
import net.staticstudios.data.shared.CollectionEntry;
import net.staticstudios.data.shared.EntryValue;
import net.staticstudios.data.value.PersistentCollection;
import net.staticstudios.data.value.PersistentEntryValue;
import net.staticstudios.messaging.MessageHandler;
import net.staticstudios.messaging.MessageMetadata;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class PersistentCollectionEntryUpdateMessageHandler implements MessageHandler<CollectionEntryUpdateMessage> {

    private final DataManager dataManager;

    public PersistentCollectionEntryUpdateMessageHandler(DataManager dataManager) {
        this.dataManager = dataManager;
    }

    @Override
    public CompletableFuture<String> onMessage(MessageMetadata metadata, CollectionEntryUpdateMessage payload) {
        PersistentCollectionMetadata collectionMetadata = (PersistentCollectionMetadata) dataManager.getMetadata(payload.collectionAddress());

        // If the collectionMetadata is null, we aren't actively tracking/using this data type.
        if (collectionMetadata == null) {
            DataManager.debug("Received a CollectionEntryUpdateMessage for a data type that is not being tracked. Address: " + payload.collectionAddress());
            return null;
        }
        DataProvider<?> provider = dataManager.getDataProvider(collectionMetadata.getParentClass());


        UniqueData entity = provider.get(payload.uniqueId());
        PersistentCollection<?> collection = collectionMetadata.getCollection(entity);

        CollectionEntryUpdate update = payload.update();
        CollectionEntry entryToUpdate = null;

        for (CollectionEntry entry : collection) {
            List<EntryValue<?>> values = entry.getValues();

            if (update.oldValues().size() != values.size()) {
                continue;
            }

            boolean matches = true;

            for (EntryValue<?> value : values) { //Check equality of all pkeys
                PersistentEntryValue<?> persistentEntryValue = (PersistentEntryValue<?>) value;
                if (!persistentEntryValue.isPkey()) {
                    continue;
                }

                String oldValue = update.oldValues().get(value.getUniqueId());

                if (oldValue == null) {
                    matches = false;
                    break;
                }

                Object decoded = DatabaseSupportedType.decode(oldValue);
                Object deserialize = dataManager.deserialize(value.getType(), decoded);

                if (!Objects.equals(value.get(), deserialize)) {
                    matches = false;
                    break;
                }
            }

            if (matches) {
                entryToUpdate = entry;
                break;
            }
        }


        if (entryToUpdate == null) {
            DataManager.debug("Failed to find entry to update for collection: " + collectionMetadata.getAddress());
            return null;
        }


        for (EntryValue<?> value : entryToUpdate.getValues()) {
            String newValue = update.newValues().get(value.getUniqueId());

            if (newValue == null) {
                continue;
            }

            Object decoded = DatabaseSupportedType.decode(newValue);
            Object deserialize = dataManager.deserialize(value.getType(), decoded);

            value.setSyncedValue(deserialize);
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
