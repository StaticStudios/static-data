package net.staticstudios.data.messaging.handle;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.DatabaseSupportedType;
import net.staticstudios.data.messaging.CollectionEntryUpdate;
import net.staticstudios.data.messaging.CollectionEntryUpdateMessage;
import net.staticstudios.data.shared.CollectionEntry;
import net.staticstudios.data.shared.DataWrapper;
import net.staticstudios.data.shared.EntryValue;
import net.staticstudios.data.value.PersistentCollection;
import net.staticstudios.data.value.PersistentEntryValue;
import net.staticstudios.messaging.MessageHandler;
import net.staticstudios.messaging.MessageMetadata;

import java.util.Collection;
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
        Collection<DataWrapper> dataWrappers = dataManager.getDataWrappers(payload.collectionAddress());

        for (DataWrapper dataWrapper : dataWrappers) {
            if (!(dataWrapper instanceof PersistentCollection<?> collection)) { //This should never happen
                DataManager.debug("Received a CollectionEntryUpdateMessage for a data object that is not a PersistentCollection. Address: " + payload.collectionAddress());
                continue;
            }

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
                DataManager.debug("Failed to find entry to update for collection: " + payload.collectionAddress());
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

            collection.callUpdateHandler(entryToUpdate);
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
