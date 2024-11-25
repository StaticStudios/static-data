package net.staticstudios.data.shared;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.DatabaseSupportedType;
import net.staticstudios.data.messaging.CollectionEntryUpdate;
import net.staticstudios.data.messaging.CollectionEntryUpdateMessage;
import net.staticstudios.data.messaging.handle.PersistentCollectionEntryUpdateMessageHandler;
import net.staticstudios.utils.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.*;

/**
 * This class is used to wrap an entry in a {@link SharedCollection}.
 * All fields need to be wrapped with an {@link EntryValue} object.
 */
public abstract class CollectionEntry {
    private final Set<EntryValue<?>> dirtyValues = new HashSet<>();
    private DataManager dataManager;
    private List<EntryValue<?>> cachedValues = null;

    /**
     * Create a new CollectionEntry with the given data manager.
     *
     * @param dataManager The data manager to use
     */
    public CollectionEntry(DataManager dataManager) {
        this.dataManager = dataManager;
    }

    /**
     * This constructor should only be used when a no-args constructor is required.
     * Use {@link #CollectionEntry(DataManager)}
     */
    public CollectionEntry() {
        this.dataManager = null;
    }

    public void setDataManager(DataManager dataManager) {
        this.dataManager = dataManager;
    }

    /**
     * Sync the values of this entry with other servers and the database.
     *
     * @param collection The collection this entry belongs to
     */
    public final void update(SharedCollection<?, ?, ?> collection) {
        if (dirtyValues.isEmpty()) {
            return;
        }

        collection.updateInDataSource(this);
        broadcastUpdate(collection);
    }

    /**
     * Sync the values of this entry with other servers and the database.
     *
     * @param resource   The resource to use when updating the entry
     * @param collection The collection this entry belongs to
     */
    public final <R> void update(R resource, SharedCollection<?, ?, R> collection) {
        if (dirtyValues.isEmpty()) {
            return;
        }

        collection.updateInDataSource(resource, this);
        broadcastUpdate(collection);
    }

    private void broadcastUpdate(SharedCollection<?, ?, ?> collection) {
        Map<String, String> oldValues = new HashMap<>();
        Map<String, String> newValues = new HashMap<>();

        for (EntryValue<?> value : getValues()) {
            Object oldValue = value.getSyncedValue();
            String encodedOldValue = DatabaseSupportedType.encode(dataManager.serialize(oldValue));
            oldValues.put(value.getUniqueId(), encodedOldValue);
        }

        for (EntryValue<?> value : dirtyValues) {
            Object newValue = value.getInternalValue();
            String encodedNewValue = DatabaseSupportedType.encode(dataManager.serialize(newValue));

            newValues.put(value.getUniqueId(), encodedNewValue);
        }

        CollectionEntryUpdate update = new CollectionEntryUpdate(oldValues, newValues);

        dataManager.getMessenger().broadcastMessageNoPrefix(
                dataManager.getDataChannel(collection),
                PersistentCollectionEntryUpdateMessageHandler.class,
                new CollectionEntryUpdateMessage(
                        collection.getDataAddress(),
                        update
                ));

        for (EntryValue<?> value : dirtyValues) {
            value.markClean();
        }

        collection.callUpdateHandler(this);
    }

    public void addDirtyValue(EntryValue<?> value) {
        dirtyValues.add(value);
    }


    @Override
    public final int hashCode() {
        int result = 1;
        for (EntryValue<?> value : getValues()) {
            result = 31 * result + value.get().hashCode();
        }
        return result;
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }

        CollectionEntry other = (CollectionEntry) obj;
        List<EntryValue<?>> otherValues = other.getValues();

        if (getValues().size() != otherValues.size()) {
            return false;
        }

        for (int i = 0; i < getValues().size(); i++) {
            Object value = getValues().get(i).get();
            Object otherValue = otherValues.get(i).get();
            if (!Objects.deepEquals(value, otherValue)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Get the values of this entry.
     * Do not call this, internal use only.
     *
     * @return The values
     */
    public List<EntryValue<?>> getValues() {
        if (cachedValues == null) {
            updateCachedValues();
        }
        return cachedValues;
    }

    private synchronized void updateCachedValues() {
        cachedValues = new ArrayList<>();
        for (Field field : ReflectionUtils.getFields(EntryValue.class, this.getClass())) {
            cachedValues.add((EntryValue<?>) ReflectionUtils.getFieldValue(field, this));
        }
    }
}
