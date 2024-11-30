package net.staticstudios.data.meta;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.shared.CollectionEntry;
import net.staticstudios.data.shared.SharedCollection;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * @implSpec Implementations must declare a static extract(Class, SharedCollection, Field) method that returns an instance of the implementing class.
 */
public interface SharedCollectionMetadata<E extends CollectionEntry, V extends SharedEntryValueMetadata> extends Metadata {


    /**
     * Get the entry class for this collection.
     *
     * @return The entry class
     */
    Class<? extends E> getEntryClass();


    /**
     * Get the entry constructor for this collection.
     *
     * @return The entry constructor
     */
    Constructor<? extends E> getEntryConstructor();

    /**
     * Create a new entry for this collection.
     *
     * @return A new entry
     */
    default E createEntry(DataManager dataManager) {
        try {
            E entry = getEntryConstructor().newInstance();
            entry.setDataManager(dataManager);
            return entry;
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the entry value metadata for this collection.
     *
     * @return The entry value metadata
     */
    List<V> getEntryValueMetadata();

    default SharedCollection<?, ?, ?> getCollection(UniqueData entity) {
        Field field = getField();
        try {
            return (SharedCollection<?, ?, ?>) field.get(entity);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
