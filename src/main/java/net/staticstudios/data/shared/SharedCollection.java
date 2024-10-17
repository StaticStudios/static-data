package net.staticstudios.data.shared;

import net.staticstudios.utils.ReflectionUtils;
import net.staticstudios.utils.ThreadUtils;
import net.staticstudios.data.Addressable;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.meta.SharedCollectionMetadata;
import net.staticstudios.data.meta.UniqueDataMetadata;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Field;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * A collection of data that is shared and synced between multiple services.
 */
@SuppressWarnings("rawtypes")
public abstract class SharedCollection<T extends CollectionEntry, M extends SharedCollectionMetadata<?, ?>, R> implements Collection<T>, Addressable {
    private final Collection<T> values;
    private final Class<T> type;
    private final Class<? extends SharedCollection> collectionType;
    private UniqueData uniqueData;
    private M metadata;

    /**
     * @param uniqueData The unique data object associated with this collection
     * @param type       The type of data being stored in the collection
     */
    protected SharedCollection(UniqueData uniqueData, Class<T> type, Class<? extends SharedCollection> collectionType) {
        this.uniqueData = uniqueData;
        this.type = type;
        this.collectionType = collectionType;

        this.values = new ArrayList<>();
    }

    /**
     * Get the metadata class for the collection
     *
     * @return The metadata class
     */
    public abstract Class<M> getMetadataClass();

    /**
     * Get the unique data object associated with this collection
     *
     * @return The unique data object
     */
    public UniqueData getData() {
        return uniqueData;
    }

    /**
     * Get the type of data being stored in the collection
     *
     * @return The type of data
     */
    public Class<T> getType() {
        return type;
    }

    /**
     * Get the metadata for the collection
     *
     * @return The metadata
     */
    public M getMetadata() {
        if (metadata == null) {
            UniqueData parent = uniqueData;
            UniqueDataMetadata parentMetadata = uniqueData.getDataManager().getUniqueDataMetadata(parent.getClass());
            Set<Field> allPersistentValues = ReflectionUtils.getFields(collectionType, parent.getClass());
            for (Field field : allPersistentValues) {
                Object value = ReflectionUtils.getFieldValue(field, parent);
                if (value == this) {
                    metadata = parentMetadata.getMetadata(getMetadataClass(), field);
                    break;
                }
            }

            if (metadata == null) {
                throw new RuntimeException("Failed to find metadata for " + this);
            }
        }

        return metadata;
    }


    @Override
    public String getAddress() {
        return getMetadata().getAddress();
    }

    @Override
    public synchronized int size() {
        return values.size();
    }

    @Override
    public synchronized boolean isEmpty() {
        return values.isEmpty();
    }

    @Override
    public synchronized boolean contains(Object o) {
        return values.contains(o);
    }

    @NotNull
    @Override
    public synchronized Iterator<T> iterator() {
        return new ArrayList<>(values).iterator();
    }

    @Override
    public synchronized Stream<T> stream() {
        return new ArrayList<>(values).stream();
    }

    @NotNull
    @Override
    public synchronized Object @NotNull [] toArray() {
        return values.toArray();
    }

    @NotNull
    @Override
    public synchronized <T1> T1 @NotNull [] toArray(@NotNull T1[] a) {
        return values.toArray(a);
    }

    @Override
    public synchronized boolean add(T t) {
        addAll(Collections.singletonList(t));

        return true;
    }

    @Override
    public synchronized boolean remove(Object o) {
        return removeAll(Collections.singletonList(o));
    }

    @Override
    public synchronized boolean containsAll(@NotNull Collection<?> c) {
        return values.containsAll(c);
    }

    @Override
    public synchronized boolean addAll(@NotNull Collection<? extends T> c) {
        List<T> values = new ArrayList<>(c);
        addAllInternal(values);

        ThreadUtils.submit(() -> addAllToDataSource(values));

        return true;
    }

    @Override
    public synchronized boolean removeAll(@NotNull Collection<?> c) {
        List<T> values = new ArrayList<>();

        for (Object o : c) {
            if (type.isInstance(o)) {
                values.add(type.cast(o));
            }
        }

        removeAllInternal(values);

        ThreadUtils.submit(() -> removeAllFromDataSource(values));

        return true;
    }

    @Override
    public synchronized boolean retainAll(@NotNull Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void clear() {
        List<T> values = new ArrayList<>(this.values);
        removeAllInternal(values);

        ThreadUtils.submit(() -> removeAllFromDataSource(values));
    }

    /**
     * Add a value to the backing collection without updating the data source.
     * This is for internal use only, do not use!
     *
     * @param values The values to add
     */
    public synchronized void addAllInternal(Collection<?> values) {
        List<T> valuesToAdd = new ArrayList<>();
        for (Object value : values) {
            if (type.isInstance(value)) {
                valuesToAdd.add(type.cast(value));
            }
        }
        try {
            for (T value : valuesToAdd) {
                getAddHandler().accept(value);
            }
        } catch (Exception e) {
            DataManager.getLogger().error("Failed to invoke add handler for collection: {}", this);
            e.printStackTrace();
        }
        this.values.addAll(valuesToAdd);
    }

    /**
     * Remove a value from the backing collection without updating the data source.
     * This is for internal use only, do not use!
     *
     * @param values The values to remove
     */
    public synchronized void removeAllInternal(Collection<?> values) {
        List<T> valuesToRemove = new ArrayList<>();
        for (Object value : values) {
            if (type.isInstance(value)) {
                valuesToRemove.add(type.cast(value));
            }
        }
        try {
            for (T value : valuesToRemove) {
                getRemoveHandler().accept(value);
            }
        } catch (Exception e) {
            DataManager.getLogger().error("Failed to invoke remove handler for collection: {}", this);
            e.printStackTrace();
        }

        for (T value : valuesToRemove) {
            this.values.remove(value);
        }
    }

    /**
     * Remove all values from the backing collection that match the given predicate without updating the data source.
     *
     * @param predicate The predicate to match
     */
    public synchronized void removeAllInternalIf(Predicate<T> predicate) {
        List<T> valuesToRemove = new ArrayList<>();
        for (T value : values) {
            if (predicate.test(value)) {
                valuesToRemove.add(value);
            }
        }

        removeAllInternal(valuesToRemove);
    }

    /**
     * Set the initial values for this collection.
     * This method should be used only before the collection has been initialized in the datasource.
     * No additional checks are made, so use with caution.
     *
     * @param values The initial values
     */
    public synchronized void setInitialElements(Collection<T> values) {
        this.values.clear();
        this.values.addAll(values);
    }

    abstract protected void addAllToDataSource(Collection<T> values);

    abstract protected void removeAllFromDataSource(Collection<T> values);

    abstract public void addAllToDataSource(R resource, Collection<T> values, boolean sendMessage);

    abstract public void removeAllFromDataSource(R resource, Collection<T> values, boolean sendMessage);

    abstract public void updateInDataSource(CollectionEntry entry);

    abstract public void updateInDataSource(R resource, CollectionEntry entry);

    abstract public @NotNull Consumer<T> getAddHandler();

    abstract public @NotNull Consumer<T> getRemoveHandler();
}
