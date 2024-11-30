package net.staticstudios.data.meta.persistant.collection;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.meta.SharedCollectionMetadata;
import net.staticstudios.data.shared.CollectionEntry;
import net.staticstudios.data.value.PersistentCollection;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.List;

public class PersistentCollectionMetadata implements SharedCollectionMetadata<CollectionEntry, PersistentEntryValueMetadata> {
    private final DataManager dataManager;
    private final String table;
    private final String linkingColumn;
    private final Class<? extends UniqueData> parentClass;
    private final Class<? extends CollectionEntry> entryClass;
    private final Constructor<? extends CollectionEntry> entryConstructor;
    private final List<PersistentEntryValueMetadata> entryValueMetadata;
    private final Field field;


    private PersistentCollectionMetadata(
            DataManager dataManager,
            String table,
            String linkingColumn,
            Class<? extends UniqueData> parentClass,
            Class<? extends CollectionEntry> entryClass,
            Constructor<? extends CollectionEntry> entryConstructor,
            List<PersistentEntryValueMetadata> entryValueMetadata,
            Field field
    ) {
        this.dataManager = dataManager;
        this.table = table;
        this.linkingColumn = linkingColumn;
        this.parentClass = parentClass;
        this.entryClass = entryClass;
        this.entryConstructor = entryConstructor;
        this.entryValueMetadata = entryValueMetadata;
        this.field = field;


        boolean containsPkey = false;

        for (PersistentEntryValueMetadata entryMeta : entryValueMetadata) {
            if (entryMeta.isPkey()) {
                containsPkey = true;
                break;
            }
        }

        if (!containsPkey) {
            throw new RuntimeException("Persistent collection must contain at least one column marked as a primary key!");
        }
    }

    /**
     * Extract the metadata from a {@link PersistentCollection}.
     *
     * @param dataManager The data manager to use
     * @param parentClass The parent class that this collection is a member of
     * @param collection  A dummy instance of the collection
     * @return The metadata for the collection
     */
    @SuppressWarnings("unused") //Used via reflection
    public static <T extends UniqueData> PersistentCollectionMetadata extract(DataManager dataManager, Class<T> parentClass, PersistentCollection<?> collection, Field field) {
        try {
            Constructor<? extends CollectionEntry> entryConstructor = collection.getType().getDeclaredConstructor();
            entryConstructor.setAccessible(true);
            List<PersistentEntryValueMetadata> entryValueMetadataList = PersistentEntryValueMetadata.extract(collection);

            return new PersistentCollectionMetadata(dataManager, collection.getTable(), collection.getLinkingColumn(), parentClass, collection.getType(), entryConstructor, entryValueMetadataList, field);
        } catch (
                NoSuchMethodException e) {
            throw new RuntimeException("Persistent collection must have a no-args constructor!", e);
        }
    }

    /**
     * Get the table name where this collection is stored.
     *
     * @return The table name
     */
    public String getTable() {
        return table;
    }

    /**
     * Get the column that links this collection to its parent.
     *
     * @return The linking column
     */
    public String getLinkingColumn() {
        return linkingColumn;
    }

    /**
     * Get the collection from an {@link UniqueData} object.
     *
     * @param entity The entity to get the collection from
     * @return The collection
     */
    public PersistentCollection<?> getCollection(UniqueData entity) {
        try {
            return (PersistentCollection<?>) field.get(entity);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to get collection from parent!", e);
        }
    }

    @Override
    public Class<? extends UniqueData> getParentClass() {
        return parentClass;
    }

    @Override
    public Class<? extends CollectionEntry> getEntryClass() {
        return entryClass;
    }

    @Override
    public Constructor<? extends CollectionEntry> getEntryConstructor() {
        return entryConstructor;
    }

    @Override
    public List<PersistentEntryValueMetadata> getEntryValueMetadata() {
        return entryValueMetadata;
    }

    @Override
    public Field getField() {
        return field;
    }

    @Override
    public DataManager getDataManager() {
        return dataManager;
    }


    @Override
    public @NotNull String getMetadataAddress() {
        return "collection." + table + "." + linkingColumn;
    }
}
