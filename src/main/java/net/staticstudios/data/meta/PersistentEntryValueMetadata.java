package net.staticstudios.data.meta;

import net.staticstudios.data.shared.CollectionEntry;
import net.staticstudios.data.value.PersistentCollection;
import net.staticstudios.data.value.PersistentEntryValue;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class PersistentEntryValueMetadata implements SharedEntryValueMetadata {
    private final String column;
    private final Field field;
    private final boolean pkey;

    /**
     * Create a new PersistentEntryValueMetadata with the given column name.
     *
     * @param column The column name
     */
    public PersistentEntryValueMetadata(String column, Field field, boolean pkey) {
        this.column = column;
        this.field = field;
        this.pkey = pkey;

        field.setAccessible(true);
    }

    /**
     * Extract a list of {@link PersistentEntryValueMetadata} from a dummy instance of a {@link PersistentCollection}.
     *
     * @param parentCollection The dummy instance of the {@link PersistentCollection}
     * @return A list of {@link PersistentEntryValueMetadata}
     */
    public static List<PersistentEntryValueMetadata> extract(PersistentCollection<?> parentCollection) {
        List<PersistentEntryValueMetadata> entryValueMetadataList = new ArrayList<>();
        try {
            CollectionEntry dummyInstance;
            try {
                Constructor<? extends CollectionEntry> constructor = parentCollection.getType().getDeclaredConstructor();
                constructor.setAccessible(true);
                dummyInstance = constructor.newInstance();
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException("Could not create a dummy instance of " + parentCollection.getType().getName() + "! Ensure it has a no-args constructor", e);
            }
            for (Field field : parentCollection.getType().getDeclaredFields()) {
                field.setAccessible(true);
                Object value = field.get(dummyInstance);
                if (value instanceof PersistentEntryValue<?> persistentEntryValue) {
                    String columnName = persistentEntryValue.getColumn();
                    boolean isPkey = persistentEntryValue.isPkey();

                    PersistentEntryValueMetadata entryValueMetadata = new PersistentEntryValueMetadata(columnName, field, isPkey);
                    entryValueMetadataList.add(entryValueMetadata);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return entryValueMetadataList;
    }

    /**
     * Get the column where this {@link PersistentEntryValue} is stored.
     *
     * @return The column name
     */
    public String getColumn() {
        return column;
    }

    /**
     * Get the field where this {@link PersistentEntryValue} is stored.
     *
     * @return The field
     */
    public Field getField() {
        return field;
    }

    public PersistentEntryValue<?> getValue(CollectionEntry instance) {
        try {
            return (PersistentEntryValue<?>) field.get(instance);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isPkey() {
        return pkey;
    }
}
