package net.staticstudios.data.value;

import net.staticstudios.data.shared.CollectionEntry;
import net.staticstudios.data.shared.EntryValue;

public class PersistentEntryValue<T> extends EntryValue<T> {
    private final String column;
    private final boolean pkey;

    private PersistentEntryValue(CollectionEntry parent, String column, Class<T> type, boolean mutable, boolean pkey) {
        super(parent, type, mutable);
        this.column = column;
        this.pkey = pkey;
    }

    /**
     * Create a new mutable EntryValue with the given column and value.
     * Note that mutable values will return the latest value that has been synced, ensure to that the entry is synced to stay up to date.
     *
     * @param type   The type of the value
     * @param column The column name
     * @return A mutable EntryValue
     */
    public static <T> PersistentEntryValue<T> mutable(CollectionEntry parent, Class<T> type, String column) {
        return new PersistentEntryValue<>(parent, column, type, true, false);
    }

    /**
     * Create a new immutable EntryValue with the given column and value.
     * An immutable value will be marked as a pkey by default.
     *
     * @param type   The type of the value
     * @param column The column name
     * @return An immutable EntryValue
     */
    public static <T> PersistentEntryValue<T> immutable(CollectionEntry parent, Class<T> type, String column) {
        return new PersistentEntryValue<>(parent, column, type, false, true);
    }

    /**
     * Create a new immutable EntryValue with the given column and value.
     *
     * @param type   The type of the value
     * @param column The column name
     * @param pkey   Whether this value is a primary key
     * @return An immutable EntryValue
     */
    public static <T> PersistentEntryValue<T> immutable(CollectionEntry parent, Class<T> type, String column, boolean pkey) {
        return new PersistentEntryValue<>(parent, column, type, false, pkey);
    }

    /**
     * Get the column name of this EntryValue.
     *
     * @return The column name
     */
    public String getColumn() {
        return column;
    }

    @Override
    public String getUniqueId() {
        return getColumn();
    }

    public boolean isPkey() {
        return pkey;
    }

    @Override
    public String toString() {
        return "PersistentEntryValue{" + this.getSyncedValue() + "}";
    }
}
