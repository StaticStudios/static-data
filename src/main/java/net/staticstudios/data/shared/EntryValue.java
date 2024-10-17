package net.staticstudios.data.shared;

import java.util.Objects;

public abstract class EntryValue<T> {

    private final boolean mutable;
    private final CollectionEntry parent;
    private final Class<T> type;
    private boolean initialized = false;
    private boolean dirty = false;
    private T value;
    private T syncedValue;

    protected EntryValue(CollectionEntry parent, Class<T> type, boolean mutable) {
        this.parent = parent;
        this.type = type;
        this.mutable = mutable;
    }

    /**
     * Get the value of this EntryValue.
     *
     * @return The value
     */
    public T get() {
        if (!initialized) {
            throw new IllegalStateException("Cannot get value of uninitialized EntryValue");
        }
        return syncedValue;
    }

    /**
     * Get the type of data this EntryValue holds.
     *
     * @return The type
     */
    public Class<T> getType() {
        return type;
    }

    /**
     * Set the initial value of this EntryValue.
     *
     * @param value The initial value
     */
    public void setInitialValue(Object value) {
        if (initialized) {
            throw new IllegalStateException("Cannot set initial value of initialized EntryValue");
        }

        this.value = (T) value;
        this.syncedValue = (T) value;
        this.initialized = true;
    }

    /**
     * Set the internal value of this EntryValue.
     * Note that this will not be used until the value is synced.
     *
     * @param value The synced value
     */
    public void set(T value) {
        if (!mutable) {
            throw new UnsupportedOperationException("Cannot set value of immutable EntryValue");
        }

        if (Objects.equals(this.value, value)) {
            return;
        }
        this.value = value;
        this.dirty = true;
        parent.addDirtyValue(this);
    }

    /**
     * Set the internal value of this EntryValue.
     * Do not call this, internal use only.
     */
    public void setSyncedValue(Object value) {
        this.value = (T) value;
        this.syncedValue = (T) value;
    }

    protected final void markClean() {
        this.syncedValue = value;
        this.dirty = false;
    }

    /**
     * Get the internal value of this EntryValue.
     * Do not call this, internal use only.
     *
     * @return The internal value
     */
    public final T getInternalValue() {
        return value;
    }

    /**
     * Get the synced value of this EntryValue.
     * Do not call this, internal use only.
     *
     * @return The synced value
     */
    public final T getSyncedValue() {
        return syncedValue;
    }

    abstract public String getUniqueId();

    public boolean isMutable() {
        return mutable;
    }

}
