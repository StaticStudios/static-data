package net.staticstudios.data.delete;

public enum DeleteStrategy {
    /**
     * When the parent holder is deleted, delete this data as well.
     */
    CASCADE,
    /**
     * Do nothing when the parent holder is deleted.
     */
    NO_ACTION,
    /**
     * This is only for use in PersistentCollections created via
     * {@link PersistentCollection#oneToMany} or
     * {@link PersistentCollection#manyToMany}
     */
    UNLINK //todo: this
}
