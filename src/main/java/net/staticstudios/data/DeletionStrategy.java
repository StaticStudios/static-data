package net.staticstudios.data;

public enum DeletionStrategy {
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
     * {@link net.staticstudios.data.data.collection.PersistentCollection#oneToMany} or
     * {@link net.staticstudios.data.data.collection.PersistentCollection#manyToMany}
     */
    UNLINK
}
