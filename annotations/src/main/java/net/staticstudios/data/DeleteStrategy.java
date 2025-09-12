package net.staticstudios.data;

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
     */
    UNLINK //todo: this
}
