package net.staticstudios.data;

public enum DeleteStrategy {
    /**
     * When the parent data is deleted, delete this data as well.
     */
    CASCADE,
    /**
     * Do nothing when the parent data is deleted.
     */
    NO_ACTION
}
