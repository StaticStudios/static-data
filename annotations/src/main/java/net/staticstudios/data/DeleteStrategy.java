package net.staticstudios.data;

public enum DeleteStrategy {
    /**
     * When the parent data is deleted, delete this data as well.
     */
    CASCADE,
    /**
     * Do nothing when the parent data is deleted.
     * For a one-to-many collections, this will unlink the data by setting the foreign key to null.
     * For a many-to-many collection, this will remove the entries in the join table.
     */
    NO_ACTION
}
