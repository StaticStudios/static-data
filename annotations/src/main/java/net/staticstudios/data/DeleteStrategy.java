package net.staticstudios.data;

public enum DeleteStrategy {
    /**
     * When the parent data is deleted, delete this data as well.
     * For all data types, this means that the referenced data will be deleted when the parent data is deleted.
     */
    CASCADE,
    /**
     * In the context of a <b>persistent value or a reference:</b> <br>
     * Do nothing when the parent data is deleted.
     * <br><br>
     * In the context of a <b>collection:</b> <br>
     * For a <b>one-to-many</b> collections, this will unlink the data by setting the foreign key to null.<br>
     * For a <b>many-to-many</b> collection, this will remove the entries in the join table.
     */
    NO_ACTION
}
