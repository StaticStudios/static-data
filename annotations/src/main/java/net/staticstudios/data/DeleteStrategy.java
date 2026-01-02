package net.staticstudios.data;

public enum DeleteStrategy {
    /**
     * When the parent data is deleted, delete this data as well.
     * For all data types, this means that the referenced data will be deleted when the parent data is deleted.
     */
    CASCADE,

    /**
     * In the context of a reference or one-to-many collection,
     * set the columns in the referenced table to null.
     */
    SET_NULL,

    /**
     * In the context of a <b>persistent value, a reference, or a one-to-many collection:</b> <br>
     * Do nothing when the parent data is deleted.
     * <br><br>
     * For a <b>many-to-many</b> collection, this will remove the entries in the join table.
     */
    NO_ACTION
}
