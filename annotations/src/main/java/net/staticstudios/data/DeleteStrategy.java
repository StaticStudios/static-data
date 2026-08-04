package net.staticstudios.data;

/**
 * Actions that can be applied to related data when the object holding a relationship field is deleted.
 */
public enum DeleteStrategy {
    /**
     * Delete the related data when the object holding the annotated field is deleted.
     * For a many-to-many collection, the matching join-table entries are removed and the
     * referenced objects are deleted.
     */
    CASCADE,

    /**
     * Preserve the related data but remove the relationship when the object holding the
     * annotated field is deleted. For a one-to-many collection, the linking columns are set
     * to {@code null}. For a many-to-many collection, the matching join-table entries are
     * deleted.
     */
    SET_NULL,

    /**
     * Do not modify the related data or relationship when the object holding the annotated
     * field is deleted. A foreign-key constraint may therefore reject the deletion while a
     * relationship still exists.
     */
    NO_ACTION
}
