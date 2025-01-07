package net.staticstudios.data;

public enum InsertionStrategy {
    /**
     * Overwrite existing data with new data.
     */
    OVERWRITE_EXISTING,
    /**
     * Do not overwrite existing data, only insert if no data exists.
     */
    PREFER_EXISTING,
}
