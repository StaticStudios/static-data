package net.staticstudios.data.query;

import net.staticstudios.data.UniqueData;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

interface QueryLike<T extends UniqueData> {
    /**
     * Sets the maximum number of results to return.
     *
     * @param limit the maximum number of results to return
     * @return the current query builder
     */
    QueryLike<T> limit(int limit);

    /**
     * Sets the offset of the first result to return.
     *
     * @param offset the offset of the first result to return
     * @return the current query builder
     */
    QueryLike<T> offset(int offset);

    /**
     * Find one result.
     *
     * @return the found result, or null if none found
     */
    @Nullable T findOne();

    /**
     * Find all results.
     * This will respect the limit and offset set.
     *
     * @return the list of found results
     */
    @NotNull List<T> findAll();
}
