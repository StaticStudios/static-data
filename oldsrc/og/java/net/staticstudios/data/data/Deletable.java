package net.staticstudios.data.data;

import net.staticstudios.data.util.DeletionStrategy;
import org.jetbrains.annotations.NotNull;

public interface Deletable {
    /**
     * Set the deletion strategy for this object.
     *
     * @param strategy The deletion strategy to use.
     * @return This object.
     */
    Deletable deletionStrategy(DeletionStrategy strategy);

    /**
     * Get the deletion strategy for this object.
     *
     * @return The deletion strategy.
     */
    @NotNull DeletionStrategy getDeletionStrategy();
}
